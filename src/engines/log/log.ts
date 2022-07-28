import fs from 'fs/promises';
import path from 'path';
import { last } from 'lodash';

import { sleep } from '../../utils/time';
import type { DatabaseInstance, DatabaseOptions } from '../common/types';
import { initRWLock } from '../common/rwLock';

declare const Buffer: any;
declare type Buffer = any

type Tuple = {
  key: string,
  value: string | undefined
  buffer?: Buffer;
}

type Chunk = {
  filePath: string,
  size: number,
  isRemoved: boolean
}

const CHUNK_SIZE = 4096;


export async function runEngine({ dataPath }: DatabaseOptions): Promise<DatabaseInstance> {
  console.log('Run Log Engine');

  const filePathsList = (await fs.readdir(dataPath)).filter(fileName => fileName.startsWith('data'))
    .sort()
    .map(fileName => path.join(dataPath, fileName));

  let newChunkAfterLastCompaction = false;

  const files: Chunk[] = await Promise.all(filePathsList.map(async filePath => ({
    filePath,
    size: (await fs.stat(filePath)).size,
    isRemoved: false,
  })));

  const getAccess = initRWLock();

  async function readFile(filePath: string): Promise<Buffer> {
    return getAccess(filePath).getReadAccess(() => fs.readFile(filePath));
  }

  async function appendFile(filePath: string, buffer: Buffer): Promise<void> {
    await getAccess(filePath).getWriteAccess(() => fs.appendFile(filePath, buffer));
  }

  async function unlinkFile(filePath: string): Promise<void> {
    await getAccess(filePath).getWriteAccess(() => fs.unlink(filePath));
  }

  async function get(key: string): Promise<string | undefined> {
    for (const file of (files.filter(file => !file.isRemoved).reverse())) {
      const chunkContent = await readFile(file.filePath);

      const tuples = parseFile(chunkContent);

      for (const tuple of [...tuples].reverse()) {
        if (tuple.key === key) {
          return tuple.value;
        }
      }
    }

    return undefined;
  }

  async function set(key: string, value: string | undefined) {
    const keyBuffer = Buffer.from(key, 'utf-8');
    const valueBuffer = Buffer.from(value, 'utf-8');

    const headerBuffer = Buffer.alloc(4);
    headerBuffer.writeUInt16BE(keyBuffer.length, 0);
    headerBuffer.writeUInt16BE(valueBuffer.length, 2);

    const tuple = Buffer.concat([headerBuffer, keyBuffer, valueBuffer, Buffer.from('\n', 'utf-8')]);

    if (tuple.length > CHUNK_SIZE) {
      throw new Error('To big tuple');
    }

    let currentFile = last(files);

    if (!currentFile || currentFile.size + tuple.length > CHUNK_SIZE) {
      currentFile = {
        filePath: path.join(dataPath, `data${Date.now()}.txt`),
        size: 0,
        isRemoved: false,
      };

      files.push(currentFile);
      newChunkAfterLastCompaction = true;
    }

    await appendFile(currentFile.filePath, tuple);
    currentFile.size += tuple.length;
  }

  async function runCompaction() {
    console.log('Run Compaction');
    await fs.writeFile('stats/files.json', JSON.stringify(files, null, 2));

    const headlessFiles = files.slice(0, -1).filter(file => !file.isRemoved);
    const reversedFiles = [...headlessFiles].reverse();

    const alreadyKeys = new Set<string>();
    const removeChunks = new Set<string>();

    for (const file of reversedFiles) {
      const targetFileName = file.filePath;

      const chunkContent = await readFile(targetFileName);
      const tuples = parseFile(chunkContent, { includeTupleBuffers: true }).reverse();

      const filteredTuples = tuples.filter(tuple => {
        const alreadyHas = alreadyKeys.has(tuple.key);

        if (!alreadyHas) {
          alreadyKeys.add(tuple.key);
        }

        return !alreadyHas;
      });

      if (filteredTuples.length === 0) {
        removeChunks.add(targetFileName);
        file.size = 0;
        file.isRemoved = true;
      } else if (filteredTuples.length !== tuples.length) {
        const newFilePath = getNewVersionFileName(targetFileName);

        const updatedChunkContent = makeChunkContent(filteredTuples.reverse());

        await fs.writeFile(newFilePath, updatedChunkContent);
        file.filePath = newFilePath;
        file.size = updatedChunkContent.length;
        removeChunks.add(targetFileName);

        console.log(`Compact: compact ${targetFileName} (${tuples.length}) => ${newFilePath} (${filteredTuples.length})`);
      }

      await sleep(100);
    }

    for (let i = 0; i < headlessFiles.length - 1; i++) {
      const chunkA = headlessFiles[i];
      const chunkB = headlessFiles[i + 1];

      if (chunkA.size + chunkB.size <= CHUNK_SIZE) {
        const chunkContentA = await readFile(chunkA.filePath);
        const chunkContentB = await readFile(chunkB.filePath);

        const newChunkName = getNewVersionFileName(chunkB.filePath);

        await fs.writeFile(newChunkName, Buffer.concat([chunkContentA, chunkContentB]));

        console.log(`Compact: merge   ${chunkA.filePath} (${chunkA.size}b) + ${chunkB.filePath} (${chunkB.size}b) => (${chunkA.size + chunkB.size}b)`);

        removeChunks.add(chunkA.filePath);
        removeChunks.add(chunkB.filePath);

        chunkB.filePath = newChunkName;
        chunkB.size = chunkA.size + chunkB.size;
        chunkA.size = 0;
        chunkA.isRemoved = true;
      }
    }

    if (removeChunks.size) {
      await sleep(1000);

      for (const removeFilePath of removeChunks) {
        try {
          console.log(`Compact: remove  ${removeFilePath}`);
          await unlinkFile(removeFilePath);
        } catch (error) {
          console.error(error);
        }
      }
    }
  }

  setTimeout(async () => {
    try {
      while (true) {
        if (newChunkAfterLastCompaction) {
          newChunkAfterLastCompaction = false;
          await runCompaction();
        }
        await sleep(2000);
      }
    } catch (error) {
      console.error('Compaction failed:', error);
      process.exit(1);
    }
  }, 1000);

  return {
    get,
    set,
  };
}


function parseFile(fileContent: Buffer, { includeTupleBuffers = false } = {}): Tuple[] {
  const tuples = [];
  let offset = 0;

  while (offset < fileContent.length) {
    const keySize = fileContent.readUInt16BE(offset);
    const valueSize = fileContent.readUInt16BE(offset + 2);

    const start = offset + 4;
    const keyBuffer = fileContent.slice(start, start + keySize);
    const valueStart = start + keySize;
    const valueBuffer = fileContent.slice(valueStart, valueStart + valueSize);

    const tuple: Tuple = {
      key: keyBuffer.toString(),
      value: valueBuffer.toString(),
    };

    const tupleSize = 4 + keySize + valueSize + 1;

    if (includeTupleBuffers) {
      tuple.buffer = fileContent.slice(offset, offset + tupleSize);
    }

    tuples.push(tuple);

    offset += tupleSize;
  }

  return tuples;
}

function makeChunkContent(tuples: Tuple[]): Buffer {
  return Buffer.concat(tuples.map(tuple => tuple.buffer));
}

function getNewVersionFileName(filePath: string): string {
  const dir = path.dirname(filePath);
  const fileName = path.basename(filePath);

  const match = fileName.match(/^(data\d+)(?:\.(\d+))?\.txt$/);

  if (!match) {
    throw new Error();
  }

  return path.join(dir, `${match[1]}.${Number.parseInt(match[2] ?? '0', 10) + 1}.txt`);
}
