import fs from 'fs/promises';
import path from 'path';
import { groupBy, last, partition } from 'lodash';

import { sleep } from '../../utils/time';
import type { DatabaseInstance, DatabaseOptions } from '../common/types';
import { initRWLock } from '../common/rwLock';

declare const Buffer: any;
declare type Buffer = any

enum FileType {
  LOG = 1,
  COMPACT = 2,
  TEMP = 3,
}

type Tuple = {
  key: string,
  value: string | undefined
  buffer?: Buffer;
}

type Chunk = {
  fileInfo: FileInfo,
  size: number,
  isRemoved: boolean
}

const HEADER_SIZE = 4;
const MAX_CHUNK_CONTENT_SIZE = 4096;
const MAX_TUPLE_SIZE = MAX_CHUNK_CONTENT_SIZE - HEADER_SIZE;

export async function runEngine({ dataPath }: DatabaseOptions): Promise<DatabaseInstance> {
  console.log('Run Log Engine');

  function createNewLogFileInfo(): FileInfo {
    const baseName = `data${Date.now()}`;

    const filePath = path.join(dataPath, `${baseName}.txt`);

    return {
      type: FileType.LOG,
      baseName,
      version: 0,
      filePath,
    };
  }

  function getFileInfo(fileName: string): FileInfo | undefined {
    const filePath = path.join(dataPath, fileName);

    const match = fileName.match(/^(data\d+)(?:\.(\d+))?\.txt(_?)$/);

    if (!match) {
      return undefined;
    }

    const baseName = match[1];

    if (match[3]) {
      return {
        type: FileType.TEMP,
        baseName,
        version: 0,
        filePath,
      };
    }

    if (match[2]) {
      return {
        type: FileType.COMPACT,
        baseName,
        version: Number.parseInt(match[2], 10),
        filePath,
      };
    }

    return {
      type: FileType.LOG,
      baseName,
      version: 0,
      filePath,
    };
  }

  function getNewVersionFileInfo(fileInfo: FileInfo): FileInfo {
    const { baseName } = fileInfo;
    const version = fileInfo.version + 1;

    return {
      type: FileType.COMPACT,
      baseName,
      version,
      filePath: path.join(dataPath, `${baseName}.${version}.txt`),
    };
  }

  const filesInfoInDataDir = (await fs.readdir(dataPath))
    .map(fileName => getFileInfo(fileName)!)
    .filter(Boolean);

  let [tempFilesInfo, filesInfoList] = partition(filesInfoInDataDir, info => info.type === FileType.TEMP);

  for (const tempFileInfo of tempFilesInfo) {
    console.log(`Init: remove old temp file ${tempFileInfo.filePath}`);
    await fs.unlink(tempFileInfo.filePath);
  }

  filesInfoList
    .sort((a, b) => {
      if (a.baseName === b.baseName) {
        return a.version - b.version;
      }
      return a.baseName.localeCompare(b.baseName);
    });

  const groupedByBaseName = groupBy(filesInfoList, info => info.baseName);

  for (const filesInfo of Object.values(groupedByBaseName)) {
    if (filesInfo.length > 1) {
      filesInfo.sort((a, b) => a.version - b.version);
      filesInfo.pop();

      await Promise.all(filesInfo.map(async removeFileInfo => {
        console.log(`Init: remove previous version ${removeFileInfo.filePath}`);

        filesInfoList = filesInfoList.filter(fileInfo => fileInfo !== removeFileInfo);

        await fs.unlink(removeFileInfo.filePath);
      }));
    }
  }

  console.log(`Files:\n  ${filesInfoList.map(info => info.filePath).join('\n  ')}`);

  let newChunkAfterLastCompaction = false;

  const files: Chunk[] = await Promise.all(filesInfoList.map(async fileInfo => {
    const fileSize = (await fs.stat(fileInfo.filePath)).size;

    return {
      fileInfo,
      size: fileSize - (fileInfo.type === FileType.COMPACT ? HEADER_SIZE : 0),
      isRemoved: false,
    };
  }));

  const getAccess = initRWLock();

  async function readFile(fileInfo: FileInfo): Promise<Buffer> {
    const fileBuffer = await getAccess(fileInfo.filePath).getReadAccess(() => fs.readFile(fileInfo.filePath));

    if (fileInfo.type === FileType.TEMP) {
      throw new Error();
    }

    if (fileInfo.type === FileType.COMPACT) {
      const type = fileBuffer.readUInt16BE(0);
      const size = fileBuffer.readUInt16BE(2);

      if (type !== FileType.COMPACT) {
        throw new Error('Invalid file');
      }

      if (size !== fileBuffer.length) {
        throw new Error('Invalid file');
      }

      return fileBuffer.slice(HEADER_SIZE);
    }

    return fileBuffer;
  }

  async function writeFile(fileInfo: FileInfo, content: Buffer): Promise<void> {
    const tempFilePath = `${fileInfo.filePath}_`;
    await fs.writeFile(tempFilePath, addArchiveHeaders(content));
    await fs.rename(tempFilePath, fileInfo.filePath);
  }

  async function appendFile(fileInfo: FileInfo, buffer: Buffer): Promise<void> {
    await getAccess(fileInfo.filePath).getWriteAccess(() =>
      fs.appendFile(fileInfo.filePath, buffer),
    );
  }

  async function unlinkFile(fileInfo: FileInfo): Promise<void> {
    await getAccess(fileInfo.filePath).getWriteAccess(() => fs.unlink(fileInfo.filePath));
  }

  async function get(key: string): Promise<string | undefined> {
    for (const file of (files.filter(file => !file.isRemoved).reverse())) {
      const chunkContent = await readFile(file.fileInfo);

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

    if (tuple.length > MAX_TUPLE_SIZE) {
      throw new Error('To big tuple');
    }

    let currentFile = last(files);

    if (!currentFile || currentFile.size + tuple.length > MAX_CHUNK_CONTENT_SIZE) {
      currentFile = {
        fileInfo: createNewLogFileInfo(),
        size: 0,
        isRemoved: false,
      };

      files.push(currentFile);
      newChunkAfterLastCompaction = true;
    }

    await appendFile(currentFile.fileInfo, tuple);
    currentFile.size += tuple.length;
  }

  async function runCompaction() {
    console.log('Run Compaction');
    await fs.writeFile('stats/files.json', JSON.stringify(files, null, 2));

    const headlessFiles = files.slice(0, -1).filter(file => !file.isRemoved);
    const reversedFiles = [...headlessFiles].reverse();

    const alreadyKeys = new Set<string>();
    const removeChunks = new Set<FileInfo>();

    for (const file of reversedFiles) {
      const targetFileInfo = file.fileInfo;

      const chunkContent = await readFile(targetFileInfo);
      const tuples = parseFile(chunkContent, { includeTupleBuffers: true }).reverse();

      const filteredTuples = tuples.filter(tuple => {
        const alreadyHas = alreadyKeys.has(tuple.key);

        if (!alreadyHas) {
          alreadyKeys.add(tuple.key);
        }

        return !alreadyHas;
      });

      if (filteredTuples.length === 0) {
        console.log(`Compact: dedupl  ${targetFileInfo} (0)`);
        removeChunks.add(targetFileInfo);
        file.size = 0;
        file.isRemoved = true;
      } else if (filteredTuples.length !== tuples.length) {
        const newFileInfo = getNewVersionFileInfo(targetFileInfo);
        const updatedChunkContent = makeChunkContent(filteredTuples.reverse());

        await writeFile(newFileInfo, updatedChunkContent);
        file.fileInfo = newFileInfo;
        file.size = updatedChunkContent.length;
        removeChunks.add(targetFileInfo);

        console.log(`Compact: compact ${targetFileInfo.filePath} (${tuples.length}) => ${newFileInfo.filePath} (${filteredTuples.length})`);
      }

      await sleep(100);
    }

    const actualFiles = headlessFiles.filter(file => !file.isRemoved);

    for (let i = 0; i < actualFiles.length - 1; i++) {
      const chunkA = actualFiles[i];
      const chunkB = actualFiles[i + 1];

      if (chunkA.size + chunkB.size <= MAX_CHUNK_CONTENT_SIZE) {
        const chunkContentA = await readFile(chunkA.fileInfo);
        const chunkContentB = await readFile(chunkB.fileInfo);

        const newChunkFileInfo = getNewVersionFileInfo(chunkB.fileInfo);

        await writeFile(newChunkFileInfo, Buffer.concat([chunkContentA, chunkContentB]));

        console.log(`Compact: merge   ${chunkA.fileInfo.filePath} (${chunkA.size}b) + ${chunkB.fileInfo.filePath} (${chunkB.size}b) => ${newChunkFileInfo.filePath} (${chunkA.size + chunkB.size}b)`);

        removeChunks.add(chunkA.fileInfo);
        removeChunks.add(chunkB.fileInfo);

        chunkB.fileInfo = newChunkFileInfo;
        chunkB.size = chunkA.size + chunkB.size;
        chunkA.size = 0;
        chunkA.isRemoved = true;
      }
    }

    if (removeChunks.size) {
      await sleep(1000);

      for (const removeFileInfo of removeChunks) {
        try {
          console.log(`Compact: remove  ${removeFileInfo.filePath}`);
          await unlinkFile(removeFileInfo);
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

function addArchiveHeaders(tuplesBuffer: Buffer): Buffer {
  const header = Buffer.alloc(HEADER_SIZE);

  header.writeUInt16BE(FileType.COMPACT, 0);
  header.writeUInt16BE(HEADER_SIZE + tuplesBuffer.length, 2);

  return Buffer.concat([header, tuplesBuffer]);
}

function makeChunkContent(tuples: Tuple[]): Buffer {
  return (Buffer.concat(tuples.map(tuple => tuple.buffer)));
}

type FileInfo = {
  type: FileType,
  baseName: string,
  version: number
  filePath: string,
}


