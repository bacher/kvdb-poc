import fs from 'fs/promises';
import path from 'path';
import { last } from 'lodash';

import type { DatabaseInstance, DatabaseOptions } from '../common/types';

type Tuple = {
  key: string,
  value: string | undefined
}

const CHUNK_SIZE = 4096;

declare const Buffer: any;

export async function runEngine({ dataPath }: DatabaseOptions): Promise<DatabaseInstance> {
  console.log('Run Log Engine');

  const fileNamesList = (await fs.readdir(dataPath)).filter(fileName => fileName.startsWith('data'))
    .sort()
    .map(fileName => path.join(dataPath, fileName));

  const files = await Promise.all(fileNamesList.map(async fileName => ({
    fileName,
    size: (await fs.stat(fileName)).size,
  })));

  async function get(key: string): Promise<string | undefined> {
    for (const file of ([...files].reverse())) {
      const fileContent = await fs.readFile(file.fileName);

      const tuples = parseFile(fileContent);

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
        fileName: path.join(dataPath, `data${Date.now()}.txt`),
        size: 0,
      };

      files.push(currentFile);
    }

    await fs.appendFile(currentFile.fileName, tuple);
    currentFile.size += tuple.length;
  }

  return {
    get,
    set,
  };
}


function parseFile(fileContent: any): Tuple[] {
  const tuples = [];
  let offset = 0;

  while (offset < fileContent.length) {
    const keySize = fileContent.readUInt16BE(offset);
    const valueSize = fileContent.readUInt16BE(offset + 2);

    const start = offset + 4;
    const keyBuffer = fileContent.slice(start, start + keySize);
    const valueStart = start + keySize;
    const valueBuffer = fileContent.slice(valueStart, valueStart + valueSize);

    tuples.push({
      key: keyBuffer.toString(),
      value: valueBuffer.toString(),
    });

    offset += 4 + keySize + valueSize + 1;
  }

  return tuples;
}
