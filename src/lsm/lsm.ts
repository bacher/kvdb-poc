import fs from 'fs/promises';
import path from 'path';

import type { DatabaseInstance, DatabaseOptions } from './types';

declare const Buffer: any;

export function runLSM({ dataPath }: DatabaseOptions): DatabaseInstance {
  console.log('runLSM');

  function get(key: string): string | undefined {
    console.log('get:', key);
    return 'hello';
  }

  async function set(key: string, value: string | undefined) {
    console.log('set:', key, value);

    const keyBuffer = Buffer.from(key, 'utf-8');
    const valueBuffer = Buffer.from(value, 'utf-8');

    const headerBuffer = Buffer(8);
    headerBuffer.writeUInt16BE(keyBuffer.length, 0);
    headerBuffer.writeUInt16BE(valueBuffer.length, 4);

    console.log('l', keyBuffer.length, valueBuffer.length);

    await fs.appendFile(path.join(dataPath, 'log'), Buffer.concat([headerBuffer, keyBuffer, valueBuffer]));
  }

  return {
    get,
    set,
  };
}
