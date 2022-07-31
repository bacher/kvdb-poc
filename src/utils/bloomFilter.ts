import crypto from 'crypto';

declare const Buffer: any;
declare type Buffer = any

export const BLOOM_BYTES_SIZE = 128;
const RANGE = BLOOM_BYTES_SIZE * 8;

export function createBloomFilter(): Buffer {
  return Buffer.alloc(BLOOM_BYTES_SIZE);
}

export type BloomFilterValueOffset = {
  value: string,
  offsets: { byteOffset: number, flag: number }[],
}

export function getOffsetsForValue(value: string): BloomFilterValueOffset {
  const hash = crypto.createHash('md5').update(value).digest();
  const hashSize = hash.length;

  const already: number[] = [];
  const offsets = [];
  let bitsWritten = 0;

  for (let hashOffset = 0; hashOffset < hashSize && bitsWritten < 3; hashOffset += 2) {
    const hashValue = hash.readUInt16BE(hashSize - hashOffset - 2) % RANGE;

    if (!already.includes(hashValue)) {
      offsets.push({
        byteOffset: Math.floor(hashValue / 8),
        flag: 2 ** (hashValue % 8),
      });
    }
  }

  return {
    value,
    offsets,
  };
}

export function addValueToBloomFilter(buffer: Buffer, value: string): void {
  const { offsets } = getOffsetsForValue(value);

  for (const { byteOffset, flag } of offsets) {
    buffer.writeUInt8(buffer.readUInt8(byteOffset) | flag, byteOffset);
  }
}

export function checkIfValuePossibleInBloomFilter(buffer: Buffer, { offsets }: BloomFilterValueOffset): boolean {
  for (const { byteOffset, flag } of offsets) {
    const byteValue = buffer.readUInt8(byteOffset);

    if ((byteValue & flag) === 0) {
      return false;
    }
  }

  return true;
}

export function mergeBloomFilters(buffer1: Buffer, buffer2: Buffer): Buffer {
  if (buffer1.length !== BLOOM_BYTES_SIZE || buffer2.length !== BLOOM_BYTES_SIZE) {
    throw new Error();
  }

  const buffer = createBloomFilter();

  for (let offset = 0; offset < BLOOM_BYTES_SIZE; offset += 2) {
    const merged = buffer1.readUint16BE(offset) | buffer2.readUint16BE(offset);
    buffer.writeUint16BE(merged, offset);
  }

  return buffer;
}

const flags = [
  0,
  1,
  2,
  4,
  8,
  16,
  32,
  64,
  128,
  256,
  512,
  1024,
  2048,
  4096,
  8192,
  16384,
];

export function getBloomFilterFillRatio(buffer: Buffer): number {
  let sum = 0;

  for (let offset = 0; offset < BLOOM_BYTES_SIZE; offset += 2) {
    const value = buffer.readUint16BE(offset);

    for (let i = 0; i < 16; i += 1) {
      if (value & flags[i]) {
        sum += 1;
      }
    }
  }

  return sum / RANGE;
}
