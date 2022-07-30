import fs from 'fs/promises';
import { times } from 'lodash';

import type { DatabaseInstance } from '../engines/common/types';
import { sleep } from '../utils/time';

const THREADS_COUNT = 3;
const REPORT_EVERY_MS = 1000;

let stats = {
  readTimes: [] as number[],
  readsInProcess: 0,
  writeTimes: [] as number[],
  writesInProcess: 0,
};

type InMemoryState = {
  alreadyValues: Map<string, string | undefined>;
  alreadyKeys: string[],
}

async function runThread(db: DatabaseInstance, inMemoryState: InMemoryState) {
  const { alreadyValues, alreadyKeys } = inMemoryState;

  while (true) {
    let key: string;

    if (Math.random() < 0.8 && alreadyKeys.length > 1) {
      key = alreadyKeys[Math.floor(Math.random() * alreadyKeys.length)];
    } else {
      let keyLength = 3;

      if (Math.random() < 0.1) {
        keyLength = 4;
      }
      key = Math.floor(Math.random() * 10 ** keyLength).toString();
    }

    if (Math.random() < 0.3) {
      const value = Math.random().toString();
      stats.writesInProcess += 1;
      const start = performance.now();
      await db.set(key, value);
      const end = performance.now();
      stats.writesInProcess -= 1;
      stats.writeTimes.push(end - start);

      if (!alreadyValues.has(key)) {
        alreadyKeys.push(key);
      }
      alreadyValues.set(key, value);
    } else {
      const valueBeforeGet = alreadyValues.get(key);

      stats.readsInProcess += 1;
      const start = performance.now();
      const value = await db.get(key);
      const end = performance.now();
      stats.readsInProcess -= 1;
      stats.readTimes.push(end - start);

      if (alreadyValues.has(key)) {
        if (alreadyValues.get(key) !== value) {
          if (valueBeforeGet !== undefined && valueBeforeGet === value) {
            console.log('Equals to value before get');
          } else {
            throw new Error(`Incorrect value at ${key}, (${alreadyValues.get(key)}) !== (${value})`);
          }
        }
      } else {
        alreadyValues.set(key, value);
      }
    }

    await sleep(10);
  }
}

function runStatsReporter() {
  setInterval(() => {
    const {
      readTimes,
      writeTimes,
    } = stats;

    stats.readTimes = [];
    stats.writeTimes = [];

    let avgRead = '-';
    let avgWrite = '-';

    if (readTimes.length > 0) {
      let sumReadTime = 0;

      for (let i = 0; i < readTimes.length; i++) {
        sumReadTime += readTimes[i];
      }

      avgRead = `${Math.round(sumReadTime * 1000 / readTimes.length).toFixed(0)}mks`;
    }

    if (writeTimes.length > 0) {
      let sumWriteTime = 0;

      for (let i = 0; i < writeTimes.length; i++) {
        sumWriteTime += writeTimes[i];
      }

      avgWrite = `${Math.round(sumWriteTime * 1000 / readTimes.length).toFixed(0)}mks`;
    }

    const readFormatted = avgRead.padStart(8, ' ');
    const writeFormatted = avgWrite.padStart(8, ' ');

    const statsString = `Stats: avg read ${readFormatted}, avg write ${writeFormatted}.
Reads in process: ${stats.readsInProcess}, writes in process: ${stats.writesInProcess}.
`;

    //console.log(statsString);

    fs.writeFile('stats/perf.txt', statsString);

  }, REPORT_EVERY_MS);
}

export function runGenerator(db: DatabaseInstance): void {
  runStatsReporter();

  const inmemoryState: InMemoryState = {
    alreadyValues: new Map(),
    alreadyKeys: [],
  };

  times(THREADS_COUNT, () => {
    runThread(db, inmemoryState).catch(error => {
      console.error('Generator:', error);
      process.exit(10);
    });
  });
}
