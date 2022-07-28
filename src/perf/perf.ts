import fs from 'fs/promises';
import { times } from 'lodash';

import type { DatabaseInstance } from '../engines/common/types';
import { sleep } from '../utils/time';

const THREADS_COUNT = 3;
const REPORT_EVERY_MS = 5000;

let stats = {
  readTimes: [] as number[],
  writeTimes: [] as number[],
};


async function runThread(db: DatabaseInstance) {
  const alreadyValues = new Map();
  const alreadyKeys = [];

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
      const start = performance.now();
      await db.set(key, value);
      const end = performance.now();
      stats.writeTimes.push(end - start);

      if (!alreadyValues.has(key)) {
        alreadyKeys.push(key);
      }
      alreadyValues.set(key, value);
    } else {
      const start = performance.now();
      const value = await db.get(key);
      const end = performance.now();
      stats.readTimes.push(end - start);

      if (alreadyValues.has(key)) {
        if (alreadyValues.get(key) !== value) {
          // throw new Error(`Incorrect value at ${key}, (${alreadyValues.get(key)}) !== (${value})`);
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
    const reportStats = stats;

    stats = {
      readTimes: [],
      writeTimes: [],
    };

    let avgRead = '-';
    let avgWrite = '-';

    if (reportStats.readTimes.length > 0) {
      let sumReadTime = 0;

      for (let i = 0; i < reportStats.readTimes.length; i++) {
        sumReadTime += reportStats.readTimes[i];
      }

      avgRead = `${Math.round(sumReadTime * 1000 / reportStats.readTimes.length).toFixed(0)}mks`;
    }

    if (reportStats.writeTimes.length > 0) {
      let sumWriteTime = 0;

      for (let i = 0; i < reportStats.writeTimes.length; i++) {
        sumWriteTime += reportStats.writeTimes[i];
      }

      avgWrite = `${Math.round(sumWriteTime * 1000 / reportStats.readTimes.length).toFixed(0)}mks`;
    }

    const readFormatted = avgRead.padStart(8, ' ');
    const writeFormatted = avgWrite.padStart(8, ' ');

    const statsString = `Stats: avg read ${readFormatted}, avg write ${writeFormatted}.`;

    //console.log(statsString);

    fs.writeFile('stats/perf.txt', statsString);

  }, REPORT_EVERY_MS);
}

export function runGenerator(db: DatabaseInstance): void {
  runStatsReporter();

  times(THREADS_COUNT, () => {
    runThread(db).catch(error => {
      console.error('Generator:', error);
      process.exit(10);
    });
  });


}
