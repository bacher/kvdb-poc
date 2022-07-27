import type { DatabaseInstance } from '../engines/common/types';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

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
          throw new Error(`Incorrect value at ${key}, "${alreadyValues.get(key)}" !== "${value}"`);
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

    let sumReadTime = 0;
    let sumWriteTime = 0;

    for (let i = 0; i < reportStats.readTimes.length; i++) {
      sumReadTime += reportStats.readTimes[i];
    }

    for (let i = 0; i < reportStats.writeTimes.length; i++) {
      sumWriteTime += reportStats.writeTimes[i];
    }

    const avgRead = sumReadTime * 1000 / reportStats.readTimes.length;
    const avgWrite = sumWriteTime * 1000 / reportStats.writeTimes.length;

    const readFormatted = `${Math.round(avgRead)}mks`.padStart(8, ' ');
    const writeFormatted = `${Math.round(avgWrite)}mks`.padStart(8, ' ');

    console.log(`Stats: avg read ${readFormatted}, avg write ${writeFormatted}.`);
  }, 1000);
}

export function runGenerator(db: DatabaseInstance): void {

  runStatsReporter();

  runThread(db).catch(error => {
    console.error('Generator:', error);
  });

}
