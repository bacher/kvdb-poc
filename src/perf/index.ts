import type { DatabaseInstance } from '../engines/common/types';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

export async function runThread(db: DatabaseInstance) {
  while (true) {
    const key = Math.floor(Math.random() * 1000).toString();

    if (Math.random() < 0.5) {
      await db.set(key, Math.random().toString());
    } else {
      await db.get(key);
    }

    await sleep(10);
  }
}

export function runGenerator(db: DatabaseInstance): void {

  runThread(db).catch(error => {
    console.error('Generator:', error);
  });


}
