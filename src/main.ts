import { runEngine } from './engines/log';
import { runGenerator } from './perf';

export async function run() {

  const logStorage = await runEngine({
    dataPath: './data',
  });

  runGenerator(logStorage);

}
