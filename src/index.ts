import { runLSM } from './lsm';
import { runGenerator } from './generator';

const lsm = runLSM({
  dataPath: './data'
});

runGenerator(lsm);
