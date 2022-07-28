declare type Buffer = any

type ReadAccessCallback = () => Promise<Buffer>;
type WriteAccessCallback = () => Promise<void>;

type State = {
  writingInProcess: boolean,
  readInProcess: number,
  waitForRead: ReadAccessCallback[],
  waitForWrite: WriteAccessCallback[],
}

type AccessEntry = {
  getReadAccess: (callback: ReadAccessCallback) => Buffer;
  getWriteAccess: (callback: WriteAccessCallback) => Buffer;
}

export function initRWLock() {
  const accessMap = new Map<string, AccessEntry>();

  function getAccess(filePath: string) {
    let accessInfo = accessMap.get(filePath);

    if (!accessInfo) {
      const state: State = {
        writingInProcess: false,
        readInProcess: 0,
        waitForRead: [],
        waitForWrite: [],
      };

      function actualize() {
        if (state.writingInProcess) {
          return;
        }

        if (state.waitForWrite.length > 0) {
          if (state.readInProcess === 0) {
            state.writingInProcess = true;
            const writeCallback = state.waitForWrite.shift()!;
            writeCallback().then(() => {
              state.writingInProcess = false;
              actualize();
            });
          }
          return;
        }

        if (state.waitForRead.length > 0) {
          for (const callback of state.waitForRead) {
            state.readInProcess += 1;
            callback().then(() => {
              state.readInProcess -= 1;
              actualize();
            });
          }

          state.waitForRead = [];
        }
      }

      accessInfo = {
        getReadAccess: async (callback: ReadAccessCallback): Promise<Buffer> => {
          return new Promise<Buffer>(resolve => {
            state.waitForRead.push(async () => {
              resolve(await callback());
            });
            actualize();
          });
        },
        getWriteAccess: async (callback: WriteAccessCallback): Promise<void> => {
          await new Promise<void>(resolve => {
            state.waitForWrite.push(async () => {
              await callback();
              resolve();
            });
            actualize();
          });
        },
      };

      accessMap.set(filePath, accessInfo);
    }

    return accessInfo;
  }

  return getAccess;
}
