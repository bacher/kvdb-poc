export type DatabaseInstance = {
  get: (key: string) => string | undefined,
  set: (key: string, value: string | undefined) => Promise<void>,
}

export type DatabaseOptions = {
  dataPath: string
}
