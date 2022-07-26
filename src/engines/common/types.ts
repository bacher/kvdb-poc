export type DatabaseInstance = {
  get: (key: string) => Promise<string | undefined>,
  set: (key: string, value: string | undefined) => Promise<void>,
}

export type DatabaseOptions = {
  dataPath: string
}
