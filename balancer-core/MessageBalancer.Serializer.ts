import {measureAsync} from './utils'

export default class ExecutionSerializer {
  private readonly lastExecutionPromiseByName = new Map<string, Promise<unknown>>()

  public async serializeExecution<TResult>(key: string, action: () => Promise<TResult>): Promise<[number, TResult]> {
    const lastExecutionPromise = this.lastExecutionPromiseByName.get(key) || Promise.resolve()
    const executionPromise = lastExecutionPromise.then(() => action())
    this.lastExecutionPromiseByName.set(key, executionPromise)

    const [duration] = await measureAsync(() => lastExecutionPromise)
    return [duration, await executionPromise]
  }

  public async serializeResolution<TResult>(key: string, promise: Promise<TResult>): Promise<[number, TResult]> {
    return this.serializeExecution(key, () => promise)
  }
}
