import {promisify} from 'util'

export async function measureAsync<TResult>(action: () => Promise<TResult>): Promise<[number, TResult]> {
  const startedAt = Date.now()
  const result = await action()
  const duration = Date.now() - startedAt

  return [duration, result]
}

export const setTimeoutAsync = promisify(setTimeout)
