export type AsyncTask<TTask, TResult> = TTask & {
  readonly resolve: (result: TResult) => void
  readonly reject: (err: Error) => void
}

export function createAsyncBatchProcessor<TTask, TResult>(
  processBatch: (batch: AsyncTask<TTask, TResult>[]) => Promise<void>,
  batchSize: number
) {
  const processTask = createBatchProcessor(processBatch, batchSize)

  return (task: TTask) => {
    return new Promise<TResult>((resolve, reject) => {
      processTask({...task, resolve, reject})
    })
  }
}

function createBatchProcessor<TTask>(processBatch: (batch: TTask[]) => Promise<void>, batchSize: number) {
  const taskQueue = [] as TTask[]
  let processing = false

  async function pulseTaskQueue(): Promise<void> {
    if (processing) {
      return
    }

    processing = true

    while (taskQueue.length) {
      const tasks = taskQueue.splice(0, batchSize)
      await processBatch(tasks)
    }

    processing = false
  }

  return (task: TTask): void => {
    taskQueue.push(task)
    process.nextTick(pulseTaskQueue)
  }
}
