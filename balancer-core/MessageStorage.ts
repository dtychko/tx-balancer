import Db, {FullOrPartialMessage, Message, MessageData, ReadMessagesOrderedByIdSpec} from './Db'
import {emptyLogger, MessageBalancerLogger} from './MessageBalancer.Logger'
import {emptyMetric, MessageBalancerSummaryMetric} from './MessageBalancer.Metrics'
import {AsyncTask, createAsyncBatchProcessor} from './MessageStorage.BatchProcessor'
import {measureAsync} from './utils'

type CreateMessageTaskAsync = AsyncTask<CreateMessageTask, Message>

interface CreateMessageTask {
  readonly partitionGroup: string
  readonly partitionKey: string
  readonly content: Buffer
  readonly properties?: unknown
  readonly receivedDate: Date
}

type RemoveMessageTaskAsync = AsyncTask<RemoveMessageTask, void>

interface RemoveMessageTask {
  readonly messageId: number
}

type ReadMessageTaskAsync = AsyncTask<ReadMessageTask, Message | undefined>

interface ReadMessageTask {
  readonly messageId: number
}

type ReadMessagesOrderedByIdTaskAsync = AsyncTask<ReadMessagesOrderedByIdTask, Message[]>

interface ReadMessagesOrderedByIdTask {
  readonly partitionGroup: string
  readonly partitionKey: string
  readonly minMessageId: number
  readonly limit: number
}

type ScheduleTask<TTask, TResult = void> = (task: TTask) => Promise<TResult>

const defaultBatchSize = 100
const defaultOperationDurationWarnThreshold = 100

export default class MessageStorage {
  private readonly db: Db
  private readonly logger: MessageBalancerLogger
  private readonly operationDurationWarnThreshold: number
  private readonly createMessagesDurationMetric: MessageBalancerSummaryMetric
  private readonly updateMessageDurationMetric: MessageBalancerSummaryMetric
  private readonly removeMessagesDurationMetric: MessageBalancerSummaryMetric
  private readonly readMessagesDurationMetric: MessageBalancerSummaryMetric
  private readonly readMessagesOrderedByIdDurationMetric: MessageBalancerSummaryMetric
  private readonly scheduleCreateMessageTask: ScheduleTask<CreateMessageTask, Message>
  private readonly scheduleRemoveMessageTask: ScheduleTask<RemoveMessageTask>
  private readonly scheduleReadMessageTask: ScheduleTask<ReadMessageTask, Message | undefined>
  private readonly scheduleReadMessagesOrderedByIdTask: ScheduleTask<ReadMessagesOrderedByIdTask, Message[]>

  constructor(params: {
    db: Db
    logger?: MessageBalancerLogger
    batchSize?: number
    operationDurationWarnThreshold?: number
    createMessagesDurationMetric?: MessageBalancerSummaryMetric
    updateMessageDurationMetric?: MessageBalancerSummaryMetric
    removeMessagesDurationMetric?: MessageBalancerSummaryMetric
    readMessagesDurationMetric?: MessageBalancerSummaryMetric
    readMessagesOrderedByIdDurationMetric?: MessageBalancerSummaryMetric
  }) {
    const {
      db,
      logger,
      batchSize,
      operationDurationWarnThreshold,
      createMessagesDurationMetric,
      updateMessageDurationMetric,
      removeMessagesDurationMetric,
      readMessagesDurationMetric,
      readMessagesOrderedByIdDurationMetric
    } = {
      logger: emptyLogger,
      batchSize: defaultBatchSize,
      operationDurationWarnThreshold: defaultOperationDurationWarnThreshold,
      createMessagesDurationMetric: emptyMetric,
      updateMessageDurationMetric: emptyMetric,
      removeMessagesDurationMetric: emptyMetric,
      readMessagesDurationMetric: emptyMetric,
      readMessagesOrderedByIdDurationMetric: emptyMetric,
      ...params
    }

    this.db = db
    this.logger = logger
    this.operationDurationWarnThreshold = operationDurationWarnThreshold
    this.createMessagesDurationMetric = createMessagesDurationMetric
    this.updateMessageDurationMetric = updateMessageDurationMetric
    this.removeMessagesDurationMetric = removeMessagesDurationMetric
    this.readMessagesDurationMetric = readMessagesDurationMetric
    this.readMessagesOrderedByIdDurationMetric = readMessagesOrderedByIdDurationMetric
    this.scheduleCreateMessageTask = createAsyncBatchProcessor<CreateMessageTask, Message>(this.processCreateMessageTasks.bind(this), batchSize)
    this.scheduleRemoveMessageTask = createAsyncBatchProcessor<RemoveMessageTask, void>(this.processRemoveMessageTasks.bind(this), batchSize)
    this.scheduleReadMessageTask = createAsyncBatchProcessor<ReadMessageTask, Message | undefined>(this.processReadMessageTasks.bind(this), batchSize)
    this.scheduleReadMessagesOrderedByIdTask = createAsyncBatchProcessor<ReadMessagesOrderedByIdTask, Message[]>(this.processReadMessagesOrderedByIdTasks.bind(this), batchSize)
  }

  public createMessage(message: MessageData) {
    return this.scheduleCreateMessageTask(message)
  }

  public async updateMessage(message: {messageId: number; properties?: unknown}) {
    const {messageId, properties} = message

    try {
      const [duration] = await measureAsync(() => this.db.updateMessageProperties({messageId, properties}))

      this.updateMessageDurationMetric.observe(duration)

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Updating message#${messageId} completed in ${duration} ms`,
          eventType: 'message-storage/update-message/completed',
          duration
        })
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to update message#${messageId}`,
        eventType: 'message-storage/update-message/unexpected-error',
        error
      })

      throw error
    }
  }

  public removeMessage(messageId: number) {
    return this.scheduleRemoveMessageTask({messageId})
  }

  public readMessage(messageId: number) {
    return this.scheduleReadMessageTask({messageId})
  }

  public readMessagesOrderedById(spec: ReadMessagesOrderedByIdSpec) {
    return this.scheduleReadMessagesOrderedByIdTask(spec)
  }

  public async readPartitionGroupMessagesOrderedById(spec: {zeroBasedPage: number; pageSize: number; contentSizeLimit: number}): Promise<Map<string, FullOrPartialMessage[]>> {
    // TODO: Add logging/diagnostics/errorHandling
      const {zeroBasedPage, pageSize, contentSizeLimit} = spec
      return await this.db.readPartitionGroupMessagesOrderedById(zeroBasedPage, pageSize, contentSizeLimit)
  }

  public async readAllPartitionMessagesOrderedById(spec: {partitionGroup: string; partitionSize: number}): Promise<Map<string, Message[]>> {
    try {
      const {partitionGroup, partitionSize} = spec
      const [duration, result] = await measureAsync(() => this.db.readAllPartitionMessagesOrderedById(partitionGroup, partitionSize))

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Reading all partition messages for group "${partitionGroup}" completed in ${duration} ms`,
          eventType: 'message-storage/read-all-partition-messages-ordered-by-id/completed',
          duration
        })
      }

      return result
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to read all partition messages ordered by id`,
        eventType: 'message-storage/read-all-partition-messages-ordered-by-id/unexpected-error',
        error
      })

      throw error
    }
  }

  private async processCreateMessageTasks(tasks: CreateMessageTaskAsync[]): Promise<void> {
    try {
      const [duration, messageIds] = await measureAsync(() => this.db.createMessages(tasks))

      this.createMessagesDurationMetric.observe(duration)

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Processing ${tasks.length} create message tasks in ${duration} ms`,
          eventType: 'message-storage/process-create-message-tasks/completed',
          duration,
          meta: {taskCount: tasks.length}
        })
      }

      for (let i = 0; i < tasks.length; i++) {
        // eslint-disable-next-line security/detect-object-injection
        const {partitionGroup, partitionKey, content, properties, receivedDate, resolve} = tasks[i]
        // eslint-disable-next-line security/detect-object-injection
        resolve({messageId: messageIds[i], partitionGroup, partitionKey, content, properties, receivedDate})
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to process ${tasks.length} create message tasks`,
        eventType: 'message-storage/process-create-message-tasks/unexpected-error',
        error,
        meta: {taskCount: tasks.length}
      })

      for (const task of tasks) {
        task.reject(error)
      }
    }
  }

  private async processRemoveMessageTasks(tasks: RemoveMessageTaskAsync[]): Promise<void> {
    try {
      const messageIds = tasks.map(task => task.messageId)
      const [duration] = await measureAsync(() => this.db.removeMessages(messageIds))

      this.removeMessagesDurationMetric.observe(duration)

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Processing ${tasks.length} remove message tasks in ${duration} ms`,
          eventType: 'message-storage/process-remove-message-tasks/completed',
          duration,
          meta: {taskCount: tasks.length}
        })
      }

      for (const task of tasks) {
        task.resolve()
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to process ${tasks.length} remove message tasks`,
        eventType: 'message-storage/process-remove-message-tasks/unexpected-error',
        error,
        meta: {taskCount: tasks.length}
      })

      for (const task of tasks) {
        task.reject(error)
      }
    }
  }

  private async processReadMessageTasks(tasks: ReadMessageTaskAsync[]): Promise<void> {
    try {
      const messageIds = tasks.map(x => x.messageId)
      const [duration, messages] = await measureAsync(() => this.db.readMessages(messageIds))

      this.readMessagesDurationMetric.observe(duration)

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Processing ${tasks.length} read message tasks in ${duration} ms`,
          eventType: 'message-storage/process-read-message-tasks/completed',
          duration,
          meta: {taskCount: tasks.length}
        })
      }

      const messageById = messages.reduce((acc, message) => {
        return acc.set(message.messageId, message)
      }, new Map<number, Message>())

      for (const task of tasks) {
        task.resolve(messageById.get(task.messageId))
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to process ${tasks.length} read message tasks`,
        eventType: 'message-storage/process-read-message-tasks/unexpected-error',
        error,
        meta: {taskCount: tasks.length}
      })

      for (const task of tasks) {
        task.reject(error)
      }
    }
  }

  private async processReadMessagesOrderedByIdTasks(tasks: ReadMessagesOrderedByIdTaskAsync[]) {
    try {
      const [duration, messages] = await measureAsync(() => this.db.readMessagesOrderedById(tasks))

      this.readMessagesOrderedByIdDurationMetric.observe(duration)

      if (duration > this.operationDurationWarnThreshold) {
        this.logger.log({
          level: 'warn',
          message: `Processing ${tasks.length} read messages ordered by id tasks in ${duration} ms`,
          eventType: 'message-storage/process-read-messages-ordered-by-id-tasks/completed',
          duration,
          meta: {taskCount: tasks.length}
        })
      }

      for (let i = 0; i < tasks.length; i++) {
        // eslint-disable-next-line security/detect-object-injection
        tasks[i].resolve(messages[i])
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to process ${tasks.length} read messages ordered by id tasks`,
        eventType: 'message-storage/process-read-messages-ordered-by-id-tasks/unexpected-error',
        error,
        meta: {taskCount: tasks.length}
      })

      for (const task of tasks) {
        task.reject(error)
      }
    }
  }
}
