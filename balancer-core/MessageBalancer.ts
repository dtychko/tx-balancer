import Centrifuge from './Centrifuge'
import {Message, MessageData} from './Db'
import {emptyLogger, MessageBalancerLogger} from './MessageBalancer.Logger'
import {MessageBalancerGaugeMetric, MessageBalancerSummaryMetric} from './MessageBalancer.Metrics'
import ExecutionSerializer from './MessageBalancer.Serializer'
import MessageCache from './MessageCache'
import MessageStorage from './MessageStorage'
import {measureAsync} from './utils'

export type ProcessMethod<TProp> = (message: Message<TProp>) => Promise<ProcessMethodResult<TProp>>

type ProcessMethodResult<TProp> = void | {type: 'Ok'} | {type: 'Requeue'; update?: {properties?: TProp}}

interface PartitionDescriptor {
  readonly isAppendable: boolean
  readonly maxMessageId: number
  readonly lastStoredMessageId: number
}

const defaultPartitionDescriptor = {
  isAppendable: true,
  maxMessageId: -1,
  lastStoredMessageId: -1
}

const defaultPartitionSizeLimit = 100

export default class MessageBalancer<TProp = unknown> {
  public readonly partitionGroup: string
  public readonly partitionSizeLimit: number
  private readonly lockPartition: boolean
  private readonly syncMessageRemove: boolean
  private readonly partitionDescriptorByPartitionKey = new Map<string, PartitionDescriptor>()
  private readonly centrifuge = new Centrifuge<number>()
  private readonly storage: MessageStorage
  private readonly cache?: MessageCache
  private readonly logger: MessageBalancerLogger
  private readonly endToEndMessageProcessingDurationMetric?: MessageBalancerSummaryMetric
  private readonly messageProcessingDurationMetric?: MessageBalancerSummaryMetric
  private readonly centrifugePartitionCountMetric?: MessageBalancerGaugeMetric
  private readonly centrifugeMessageCountMetric?: MessageBalancerGaugeMetric
  private readonly executorWaitingTimeMetric?: MessageBalancerSummaryMetric
  private readonly executor = new ExecutionSerializer()
  private initialized: boolean = false

  constructor(params: {
    partitionGroup: string
    storage: MessageStorage
    cache?: MessageCache
    logger?: MessageBalancerLogger
    partitionSizeLimit?: number
    lockPartition?: boolean
    syncMessageRemove?: boolean
    endToEndMessageProcessingDurationMetric?: MessageBalancerSummaryMetric
    messageProcessingDurationMetric?: MessageBalancerSummaryMetric
    centrifugePartitionCountMetric?: MessageBalancerGaugeMetric
    centrifugeMessageCountMetric?: MessageBalancerGaugeMetric
    executorWaitingTimeMetric?: MessageBalancerSummaryMetric
  }) {
    const {
      partitionGroup,
      storage,
      cache,
      logger,
      partitionSizeLimit,
      lockPartition,
      syncMessageRemove,
      endToEndMessageProcessingDurationMetric,
      messageProcessingDurationMetric,
      centrifugePartitionCountMetric,
      centrifugeMessageCountMetric,
      executorWaitingTimeMetric
    } = {
      logger: emptyLogger,
      partitionSizeLimit: defaultPartitionSizeLimit,
      lockPartition: false,
      syncMessageRemove: false,
      ...params
    }

    this.partitionGroup = partitionGroup
    this.storage = storage
    this.cache = cache
    this.logger = logger
    this.partitionSizeLimit = partitionSizeLimit
    this.lockPartition = lockPartition
    this.syncMessageRemove = syncMessageRemove
    this.endToEndMessageProcessingDurationMetric = endToEndMessageProcessingDurationMetric
    this.messageProcessingDurationMetric = messageProcessingDurationMetric
    this.centrifugePartitionCountMetric = centrifugePartitionCountMetric
    this.centrifugeMessageCountMetric = centrifugeMessageCountMetric
    this.executorWaitingTimeMetric = executorWaitingTimeMetric
  }

  public async init(): Promise<void> {
    try {
      const [duration] = await measureAsync(async () => {
        const messagesByPartitionKey = await this.storage.readAllPartitionMessagesOrderedById({
          partitionGroup: this.partitionGroup,
          partitionSize: this.partitionSizeLimit
        })

        for (const [partitionKey, messages] of messagesByPartitionKey) {
          this.enqueueMessagesToCentrifugeAndCache(partitionKey, messages)
        }

        this.initialized = true
      })

      this.logger.log({
        level: 'info',
        message: `Initializing completed in ${duration} ms`,
        eventType: 'message-balancer/init/completed',
        duration
      })
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: 'Unable to initialize message balancer',
        eventType: 'message-balancer/init/unexpected-error',
        error
      })

      throw error
    }
  }

  public async storeMessage(messageData: Omit<MessageData<TProp>, 'partitionGroup' | 'receivedDate'>): Promise<number> {
    try {
      this.assertInitialized()

      const message = await this.storage.createMessage({
        partitionGroup: this.partitionGroup,
        receivedDate: new Date(),
        ...messageData
      })
      const {messageId, partitionKey} = message
      const {isAppendable, maxMessageId} = this.getPartitionDescriptor(partitionKey)

      this.setPartitionDescriptor(partitionKey, {isAppendable, maxMessageId, lastStoredMessageId: messageId})

      // Make sure that partition is appendable and messageId wasn't added to centrifuge yet
      if (isAppendable && messageId > maxMessageId) {
        this.enqueueMessagesToCentrifugeAndCache(partitionKey, [message])
      }

      return messageId
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to store message in partition "${messageData.partitionKey}"`,
        eventType: 'message-balancer/store-message/unexpected-error',
        error
      })

      throw error
    }
  }

  public async processNextMessage(process: ProcessMethod<TProp>): Promise<void> {
    try {
      this.assertInitialized()

      await this.centrifuge.processNext(async ({value}) => {
        this.updateCentrifugeMetrics()

        const messageId = value
        const [waitingTime, message] = await this.executor.serializeResolution('getMessageById', this.getMessage(messageId))

        if (this.executorWaitingTimeMetric) {
          this.executorWaitingTimeMetric.observe({partition_group: this.partitionGroup, execution_group: 'getMessageById'}, waitingTime)
        }

        const {partitionGroup, partitionKey, content, properties, receivedDate} = message as Message<TProp>

        this.fillNonAppendablePartitionIfEmpty(partitionKey)

        const [duration, result] = await measureAsync(() => process({messageId, partitionGroup, partitionKey, content, properties, receivedDate}))

        if (!result || result.type === 'Ok') {
          if (this.endToEndMessageProcessingDurationMetric) {
            const processingDuration = new Date().getTime() - receivedDate.getTime()
            this.endToEndMessageProcessingDurationMetric.observe({partition_group: this.partitionGroup}, Math.max(0, processingDuration))
          }

          if (this.messageProcessingDurationMetric) {
            this.messageProcessingDurationMetric.observe({partition_group: this.partitionGroup}, duration)
          }

          const removeMessagePromise = this.removeMessage(messageId)

          if (this.syncMessageRemove) {
            await removeMessagePromise
          }

          return
        }

        if (result && result.type === 'Requeue') {
          if (result.update) {
            await this.updateMessage({messageId, properties: result.update.properties})
          }

          const updatedProperties = result.update ? result.update.properties : message.properties
          this.requeueMessageToCentrifugeAndCache(partitionKey, {...message, properties: updatedProperties})
          return
        }

        // tslint:disable-next-line:no-any
        throw new Error(`Unknown result type "${(result as any).type}"`)
      }, this.lockPartition)
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unable to process next message`,
        eventType: 'message-balancer/process-next-message/unexpected-error',
        error
      })

      throw error
    }
  }

  public partitionCount() {
    return this.centrifuge.partitionCount()
  }

  public messageCount() {
    return this.centrifuge.size()
  }

  public partitionKeys() {
    return this.centrifuge.partitionKeys()
  }

  public partitionStats(partitionKey: string) {
    const descriptor = this.partitionDescriptorByPartitionKey.get(partitionKey)
    if (descriptor === undefined) {
      return undefined
    }

    return {
      name: partitionKey,
      isLocked: this.centrifuge.isPartitionLocked(partitionKey),
      messageCount: this.centrifuge.partitionSize(partitionKey),
      isAppendable: descriptor.isAppendable,
      maxMessageId: descriptor.maxMessageId
    }
  }

  private async fillNonAppendablePartitionIfEmpty(partitionKey: string) {
    try {
      const [waitingTime, completed] = await this.executor.serializeExecution(`fillPartition:${partitionKey}`, async () => {
        const isEmpty = this.centrifuge.partitionSize(partitionKey) === 0
        if (!isEmpty) {
          return false
        }

        const {isAppendable, maxMessageId} = this.getPartitionDescriptor(partitionKey)
        if (isAppendable) {
          return false
        }

        const messages = await this.readMessagesOrderedById({
          partitionKey,
          minMessageId: maxMessageId + 1,
          limit: this.partitionSizeLimit
        })

        this.enqueueMessagesToCentrifugeAndCache(partitionKey, messages)

        return true
      })

      if (completed && this.executorWaitingTimeMetric) {
        this.executorWaitingTimeMetric.observe({partition_group: this.partitionGroup, execution_group: 'fillPartition'}, waitingTime)
      }
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unexpected error during filling non-appendable empty partition "${partitionKey}"`,
        eventType: 'message-balancer/fill-partition/unexpected-error',
        error
      })

      throw error
    }
  }

  private async readMessagesOrderedById(spec: {partitionKey: string; minMessageId: number; limit: number}): Promise<Message[]> {
    const {partitionKey, minMessageId, limit} = spec
    const messages = await this.storage.readMessagesOrderedById({
      partitionGroup: this.partitionGroup,
      partitionKey,
      minMessageId,
      limit
    })

    if (messages.length >= limit) {
      return messages
    }

    const lastStoredMessageId = this.getPartitionDescriptor(partitionKey).lastStoredMessageId
    const maxMessageId = messages.length ? messages[messages.length - 1].messageId : minMessageId - 1
    if (maxMessageId >= lastStoredMessageId) {
      return messages
    }

    // Most likely some new messages where stored in MessageBalancer when message reading operation was in progress,
    // so we need to read messages one more time to make sure these new messages are included in result set
    return [
      ...messages,
      ...(await this.readMessagesOrderedById({
        partitionKey,
        minMessageId: maxMessageId + 1,
        limit: limit - messages.length
      }))
    ]
  }

  private enqueueMessagesToCentrifugeAndCache(partitionKey: string, messages: Message[]) {
    const {maxMessageId, lastStoredMessageId} = this.getPartitionDescriptor(partitionKey)

    for (const {messageId} of messages) {
      if (messageId <= maxMessageId) {
        throw new Error(`Couldn't enqueue message that was already added to centrifuge before`)
      }

      this.centrifuge.enqueue(partitionKey, messageId)
    }

    this.setPartitionDescriptor(partitionKey, {
      isAppendable: this.centrifuge.partitionSize(partitionKey) < this.partitionSizeLimit,
      maxMessageId: messages.length ? Math.max(maxMessageId, messages[messages.length - 1].messageId) : maxMessageId,
      lastStoredMessageId
    })

    this.updateCentrifugeMetrics()
    this.addMessagesToCache(messages)
  }

  private requeueMessageToCentrifugeAndCache(partitionKey: string, message: Message) {
    const {maxMessageId} = this.getPartitionDescriptor(partitionKey)
    if (message.messageId > maxMessageId) {
      throw new Error(`Couldn't requeue message that wasn't added to centrifuge before`)
    }

    this.centrifuge.requeue(partitionKey, message.messageId)

    this.updateCentrifugeMetrics()
    this.addMessagesToCache([message])
  }

  private async updateMessage(message: {messageId: number; properties?: unknown}) {
    try {
      await this.storage.updateMessage(message)
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unexpected error during updating message#${message.messageId}`,
        eventType: 'message-balancer/update-message/unexpected-error',
        error
      })

      throw error
    }
  }

  private async removeMessage(messageId: number) {
    try {
      await this.storage.removeMessage(messageId)
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: `Unexpected error during removing message#${messageId}`,
        eventType: 'message-balancer/remove-message/unexpected-error',
        error
      })

      throw error
    }
  }

  private getPartitionDescriptor(partitionKey: string) {
    return this.partitionDescriptorByPartitionKey.get(partitionKey) || defaultPartitionDescriptor
  }

  private setPartitionDescriptor(partitionKey: string, descriptor: PartitionDescriptor) {
    this.partitionDescriptorByPartitionKey.set(partitionKey, descriptor)
  }

  private addMessagesToCache(messages: Message[]) {
    if (this.cache) {
      this.cache.addMessages(messages)
    }
  }

  private async getMessage(messageId: number): Promise<Message> {
    return this.getAndRemoveMessageFromCache(messageId) || (await this.storage.readMessage(messageId))!
  }

  private getAndRemoveMessageFromCache(messageId: number): Message | undefined {
    return this.cache ? this.cache.getAndRemoveMessage(messageId) : undefined
  }

  private updateCentrifugeMetrics() {
    if (this.centrifugePartitionCountMetric) {
      this.centrifugePartitionCountMetric.set({partition_group: this.partitionGroup}, this.centrifuge.partitionCount())
    }

    if (this.centrifugeMessageCountMetric) {
      this.centrifugeMessageCountMetric.set({partition_group: this.partitionGroup}, this.centrifuge.size())
    }
  }

  private assertInitialized() {
    if (!this.initialized) {
      throw new Error(`Message service isn't initialized`)
    }
  }
}
