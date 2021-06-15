import {outputQueueName} from './config'

interface QueueState {
  queueName: string
  partitionKeys: Set<string>
  messageCount: number
}

interface Session {
  partitionKey: string
  queueName: string
  messageCount: number
}

export class QStateWithLimits {
  private readonly qState: QState
  private readonly queueSizeLimit: number
  private readonly singlePartitionKeyLimit: number

  constructor(
    ackMessages: (deliveryTag1: number, deliveryTag2: number) => Promise<void>,
    queueCount: number,
    onMessageProcessed: () => void,
    queueSizeLimit: number,
    singlePartitionKeyLimit: number
  ) {
    this.qState = new QState(ackMessages, queueCount, onMessageProcessed)
    this.queueSizeLimit = queueSizeLimit
    this.singlePartitionKeyLimit = singlePartitionKeyLimit
  }

  public canPublish(partitionKey: string) {
    const sessionsByPartitionKey = this.qState.sessionsByPartitionKey
    const queueStatesByQueueName = this.qState.queueStatesByQueueName
    const queueStates = this.qState.queueStates

    const session = sessionsByPartitionKey.get(partitionKey)

    if (!session) {
      // TODO: totalMessageCount < queueSizeLimit * queueCount
      return queueStates.some(q => q.messageCount < this.queueSizeLimit)
    }

    if (session.messageCount >= this.singlePartitionKeyLimit) {
      return false
    }

    // TODO: use session.queueState for performance reason
    const queueState = queueStatesByQueueName.get(session.queueName)!

    return queueState.messageCount < this.queueSizeLimit
  }

  public async registerOutputDeliveryTag(messageId: string, deliveryTag: number) {
    await this.qState.registerOutputDeliveryTag(messageId, deliveryTag)
  }

  public async registerResponseDeliveryTag(messageId: string, deliveryTag: number) {
    await this.qState.registerResponseDeliveryTag(messageId, deliveryTag)
  }

  public registerOutputMessage(messageId: string, partitionKey: string, queueName?: string): {queueName: string} {
    return this.qState.registerOutputMessage(messageId, partitionKey, queueName)
  }
}

export class QState {
  private readonly ackMessages: (deliveryTag1: number, deliveryTag2: number) => Promise<void>
  private readonly onMessageProcessed: () => void

  private readonly outputDeliveryTagsByMessageId = new Map<string, number>()
  private readonly responseDeliveryTagsByMessageId = new Map<string, number>()
  private readonly partitionKeysByMessageId = new Map<string, string>()
  public readonly sessionsByPartitionKey = new Map<string, Session>()
  public readonly queueStatesByQueueName = new Map<string, QueueState>()
  public readonly queueStates = [] as QueueState[]
  // private readonly queueStatesByPartitionKey = new Map<string, QueueState>()

  constructor(
    ackMessages: (deliveryTag1: number, deliveryTag2: number) => Promise<void>,
    queueCount: number,
    onMessageProcessed: () => void
  ) {
    this.ackMessages = ackMessages
    this.onMessageProcessed = onMessageProcessed

    const queueNames = Array.from({length: queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueState = {queueName, partitionKeys: new Set<string>(), messageCount: 0}
      this.queueStates.push(queueState)
      this.queueStatesByQueueName.set(queueName, queueState)
    }
  }

  public async registerOutputDeliveryTag(messageId: string, deliveryTag: number) {
    const responseDeliveryTag = this.responseDeliveryTagsByMessageId.get(messageId)
    if (responseDeliveryTag !== undefined) {
      this.responseDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, deliveryTag, responseDeliveryTag)
    } else {
      this.outputDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public async registerResponseDeliveryTag(messageId: string, deliveryTag: number) {
    const outputDeliveryTag = this.outputDeliveryTagsByMessageId.get(messageId)
    if (outputDeliveryTag !== undefined) {
      this.outputDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, outputDeliveryTag, deliveryTag)
    } else {
      this.responseDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public registerOutputMessage(messageId: string, partitionKey: string, queueName?: string): {queueName: string} {
    const session = this.getOrAddSessions(partitionKey, queueName)
    const queueState = this.getOrAddQueueState(session.queueName)
    // TODO: ? Assert queueState + session limits

    session.messageCount += 1
    queueState.messageCount += 1

    if (session.messageCount === 1) {
      queueState.partitionKeys.add(partitionKey)
    }

    this.partitionKeysByMessageId.set(messageId, partitionKey)

    return {queueName: session.queueName}
  }

  private getOrAddSessions(partitionKey: string, queueName?: string): Session {
    let session = this.sessionsByPartitionKey.get(partitionKey)
    if (session && queueName && session.queueName !== queueName) {
      throw new Error(
        `Can't bind partition key "${partitionKey}" to queue "${queueName}" ` +
          `because it's already bound to queue "${session.queueName}"`
      )
    }

    if (!session) {
      queueName = queueName || this.getQueueNameWithMinMessageCount()
      session = {partitionKey, queueName, messageCount: 0}
      this.sessionsByPartitionKey.set(partitionKey, session)
    }

    return session
  }

  private getOrAddQueueState(queueName: string) {
    let queueState = this.queueStatesByQueueName.get(queueName)
    if (!queueState) {
      throw new Error(`Unknown queue "${queueName}"`)
    }
    return queueState
  }

  private getQueueNameWithMinMessageCount() {
    let result = this.queueStates[0]
    for (let i = 1; i < this.queueStates.length; i++) {
      if (this.queueStates[i].messageCount < result.messageCount) {
        result = this.queueStates[i]
      }
    }

    return result.queueName
  }

  private async processMessage(messageId: string, outputDeliveryTag: number, responseDeliveryTag: number) {
    await this.ackMessages(outputDeliveryTag, responseDeliveryTag)

    const partitionKey = this.partitionKeysByMessageId.get(messageId)!
    this.partitionKeysByMessageId.delete(messageId)

    const session = this.sessionsByPartitionKey.get(partitionKey)!
    session.messageCount -= 1
    if (!session.messageCount) {
      this.sessionsByPartitionKey.delete(partitionKey)
    }

    const queueState = this.queueStatesByQueueName.get(session.queueName)!
    queueState.messageCount -= 1
    if (!session.messageCount) {
      queueState.partitionKeys.delete(session.partitionKey)
    }

    this.onMessageProcessed()
  }
}
