import {outputQueueName} from './config'

interface QueueState {
  queueName: string
  messageCount: number
}

interface Session {
  partitionKey: string
  queueState: QueueState
  messageCount: number
}

interface QStateParams {
  onMessageProcessed: (messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) => void
  queueCount: number
  queueSizeLimit: number
  singlePartitionKeyLimit: number
}

export class QState {
  private readonly onMessageProcessed: QStateParams['onMessageProcessed']
  private readonly queueSizeLimit: number
  private readonly singlePartitionKeyLimit: number

  private readonly mirrorDeliveryTagsByMessageId = new Map<string, number>()
  private readonly responseDeliveryTagsByMessageId = new Map<string, number>()
  private readonly partitionKeysByMessageId = new Map<string, string>()
  private readonly sessionsByPartitionKey = new Map<string, Session>()
  private readonly queueStatesByQueueName = new Map<string, QueueState>()
  private readonly queueStates = [] as QueueState[]

  constructor(params: QStateParams) {
    this.onMessageProcessed = params.onMessageProcessed
    this.queueSizeLimit = params.queueSizeLimit
    this.singlePartitionKeyLimit = params.singlePartitionKeyLimit

    const queueNames = Array.from({length: params.queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueState = {queueName, messageCount: 0}
      this.queueStates.push(queueState)
      this.queueStatesByQueueName.set(queueName, queueState)
    }
  }

  public canPublish(partitionKey: string) {
    const session = this.sessionsByPartitionKey.get(partitionKey)

    if (!session) {
      return this.getQueueState(partitionKey).messageCount < this.queueSizeLimit
    }

    return session.queueState.messageCount < this.queueSizeLimit && session.messageCount < this.singlePartitionKeyLimit
  }

  public async registerMirrorDeliveryTag(messageId: string, deliveryTag: number) {
    const responseDeliveryTag = this.responseDeliveryTagsByMessageId.get(messageId)
    if (responseDeliveryTag !== undefined) {
      this.responseDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, deliveryTag, responseDeliveryTag)
    } else {
      this.mirrorDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public async registerResponseDeliveryTag(messageId: string, deliveryTag: number) {
    const mirrorDeliveryTag = this.mirrorDeliveryTagsByMessageId.get(messageId)
    if (mirrorDeliveryTag !== undefined) {
      this.mirrorDeliveryTagsByMessageId.delete(messageId)
      await this.processMessage(messageId, mirrorDeliveryTag, deliveryTag)
    } else {
      this.responseDeliveryTagsByMessageId.set(messageId, deliveryTag)
    }
  }

  public restoreMessage(messageId: string, partitionKey: string, queueName: string) {
    const expectedQueueName = this.getQueueState(partitionKey).queueName

    // TODO: Maybe move this check to the caller side and just log warning instead of throwing
    if (queueName !== expectedQueueName) {
      throw new Error(
        `Can't bind partition key "${partitionKey}" to queue "${queueName}" ` +
          `because it's bound to queue "${expectedQueueName}"`
      )
    }

    this.registerMessage(messageId, partitionKey)
  }

  public registerMessage(messageId: string, partitionKey: string): {queueName: string} {
    const session = this.getOrAddSessions(partitionKey)

    // TODO: ? Assert queueState + session limits

    session.messageCount += 1
    session.queueState.messageCount += 1

    this.partitionKeysByMessageId.set(messageId, partitionKey)

    return {queueName: session.queueState.queueName}
  }

  private getOrAddSessions(partitionKey: string): Session {
    let session = this.sessionsByPartitionKey.get(partitionKey)

    if (!session) {
      const queueState = this.getQueueState(partitionKey)
      session = {partitionKey, queueState, messageCount: 0}
      this.sessionsByPartitionKey.set(partitionKey, session)
    }

    return session
  }

  private getQueueState(partitionKey: string): QueueState {
    // Just sum partitionKey char codes instead of calculating a complex hash
    const queueIndex = sumCharCodes(partitionKey) % this.queueStates.length
    return this.queueStates[queueIndex]
  }

  private getOrAddQueueState(queueName: string) {
    let queueState = this.queueStatesByQueueName.get(queueName)
    if (!queueState) {
      throw new Error(`Unknown queue "${queueName}"`)
    }
    return queueState
  }

  private async processMessage(messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) {
    const partitionKey = this.partitionKeysByMessageId.get(messageId)!
    this.partitionKeysByMessageId.delete(messageId)

    const session = this.sessionsByPartitionKey.get(partitionKey)!
    session.messageCount -= 1
    if (!session.messageCount) {
      this.sessionsByPartitionKey.delete(partitionKey)
    }

    session.queueState.messageCount -= 1

    this.onMessageProcessed(messageId, mirrorDeliveryTag, responseDeliveryTag)
  }
}

function sumCharCodes(str: string) {
  let sum = 0
  for (let i = 0; i < str.length; i++) {
    sum += str.charCodeAt(i)
  }
  return sum
}
