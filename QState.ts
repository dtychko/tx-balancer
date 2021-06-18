import {outputQueueName} from './config'

interface QueueState {
  queueName: string
  messageCount: number
}

interface Session {
  partitionGroup: string
  queueState: QueueState
  messageCount: number
}

interface QStateParams {
  onMessageProcessed: (messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) => void
  queueCount: number
  queueSizeLimit: number
  singlePartitionGroupLimit: number
}

export class QState {
  private readonly onMessageProcessed: QStateParams['onMessageProcessed']
  private readonly queueSizeLimit: number
  private readonly singlePartitionGroupLimit: number

  private readonly mirrorDeliveryTagsByMessageId = new Map<string, number>()
  private readonly responseDeliveryTagsByMessageId = new Map<string, number>()
  private readonly partitionGroupsByMessageId = new Map<string, string>()
  private readonly sessionsByPartitionGroup = new Map<string, Session>()
  private readonly queueStatesByQueueName = new Map<string, QueueState>()
  private readonly queueStates = [] as QueueState[]

  constructor(params: QStateParams) {
    this.onMessageProcessed = params.onMessageProcessed
    this.queueSizeLimit = params.queueSizeLimit
    this.singlePartitionGroupLimit = params.singlePartitionGroupLimit

    const queueNames = Array.from({length: params.queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueState = {queueName, messageCount: 0}
      this.queueStates.push(queueState)
      this.queueStatesByQueueName.set(queueName, queueState)
    }
  }

  public canPublish(partitionGroup: string) {
    const session = this.sessionsByPartitionGroup.get(partitionGroup)

    if (!session) {
      return this.getQueueState(partitionGroup).messageCount < this.queueSizeLimit
    }

    return session.queueState.messageCount < this.queueSizeLimit && session.messageCount < this.singlePartitionGroupLimit
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

  public restoreMessage(messageId: string, partitionGroup: string, partitionKey: string, queueName: string) {
    const expectedQueueName = this.getQueueState(partitionGroup).queueName

    // TODO: Maybe move this check to the caller side and just log warning instead of throwing
    if (queueName !== expectedQueueName) {
      throw new Error(
        `Can't bind partition group "${partitionGroup}" to queue "${queueName}" ` +
          `because it's already bound to queue "${expectedQueueName}"`
      )
    }

    this.registerMessage(messageId, partitionGroup, partitionKey)
  }

  public registerMessage(messageId: string, partitionGroup: string, _partitionKey: string): {queueName: string} {
    const session = this.getOrAddSessions(partitionGroup)

    // TODO: ? Assert queueState + session limits

    session.messageCount += 1
    session.queueState.messageCount += 1

    this.partitionGroupsByMessageId.set(messageId, partitionGroup)

    return {queueName: session.queueState.queueName}
  }

  private getOrAddSessions(partitionGroup: string): Session {
    let session = this.sessionsByPartitionGroup.get(partitionGroup)

    if (!session) {
      const queueState = this.getQueueState(partitionGroup)
      session = {partitionGroup: partitionGroup, queueState, messageCount: 0}
      this.sessionsByPartitionGroup.set(partitionGroup, session)
    }

    return session
  }

  private getQueueState(partitionGroup: string): QueueState {
    // Just sum partitionGroup char codes instead of calculating a complex hash
    const queueIndex = sumCharCodes(partitionGroup) % this.queueStates.length
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
    const partitionGroup = this.partitionGroupsByMessageId.get(messageId)!
    this.partitionGroupsByMessageId.delete(messageId)

    const session = this.sessionsByPartitionGroup.get(partitionGroup)!
    session.messageCount -= 1
    if (!session.messageCount) {
      this.sessionsByPartitionGroup.delete(partitionGroup)
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
