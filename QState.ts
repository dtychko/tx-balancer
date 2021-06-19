import {outputQueueName} from './config'

interface QueueSession {
  queueName: string
  messageCount: number
}

interface PartitionGroupSession {
  partitionGroup: string
  queueSession: QueueSession
  messageCount: number
  partitionKeySessions: Map<string, PartitionKeySession>
}

interface PartitionKeySession {
  partitionKey: string
  partitionGroupSession: PartitionGroupSession
  messageCount: number
}

interface MessageSession {
  partitionKeySession: PartitionKeySession
}

export interface PartitionGroupGuard {
  canProcessPartitionGroup: boolean
  canProcessPartitionKey: (partitionKey: string) => boolean
}

interface QStateParams {
  onMessageProcessed: (messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) => void
  queueCount: number
  queueSizeLimit: number
  singlePartitionGroupLimit: number
  singlePartitionKeyLimit: number
}

export class QState {
  private readonly onMessageProcessed: QStateParams['onMessageProcessed']
  private readonly queueSizeLimit: number
  private readonly singlePartitionGroupLimit: number
  private readonly singlePartitionKeyLimit: number

  private readonly mirrorDeliveryTagsByMessageId = new Map<string, number>()
  private readonly responseDeliveryTagsByMessageId = new Map<string, number>()
  private readonly messageSessionsByMessageId = new Map<string, MessageSession>()
  private readonly partitionGroupSessionsByPartitionGroup = new Map<string, PartitionGroupSession>()
  private readonly queueSessionsByQueueName = new Map<string, QueueSession>()
  private readonly queueSessions = [] as QueueSession[]

  constructor(params: QStateParams) {
    this.onMessageProcessed = params.onMessageProcessed
    this.queueSizeLimit = params.queueSizeLimit
    this.singlePartitionGroupLimit = params.singlePartitionGroupLimit
    this.singlePartitionKeyLimit = params.singlePartitionKeyLimit

    const queueNames = Array.from({length: params.queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueState = {queueName, messageCount: 0}
      this.queueSessions.push(queueState)
      this.queueSessionsByQueueName.set(queueName, queueState)
    }
  }

  public canRegister(partitionGroup: string): PartitionGroupGuard {
    const partitionGroupSession = this.partitionGroupSessionsByPartitionGroup.get(partitionGroup)

    if (!partitionGroupSession) {
      const canPublish = this.getQueueState(partitionGroup).messageCount < this.queueSizeLimit

      return {
        canProcessPartitionGroup: canPublish,
        canProcessPartitionKey: () => canPublish
      }
    }

    const canPublish =
      partitionGroupSession.queueSession.messageCount < this.queueSizeLimit &&
      partitionGroupSession.messageCount < this.singlePartitionGroupLimit

    if (canPublish) {
      return {
        canProcessPartitionGroup: true,
        canProcessPartitionKey: (partitionKey: string) => {
          const partitionKeySession = partitionGroupSession!.partitionKeySessions.get(partitionKey)
          if (!partitionKeySession) {
            return true
          }
          return partitionKeySession.messageCount < this.singlePartitionKeyLimit
        }
      }
    }

    return {
      canProcessPartitionGroup: false,
      canProcessPartitionKey: () => false
    }
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

  public registerMessage(messageId: string, partitionGroup: string, partitionKey: string): {queueName: string} {
    const partitionGroupSession = this.getOrAddPartitionGroupSession(partitionGroup)
    const partitionKeySession = QState.getOrAddPartitionKeySession(partitionGroupSession, partitionKey)
    const messageSession = {
      partitionKeySession
    }

    partitionGroupSession.messageCount += 1
    partitionGroupSession.queueSession.messageCount += 1
    partitionKeySession.messageCount += 1

    this.messageSessionsByMessageId.set(messageId, messageSession)

    return {queueName: partitionGroupSession.queueSession.queueName}
  }

  private getOrAddPartitionGroupSession(partitionGroup: string): PartitionGroupSession {
    let session = this.partitionGroupSessionsByPartitionGroup.get(partitionGroup)

    if (!session) {
      const queueState = this.getQueueState(partitionGroup)
      session = {
        partitionGroup,
        queueSession: queueState,
        partitionKeySessions: new Map<string, PartitionKeySession>(),
        messageCount: 0
      }
      this.partitionGroupSessionsByPartitionGroup.set(partitionGroup, session)
    }

    return session
  }

  private static getOrAddPartitionKeySession(
    partitionGroupSession: PartitionGroupSession,
    partitionKey: string
  ): PartitionKeySession {
    let partitionKeySession = partitionGroupSession.partitionKeySessions.get(partitionKey)

    if (!partitionKeySession) {
      partitionKeySession = {
        partitionKey,
        partitionGroupSession,
        messageCount: 0
      }
      partitionGroupSession.partitionKeySessions.set(partitionKey, partitionKeySession)
    }

    return partitionKeySession
  }

  private getQueueState(partitionGroup: string): QueueSession {
    // Just sum partitionGroup char codes instead of calculating a complex hash
    const queueIndex = sumCharCodes(partitionGroup) % this.queueSessions.length
    return this.queueSessions[queueIndex]
  }

  // private getOrAddQueueState(queueName: string) {
  //   let queueState = this.queueStatesByQueueName.get(queueName)
  //   if (!queueState) {
  //     throw new Error(`Unknown queue "${queueName}"`)
  //   }
  //   return queueState
  // }

  private async processMessage(messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) {
    const messageSession = this.messageSessionsByMessageId.get(messageId)!
    const partitionKeySession = messageSession.partitionKeySession
    const partitionGroupSession = partitionKeySession.partitionGroupSession
    const queueState = partitionGroupSession.queueSession

    this.messageSessionsByMessageId.delete(messageId)

    partitionKeySession.messageCount -= 1
    if (!partitionKeySession.messageCount) {
      partitionGroupSession.partitionKeySessions.delete(partitionKeySession.partitionKey)
    }

    partitionGroupSession.messageCount -= 1
    if (!partitionGroupSession.messageCount) {
      this.partitionGroupSessionsByPartitionGroup.delete(partitionGroupSession.partitionGroup)
    }

    queueState.messageCount -= 1

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
