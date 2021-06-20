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
  canProcessPartitionGroup: () => boolean
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

  private version = 0

  constructor(params: QStateParams) {
    this.onMessageProcessed = params.onMessageProcessed
    this.queueSizeLimit = params.queueSizeLimit
    this.singlePartitionGroupLimit = params.singlePartitionGroupLimit
    this.singlePartitionKeyLimit = params.singlePartitionKeyLimit

    const queueNames = Array.from({length: params.queueCount}, (_, i) => outputQueueName(i + 1))
    for (const queueName of queueNames) {
      const queueSession = {queueName, messageCount: 0}
      this.queueSessions.push(queueSession)
      this.queueSessionsByQueueName.set(queueName, queueSession)
    }
  }

  public canRegister(partitionGroup: string): PartitionGroupGuard {
    const capturedVersion = this.version
    const partitionGroupSession = this.partitionGroupSessionsByPartitionGroup.get(partitionGroup)

    if (!partitionGroupSession) {
      const canRegisterNewPartitionGroup = this.getQueueSession(partitionGroup).messageCount < this.queueSizeLimit
      const canProcess = this.canProcess(capturedVersion, canRegisterNewPartitionGroup)
      return {
        canProcessPartitionGroup: canProcess,
        canProcessPartitionKey: canProcess
      }
    }

    const canRegisterExistingPartitionGroup =
      partitionGroupSession.queueSession.messageCount < this.queueSizeLimit &&
      partitionGroupSession.messageCount < this.singlePartitionGroupLimit

    if (canRegisterExistingPartitionGroup) {
      return {
        canProcessPartitionGroup: this.canProcess(capturedVersion, true),
        canProcessPartitionKey: (partitionKey: string) => {
          this.assertVersion(capturedVersion)
          const partitionKeySession = partitionGroupSession!.partitionKeySessions.get(partitionKey)
          if (!partitionKeySession) {
            return true
          }
          return partitionKeySession.messageCount < this.singlePartitionKeyLimit
        }
      }
    }

    const canProcess = this.canProcess(capturedVersion, false)
    return {
      canProcessPartitionGroup: canProcess,
      canProcessPartitionKey: canProcess
    }
  }

  private canProcess(capturedVersion: number, value: boolean) {
    return () => {
      this.assertVersion(capturedVersion)
      return value
    }
  }

  private assertVersion(capturedVersion: number) {
    if (capturedVersion !== this.version) {
      throw new Error('Incompatible QState version')
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
    const expectedQueueName = this.getQueueSession(partitionGroup).queueName

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
    const partitionKeySession = this.getOrAddPartitionKeySession(partitionGroup, partitionKey)
    const messageSession = {
      partitionKeySession
    }

    this.messageSessionsByMessageId.set(messageId, messageSession)
    partitionKeySession.messageCount += 1
    partitionKeySession.partitionGroupSession.messageCount += 1
    partitionKeySession.partitionGroupSession.queueSession.messageCount += 1

    this.version += 1

    return {queueName: partitionKeySession.partitionGroupSession.queueSession.queueName}
  }

  private getOrAddPartitionKeySession(partitionGroup: string, partitionKey: string): PartitionKeySession {
    const partitionGroupSession = this.getOrAddPartitionGroupSession(partitionGroup)
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

  private getOrAddPartitionGroupSession(partitionGroup: string): PartitionGroupSession {
    let session = this.partitionGroupSessionsByPartitionGroup.get(partitionGroup)

    if (!session) {
      const queueSession = this.getQueueSession(partitionGroup)
      session = {
        partitionGroup,
        queueSession,
        partitionKeySessions: new Map<string, PartitionKeySession>(),
        messageCount: 0
      }
      this.partitionGroupSessionsByPartitionGroup.set(partitionGroup, session)
    }

    return session
  }

  private getQueueSession(partitionGroup: string): QueueSession {
    // Just sum partitionGroup char codes instead of calculating a complex hash
    const queueIndex = sumCharCodes(partitionGroup) % this.queueSessions.length
    return this.queueSessions[queueIndex]
  }

  private async processMessage(messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) {
    const messageSession = this.messageSessionsByMessageId.get(messageId)!
    const partitionKeySession = messageSession.partitionKeySession
    const partitionGroupSession = partitionKeySession.partitionGroupSession
    const queueSession = partitionGroupSession.queueSession

    this.messageSessionsByMessageId.delete(messageId)

    partitionKeySession.messageCount -= 1
    if (!partitionKeySession.messageCount) {
      partitionGroupSession.partitionKeySessions.delete(partitionKeySession.partitionKey)
    }

    partitionGroupSession.messageCount -= 1
    if (!partitionGroupSession.messageCount) {
      this.partitionGroupSessionsByPartitionGroup.delete(partitionGroupSession.partitionGroup)
    }

    queueSession.messageCount -= 1

    this.version += 1

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
