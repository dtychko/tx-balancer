import {nanoid} from 'nanoid'

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
  messageId: string
  mirrorDeliveryTag?: number
  responseDeliveryTag?: number
  partitionKeySession: PartitionKeySession
}

export interface PartitionGroupGuard {
  canProcessPartitionGroup: () => boolean
  canProcessPartitionKey: (partitionKey: string) => boolean
}

interface QStateParams {
  onMessageProcessed: (messageId: string, mirrorDeliveryTag: number, responseDeliveryTag: number) => void
  partitionGroupHash: (partitionGroup: string) => number
  outputQueueName: (oneBasedIndex: number) => string
  queueCount: number
  queueSizeLimit: number
  singlePartitionGroupLimit: number
  singlePartitionKeyLimit: number
}

export class QState {
  private readonly onMessageProcessed: QStateParams['onMessageProcessed']
  private readonly partitionGroupHash: QStateParams['partitionGroupHash']
  private readonly queueSizeLimit: number
  private readonly singlePartitionGroupLimit: number
  private readonly singlePartitionKeyLimit: number

  private readonly messageSessionsByMessageId = new Map<string, MessageSession>()
  private readonly partitionGroupSessionsByPartitionGroup = new Map<string, PartitionGroupSession>()
  private readonly queueSessionsByQueueName = new Map<string, QueueSession>()
  private readonly queueSessions = [] as QueueSession[]

  private version = 0

  constructor(params: QStateParams) {
    this.onMessageProcessed = params.onMessageProcessed
    this.partitionGroupHash = params.partitionGroupHash
    this.queueSizeLimit = params.queueSizeLimit
    this.singlePartitionGroupLimit = params.singlePartitionGroupLimit
    this.singlePartitionKeyLimit = params.singlePartitionKeyLimit

    for (let queueIndex = 1; queueIndex <= params.queueCount; queueIndex++) {
      const queueName = params.outputQueueName(queueIndex)
      const queueSession = {queueName, messageCount: 0}
      this.queueSessions.push(queueSession)
      this.queueSessionsByQueueName.set(queueName, queueSession)
    }
  }

  public size() {
    return this.messageSessionsByMessageId.size
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

  public registerMirrorDeliveryTag(messageId: string, deliveryTag: number): {registered: boolean} {
    const messageSession = this.messageSessionsByMessageId.get(messageId)
    if (!messageSession) {
      return {registered: false}
    }

    messageSession.mirrorDeliveryTag = deliveryTag
    if (messageSession.responseDeliveryTag !== undefined) {
      this.processMessage(messageSession)
    }

    return {registered: true}
  }

  public registerResponseDeliveryTag(messageId: string, deliveryTag: number): {registered: boolean} {
    const messageSession = this.messageSessionsByMessageId.get(messageId)
    if (!messageSession) {
      return {registered: false}
    }

    messageSession.responseDeliveryTag = deliveryTag
    if (messageSession.mirrorDeliveryTag !== undefined) {
      this.processMessage(messageSession)
    }

    return {registered: true}
  }

  public restoreMessage(messageId: string, partitionGroup: string, partitionKey: string, queueName: string) {
    if (this.messageSessionsByMessageId.has(messageId)) {
      throw new Error(
        `Can't restore message#${messageId} because another message with the same id is already registered`
      )
    }

    const expectedQueueName = this.getQueueSession(partitionGroup).queueName
    if (queueName !== expectedQueueName) {
      throw new Error(
        `Can't bind partition group "${partitionGroup}" to queue "${queueName}" ` +
          `because it's already bound to queue "${expectedQueueName}"`
      )
    }

    this.registerMessageSession(messageId, partitionGroup, partitionKey)
  }

  public registerMessage(partitionGroup: string, partitionKey: string): {queueMessageId: string; queueName: string} {
    const messageId = nanoid()
    return this.registerMessageSession(messageId, partitionGroup, partitionKey)
  }

  private registerMessageSession(
    messageId: string,
    partitionGroup: string,
    partitionKey: string
  ): {queueMessageId: string; queueName: string} {
    const partitionKeySession = this.getOrAddPartitionKeySession(partitionGroup, partitionKey)
    const messageSession = {
      messageId,
      partitionKeySession
    }

    this.messageSessionsByMessageId.set(messageId, messageSession)
    partitionKeySession.messageCount += 1
    partitionKeySession.partitionGroupSession.messageCount += 1
    partitionKeySession.partitionGroupSession.queueSession.messageCount += 1

    this.version += 1

    return {queueMessageId: messageId, queueName: partitionKeySession.partitionGroupSession.queueSession.queueName}
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
    const queueIndex = this.partitionGroupHash(partitionGroup) % this.queueSessions.length
    return this.queueSessions[queueIndex]
  }

  private async processMessage(messageSession: MessageSession) {
    const {messageId, mirrorDeliveryTag, responseDeliveryTag, partitionKeySession} = messageSession
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

    if (mirrorDeliveryTag === undefined || responseDeliveryTag === undefined) {
      console.error('Invalid message session state')
      return
    }

    this.onMessageProcessed(messageId, mirrorDeliveryTag, responseDeliveryTag)
  }
}
