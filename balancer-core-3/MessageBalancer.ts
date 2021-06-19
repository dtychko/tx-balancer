import {MessageProperties} from 'amqplib'
import {Message, MessageCache, MessageStorage} from '../balancer-core'
import PartitionGroupQueue from './PartitionGroupQueue'
import {PartitionGroupGuard} from '../QState'

interface MessageData {
  partitionGroup: string
  partitionKey: string
  content: Buffer
  properties: MessageProperties
}

export interface MessageRef {
  messageId: number
  partitionGroup: string
  partitionKey: string
}

export default class MessageBalancer {
  private readonly storage: MessageStorage
  private readonly cache: MessageCache
  private readonly onPartitionAdded: (partitionGroup: string, partitionKey: string) => void

  private readonly partitionGroupQueue = new PartitionGroupQueue()

  constructor(params: {
    storage: MessageStorage
    cache: MessageCache
    onPartitionAdded: (partitionGroup: string, partitionKey: string) => void
  }) {
    this.storage = params.storage
    this.cache = params.cache
    this.onPartitionAdded = params.onPartitionAdded
  }

  public async storeMessage(messageData: MessageData) {
    const message = await this.storage.createMessage({
      receivedDate: new Date(),
      ...messageData
    })
    const {messageId, partitionGroup, partitionKey} = message

    const {partitionKeyAdded} = this.partitionGroupQueue.enqueue(messageId, partitionGroup, partitionKey)
    this.cache.addMessage(message)

    if (partitionKeyAdded) {
      this.onPartitionAdded(partitionGroup, partitionKey)
    }
  }

  public tryDequeueMessage(
    canProcess: (partitionGroup: string) => PartitionGroupGuard
  ): MessageRef | undefined {
    return this.partitionGroupQueue.tryDequeue(canProcess)
  }

  public async getMessage(messageId: number): Promise<Message> {
    return this.cache.getAndRemoveMessage(messageId) || (await this.storage.readMessage(messageId))!
  }

  public async removeMessage(messageId: number) {
    await this.storage.removeMessage(messageId)
  }
}
