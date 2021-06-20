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
  private isInitialized = false

  constructor(params: {
    storage: MessageStorage
    cache: MessageCache
    onPartitionAdded: (partitionGroup: string, partitionKey: string) => void
  }) {
    this.storage = params.storage
    this.cache = params.cache
    this.onPartitionAdded = params.onPartitionAdded
  }

  public async init() {
    const pageSize = 1000

    for (let zeroBasedPage = 0; ; zeroBasedPage++) {
      // TODO: Add contentSizeLimit
      // TODO: Don't fetch content at all if cache is already full
      const result = await this.storage.readPartitionGroupMessagesOrderedById({zeroBasedPage, pageSize})

      if (!result.size) {
        break
      }

      for (const [partitionGroup, messages] of result) {
        for (const message of messages) {
          const {messageId, partitionKey} = message
          this.partitionGroupQueue.enqueue(messageId, partitionGroup, partitionKey)
          this.cache.addMessage(message)
        }
      }
    }

    this.isInitialized = true
  }

  public async storeMessage(messageData: MessageData) {
    this.assertInitialized()

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

  public tryDequeueMessage(canProcess: (partitionGroup: string) => PartitionGroupGuard): MessageRef | undefined {
    this.assertInitialized()
    return this.partitionGroupQueue.tryDequeue(canProcess)
  }

  public async getMessage(messageId: number): Promise<Message> {
    this.assertInitialized()
    return this.cache.getAndRemoveMessage(messageId) || (await this.storage.readMessage(messageId))!
  }

  public async removeMessage(messageId: number) {
    this.assertInitialized()
    await this.storage.removeMessage(messageId)
  }

  private assertInitialized() {
    if (!this.isInitialized) {
      throw new Error(`Message service isn't initialized`)
    }
  }
}
