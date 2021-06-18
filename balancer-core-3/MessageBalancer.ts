import {MessageProperties} from 'amqplib'
import {Message, MessageCache, MessageStorage} from '../balancer-core'
import ExecutionSerializer from '../balancer-core/MessageBalancer.Serializer'
import PartitionGroupQueue from './PartitionGroupQueue'

interface MessageData {
  partitionGroup: string
  partitionKey: string
  content: Buffer
  properties: MessageProperties
}

export default class MessageBalancer {
  private readonly storage: MessageStorage
  private readonly cache: MessageCache
  private readonly onPartitionAdded: (partitionGroup: string, partitionKey: string) => void

  private readonly partitionGroupQueue = new PartitionGroupQueue()
  private readonly executor = new ExecutionSerializer()

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
    partitionGroupPredicate: (partitionGroup: string) => boolean,
    partitionKeyPredicate: (partitionKey: string) => boolean
  ): ((process: (message: Message) => Promise<void>) => Promise<void>) | undefined {
    const dequeued = this.partitionGroupQueue.tryDequeue(partitionGroupPredicate, partitionKeyPredicate)
    if (!dequeued) {
      return undefined
    }

    const {messageId} = dequeued

    return async (process: (message: Message) => Promise<void>) => {
      // TODO: Make sure that called
      // TODO: Make sure that called just once

      try {
        // TODO: create serialization key based on partitionGroup?
        const [, message] = await this.executor.serializeResolution('getMessageById', this.getMessage(messageId))
        await process(message)
        await this.storage.removeMessage(messageId)
      } catch (err) {
        console.error(err)
      }
    }
  }

  private async getMessage(messageId: number): Promise<Message> {
    return this.cache.getAndRemoveMessage(messageId) || (await this.storage.readMessage(messageId))!
  }
}
