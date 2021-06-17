import {MessageProperties} from 'amqplib'
import Centrifuge from './Centrifuge'

interface MessageData {
  partitionKey: string
  content: Buffer
  properties: MessageProperties
}

interface MessageRef {
  messageId: number
  partitionKey: string
  partitionGroup: string
}

export default class MessageBalancer {
  public readonly partitionGroup: string
  private readonly centrifuge = new Centrifuge<number>()

  constructor(params: {partitionGroup: string}) {
    this.partitionGroup = params.partitionGroup
  }

  public async storeMessage(message: MessageData) {
    // TODO: store message
    const messageId = -1

    this.centrifuge.enqueue(messageId, message.partitionKey)
  }

  public tryProcessMessage(
    predicate: (partitionKey: string) => boolean,
    process: (message: MessageRef) => Promise<void>
  ) {
    const keyValue = this.centrifuge.tryDequeue(predicate)
    if (!keyValue) {
      return false
    }

    const {value, partitionKey} = keyValue

    ;(async () => {
      try {
        await process({messageId: value, partitionKey, partitionGroup: this.partitionGroup})
      } catch (err) {
        console.error(err)
      }
    })()

    return true
  }
}
