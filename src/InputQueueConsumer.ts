import {Channel} from 'amqplib'
import MessageBalancer3 from './balancing/MessageBalancer3'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {waitFor} from './utils'

export default class InputQueueConsumer {
  private readonly ch: Channel
  private readonly messageBalancer: MessageBalancer3
  private readonly onError: (err: Error) => void
  private consumerTag: Promise<string> | undefined
  private processingMessageCount: number = 0
  private stopped: boolean = false

  constructor(params: {ch: Channel; messageBalancer: MessageBalancer3; onError: (err: Error) => void}) {
    this.ch = params.ch
    this.messageBalancer = params.messageBalancer
    this.onError = params.onError
  }

  async consume() {
    this.consumerTag = (async () => {
      const {consumerTag} = await this.ch.consume(
        inputQueueName,
        async msg => {
          if (this.stopped) {
            return
          }

          if (!msg) {
            this.stopped = true
            this.onError(new Error('Input queue consumer was canceled by broker'))
            return
          }

          this.processingMessageCount += 1

          try {
            const {content, properties} = msg
            const partitionGroup = msg.properties.headers[partitionGroupHeader]
            const partitionKey = msg.properties.headers[partitionKeyHeader]
            await this.messageBalancer.storeMessage({partitionGroup, partitionKey, content, properties})
            this.ch.ack(msg)
          } catch (err) {
            this.stopped = true
            this.onError(err)
          } finally {
            this.processingMessageCount -= 1
          }
        },
        {noAck: false}
      )

      return consumerTag
    })()

    await this.consumerTag
  }

  async destroy() {
    if (!this.consumerTag) {
      return
    }

    this.stopped = true
    await this.ch.cancel(await this.consumerTag)
    await waitFor(() => !this.processingMessageCount)
  }
}
