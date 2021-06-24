import ExecutionSerializer from '@targetprocess/balancer-core/bin/MessageBalancer.Serializer'
import MessageBalancer3, {MessageRef} from './balancing/MessageBalancer3'
import {emptyBuffer} from './constants'
import {QState} from './qState'
import {MessageProperties} from 'amqplib'
import {Publisher} from './amqp/Publisher'

export default class PublishLoop {
  private state = idleState()
  private inProgress = false

  public stats() {
    return this.state.stats()
  }

  public connectTo(params: {
    publisher: Publisher
    qState: QState
    messageBalancer: MessageBalancer3
    mirrorQueueName: (queueName: string) => string
    partitionGroupHeader: string
    partitionKeyHeader: string
  }) {
    this.state = connectedState(params)
  }

  public start(): {alreadyStarted: boolean} {
    if (this.inProgress) {
      return {alreadyStarted: true}
    }

    this.inProgress = true
    this.state.startLoop(err => {
      if (err) {
        console.error(err)
        process.exit(1)
      }

      //
      // "inProgress" flag should be reset synchronously as soon as the current loop completed.
      // "await this.state.startLoop" continuation code won't run immediately,
      // it will be scheduled to run asynchronously instead,
      // as a result some calls to "startLoop" method could be missed, because "inProgress" flag won't be reset yet.
      //
      // Try to run the following code snippet to understand the issue:
      //
      //   async function main() {
      //     await startLoop();
      //     console.log("continuation started");
      //   }
      //
      //   async function startLoop() {
      //     await Promise.resolve();
      //     Promise.resolve().then(() => console.log("another startLoop() call"));
      //     console.log('startLoop() completed')
      //   }
      //
      //   main();
      //
      // Expected console output:
      //
      //   > startLoop() completed
      //   > another startLoop() call
      //   > continuation started
      //
      this.inProgress = false
    })

    return {alreadyStarted: false}
  }
}

function idleState() {
  return {
    stats: () => {
      return {processingMessageCount: 0, processedMessageCount: 0}
    },
    startLoop: (onCompleted: (err?: Error) => void) => {
      onCompleted()
      return Promise.resolve()
    }
  }
}

function connectedState(params: {
  publisher: Publisher
  qState: QState
  messageBalancer: MessageBalancer3
  mirrorQueueName: (queueName: string) => string
  partitionGroupHeader: string
  partitionKeyHeader: string
}) {
  const {publisher, qState, messageBalancer, mirrorQueueName, partitionGroupHeader, partitionKeyHeader} = params
  const executor = new ExecutionSerializer()
  let processingMessageCount = 0
  let processedMessageCount = 0

  return {
    stats: () => {
      return {processingMessageCount, processedMessageCount}
    },
    startLoop: async (onCompleted: (err?: Error) => void) => {
      try {
        for (let i = 1; ; i++) {
          const messageRef = messageBalancer.tryDequeueMessage(partitionGroup => qState.canRegister(partitionGroup))
          if (!messageRef) {
            // if (messageBalancer.size() && !qState.size()) {
            //   console.error(`[Critical] Unpublished messages left: ${messageBalancer.size()} messages`)
            //   process.exit(1)
            // }

            break
          }

          const {partitionGroup, partitionKey} = messageRef
          const {queueMessageId, queueName} = qState.registerMessage(partitionGroup, partitionKey)

          processingMessageCount += 1
          scheduleMessageProcessing(messageRef, queueMessageId, queueName)

          if (i % 1 === 0) {
            // Let's sometimes give a chance to either
            // messages for other partitionGroup to be enqueued to messageBalancer
            // or empty slots for other partitionGroup to be appeared in QState
            await setImmediateAsync()
          }
        }
      } catch (err) {
        onCompleted(err)
        return
      }

      onCompleted()
    }
  }

  async function scheduleMessageProcessing(messageRef: MessageRef, queueMessageId: string, queueName: string) {
    try {
      const {messageId, partitionGroup, partitionKey} = messageRef

      // TODO: Serialize message resolution by partitionGroup
      // TODO: Preconditions:
      // TODO:  * Improve ExecutionSerializer to cleanup serialization key registry to prevent memory leaks
      const [, message] = await executor.serializeResolution('getMessage', messageBalancer.getMessage(messageId))
      const {content, properties} = message

      await Promise.all([
        publisher.publishAsync('', mirrorQueueName(queueName), emptyBuffer, {
          headers: {
            [partitionGroupHeader]: partitionGroup,
            [partitionKeyHeader]: partitionKey
          },
          persistent: true,
          messageId: queueMessageId
        }),
        publisher.publishAsync('', queueName, content, {
          ...(properties as MessageProperties),
          persistent: true,
          messageId: queueMessageId
        })
      ])

      await messageBalancer.removeMessage(messageId)
    } catch (err) {
      console.error(err)
      process.exit(1)
    } finally {
      processingMessageCount -= 1
      processedMessageCount += 1
    }
  }
}

async function setImmediateAsync() {
  return new Promise(res => {
    setImmediate(res)
  })
}
