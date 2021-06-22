import MessageBalancer, {MessageRef} from './balancer-core-3/MessageBalancer'
import {emptyBuffer} from './constants'
import {QState} from './qState'
import {ConfirmChannel, MessageProperties} from 'amqplib'
import {publishAsync} from './publishAsync'
import {mirrorQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import ExecutionSerializer from './balancer-core/MessageBalancer.Serializer'

export default class PublishLoop {
  private state = emptyState()
  private inProgress = false

  public connectTo(params: {ch: ConfirmChannel; qState: QState; messageBalancer: MessageBalancer}) {
    this.state = connectedState(params)
  }

  public trigger() {
    if (this.inProgress) {
      return
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
  }
}

function emptyState() {
  return {
    startLoop: (onCompleted: (err?: Error) => void) => {
      onCompleted()
      return Promise.resolve()
    }
  }
}

export let scheduledProcessingCount = 0

function connectedState(params: {ch: ConfirmChannel; qState: QState; messageBalancer: MessageBalancer}) {
  const {ch, qState, messageBalancer} = params
  const executor = new ExecutionSerializer()

  return {
    startLoop: async (onCompleted: (err?: Error) => void) => {
      try {
        for (let i = 1; ; i++) {
          const messageRef = messageBalancer.tryDequeueMessage(partitionGroup => qState.canRegister(partitionGroup))
          if (!messageRef) {
            if (messageBalancer.size() && !qState.size()) {
              console.log('WARN')
              // const messageRef2 = messageBalancer.tryDequeueMessage(partitionGroup => qState.canRegister(partitionGroup))
            }

            break
          }

          const {partitionGroup, partitionKey} = messageRef
          const {queueMessageId, queueName} = qState.registerMessage(partitionGroup, partitionKey)

          scheduleMessageProcessing(messageRef, queueMessageId, queueName)

          if (i % 5 === 0) {
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
    scheduledProcessingCount += 1

    try {
      const {messageId, partitionGroup, partitionKey} = messageRef

      // TODO: Serialize message resolution by partitionGroup
      // TODO: Preconditions:
      // TODO:  * Improve ExecutionSerializer to cleanup serialization key registry to prevent memory leaks
      const [, message] = await executor.serializeResolution('getMessage', messageBalancer.getMessage(messageId))
      const {content, properties} = message

      await Promise.all([
        publishAsync(ch, '', mirrorQueueName(queueName), emptyBuffer, {
          headers: {
            [partitionGroupHeader]: partitionGroup,
            [partitionKeyHeader]: partitionKey
          },
          persistent: true,
          messageId: queueMessageId
        }),
        publishAsync(ch, '', queueName, content, {
          ...(properties as MessageProperties),
          persistent: true,
          messageId: queueMessageId
        })
      ])

      await messageBalancer.removeMessage(messageId)
    } catch (err) {
      console.error(err)
      process.exit(1)
    }
  }
}

async function setImmediateAsync() {
  return new Promise(res => {
    setImmediate(res)
  })
}
