import {QState} from './qState'
import BalancedQueue from './balancedQueue'
import {nanoid} from 'nanoid'
import {ConfirmChannel} from 'amqplib'
import {publishAsync} from './publishAsync'
import {outputMirrorQueueName} from './config'

export default class PublishLoop {
  private state = emptyState()
  private inProgress = false

  public connectTo(ch: ConfirmChannel, qState: QState, balancedQueue: BalancedQueue<Buffer>) {
    this.state = connectedState(ch, qState, balancedQueue)
  }

  public trigger() {
    if (this.inProgress) {
      return
    }

    this.inProgress = true
    this.state.startLoop(() => {
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
    startLoop: (onCompleted: () => void) => {
      onCompleted()
      return Promise.resolve()
    }
  }
}

function connectedState(ch: ConfirmChannel, qState: QState, balancedQueue: BalancedQueue<Buffer>) {
  return {
    startLoop: async (onCompleted: () => void) => {
      // TODO: Think about error handling

      while (true) {
        const dequeueResult = balancedQueue.tryDequeue(pk => qState.canPublish(pk))
        if (!dequeueResult) {
          onCompleted()
          return
        }

        const {value, partitionKey} = dequeueResult
        const messageId = nanoid()
        const {queueName} = qState.registerMessage(messageId, partitionKey)

        publishAsync(ch, '', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId})
        publishAsync(ch, '', queueName, value, {persistent: true, messageId})

        // await Promise.all([
        //   publishAsync(ch, '', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId}),
        //   publishAsync(ch, '', queueName, value, {persistent: true, messageId})
        // ])

        // await publishAsync(ch, '', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId})
        // await publishAsync(ch, '', queueName, value, {persistent: true, messageId})

        // TODO: Looks like publishing could be started concurrently for performance reason
        // await ch.tx(tx => {
        //   tx.publish('', queueName, value, {persistent: true, messageId})
        //   tx.publish('', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId})
        // })
      }
    }
  }
}
