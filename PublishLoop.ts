import MessageBalancer, {MessageRef} from './balancer-core-3/MessageBalancer'
import {QState} from './qState'
import {ConfirmChannel, MessageProperties} from 'amqplib'
import {publishAsync} from './publishAsync'
import {outputMirrorQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import ExecutionSerializer from './balancer-core/MessageBalancer.Serializer'
import {nanoid} from 'nanoid'

export default class PublishLoop {
  private state = emptyState()
  private inProgress = false

  public connectTo(ch: ConfirmChannel, qState: QState, messageBalancer: MessageBalancer) {
    this.state = connectedState(ch, qState, messageBalancer)
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

function connectedState(ch: ConfirmChannel, qState: QState, messageBalancer: MessageBalancer) {
  const executor = new ExecutionSerializer()

  return {
    startLoop: async (onCompleted: () => void) => {
      // TODO: Think about error handling
      while (true) {
        const messageRef = messageBalancer.tryDequeueMessage(partitionGroup => qState.canRegister(partitionGroup))

        if (!messageRef) {
          onCompleted()
          return
        }

        const {partitionGroup, partitionKey} = messageRef
        const messageId = nanoid()
        const {queueName} = qState.registerMessage(messageId, partitionGroup, partitionKey)

        scheduleMessageProcessing(messageRef, messageId, queueName)
      }
    }
  }

  async function scheduleMessageProcessing(messageRef: MessageRef, messageId: string, queueName: string) {
    // TODO: Think about error handling
    // TODO: Serialize message resolution by partitionGroup
    const [, message] = await executor.serializeResolution(
      'getMessage',
      messageBalancer.getMessage(messageRef.messageId)
    )
    const {content, properties} = message
    const {partitionGroup, partitionKey} = messageRef

    await Promise.all([
      publishAsync(ch, '', outputMirrorQueueName(queueName), Buffer.from(''), {
        headers: {
          [partitionGroupHeader]: partitionGroup,
          [partitionKeyHeader]: partitionKey
        },
        persistent: true,
        messageId
      }),
      publishAsync(ch, '', queueName, content, {
        ...(properties as MessageProperties),
        persistent: true,
        messageId
      })
    ])

    await messageBalancer.removeMessage(messageRef.messageId)
  }
}
