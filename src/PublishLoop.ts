import ExecutionSerializer from '@targetprocess/balancer-core/bin/MessageBalancer.Serializer'
import MessageBalancer3, {MessageRef} from './balancing/MessageBalancer3'
import {emptyBuffer} from './constants'
import {QState} from './qState'
import {MessageProperties} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import {setImmediateAsync, waitFor} from './utils'

interface PublishLoopContext {
  readonly publisher: Publisher
  readonly qState: QState
  readonly messageBalancer: MessageBalancer3
  readonly mirrorQueueName: (queueName: string) => string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string
  readonly onError: (err: Error) => void
  readonly setState: (state: PublishLoopState) => Promise<void>
  processingMessageCount: number
  processedMessageCount: number
  isLoopInProgress: boolean
  isDestroyed: boolean
}

interface PublishLoopState {
  trigger: () => {alreadyStarted: boolean}
  readonly destroy: () => Promise<void>
  readonly waitForEnter?: () => Promise<void>
}

export default class PublishLoop {
  private ctx: PublishLoopContext | undefined
  private state: PublishLoopState = idleState()

  public stats() {
    const {
      processingMessageCount = 0,
      processedMessageCount = 0,
      isLoopInProgress = false,
      isDestroyed = false
    } = this.ctx || {}

    return {processingMessageCount, processedMessageCount, isLoopInProgress, isDestroyed}
  }

  public connectTo(params: {
    publisher: Publisher
    qState: QState
    messageBalancer: MessageBalancer3
    mirrorQueueName: (queueName: string) => string
    partitionGroupHeader: string
    partitionKeyHeader: string
    onError: (err: Error) => void
  }) {
    const {publisher, qState, messageBalancer, mirrorQueueName, partitionGroupHeader, partitionKeyHeader, onError} =
      params

    this.ctx = {
      publisher,
      qState,
      messageBalancer,
      mirrorQueueName,
      partitionGroupHeader,
      partitionKeyHeader,
      onError,
      setState: async state => {
        this.state = state
        if (this.state.waitForEnter) {
          await this.state.waitForEnter()
        }
      },
      processingMessageCount: 0,
      processedMessageCount: 0,
      isLoopInProgress: false,
      isDestroyed: false
    }

    this.state = connectedState(this.ctx)
  }

  public trigger(): {alreadyStarted: boolean} {
    return this.state.trigger()
  }

  public async destroy() {
    await this.state.destroy()
  }
}

function idleState(): PublishLoopState {
  return {
    trigger() {
      return {alreadyStarted: false}
    },
    async destroy() {
      // TODO: Move to destroyed state instead of throwing
      throwUnsupportedSignal(this.destroy.name, idleState.name)
    }
  }
}

function connectedState(ctx: PublishLoopContext): PublishLoopState {
  const executor = new ExecutionSerializer()

  return {
    trigger: () => {
      if (ctx.isLoopInProgress) {
        return {alreadyStarted: true}
      }

      ctx.isLoopInProgress = true
      startLoop(ctx, executor)

      return {alreadyStarted: false}
    },
    destroy: async () => {
      await ctx.setState(destroyedState(ctx))
    }
  }
}

function destroyedState(ctx: PublishLoopContext): PublishLoopState {
  const destroyed = (async () => {
    ctx.isDestroyed = true
    await waitFor(() => !ctx.processingMessageCount)
  })()

  return {
    trigger() {
      throwUnsupportedSignal(this.trigger.name, destroyedState.name)
    },
    async destroy() {
      throwUnsupportedSignal(this.destroy.name, destroyedState.name)
    },
    async waitForEnter() {
      await destroyed
    }
  }
}

async function startLoop(ctx: PublishLoopContext, executor: ExecutionSerializer) {
  try {
    for (let i = 1; ; i++) {
      if (ctx.isDestroyed) {
        // TODO: Log that the loop is stopped because of destroying
        return
      }

      const messageRef = ctx.messageBalancer.tryDequeueMessage(partitionGroup => ctx.qState.canRegister(partitionGroup))
      if (!messageRef) {
        // if (messageBalancer.size() && !qState.size()) {
        //   console.error(`[Critical] Unpublished messages left: ${messageBalancer.size()} messages`)
        //   process.exit(1)
        // }

        break
      }

      const {partitionGroup, partitionKey} = messageRef
      const {queueMessageId, queueName} = ctx.qState.registerMessage(partitionGroup, partitionKey)

      ctx.processingMessageCount += 1
      scheduleMessageProcessing(ctx, executor, messageRef, queueMessageId, queueName)

      if (i % 1 === 0) {
        // Let's sometimes give a chance to either
        // messages for other partitionGroup to be enqueued to messageBalancer
        // or empty slots for other partitionGroup to be appeared in QState
        await setImmediateAsync()
      }
    }
  } catch (err) {
    // TODO: Log error with a real logger
    console.error(err)

    ctx.onError(err)
  } finally {
    ctx.isLoopInProgress = false
  }
}

async function scheduleMessageProcessing(
  ctx: PublishLoopContext,
  executor: ExecutionSerializer,
  messageRef: MessageRef,
  queueMessageId: string,
  queueName: string
) {
  try {
    const {messageId, partitionGroup, partitionKey} = messageRef

    // TODO: Serialize message resolution by partitionGroup
    // TODO: Preconditions:
    // TODO:  * Improve ExecutionSerializer to cleanup serialization key registry to prevent memory leaks
    const [, message] = await executor.serializeResolution('getMessage', ctx.messageBalancer.getMessage(messageId))
    const {content, properties} = message

    if (ctx.isDestroyed) {
      return
    }

    await Promise.all([
      ctx.publisher.publishAsync('', ctx.mirrorQueueName(queueName), emptyBuffer, {
        headers: {
          [ctx.partitionGroupHeader]: partitionGroup,
          [ctx.partitionKeyHeader]: partitionKey
        },
        persistent: true,
        messageId: queueMessageId
      }),
      ctx.publisher.publishAsync('', queueName, content, {
        ...(properties as MessageProperties),
        persistent: true,
        messageId: queueMessageId
      })
    ])

    await ctx.messageBalancer.removeMessage(messageId)
  } catch (err) {
    // TODO: Log error with a logger
    console.error(err)

    ctx.onError(err)
  } finally {
    ctx.processingMessageCount -= 1
    ctx.processedMessageCount += 1
  }
}

function throwUnsupportedSignal(signal: string, state: string): never {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}
