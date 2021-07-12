import ExecutionSerializer from '@targetprocess/balancer-core/bin/MessageBalancer.Serializer'
import {MessageProperties} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import MessageBalancer3, {MessageRef} from './balancing/MessageBalancer3'
import {emptyBuffer} from './constants'
import {QState} from './QState'
import {setImmediateAsync, waitFor} from './utils'
import {callSafe, compareExchangeState, deferred, throwUnsupportedSignal} from './stateMachine'

interface PublishLoopContext extends PublishLoopArgs {
  executor: ExecutionSerializer
  statistics: PublishLoopStatistics
  cancellationToken: CancellationToken

  compareExchangeState: CompareExchangeState
  processError: ProcessError
}

interface PublishLoopArgs {
  mirrorQueueName: (queueName: string) => string
  partitionGroupHeader: string
  partitionKeyHeader: string
  onError: (err: Error) => void
}

interface PublishLoopStatistics {
  processingMessageCount: number
  processedMessageCount: number
  failedMessageCount: number
}

interface CancellationToken {
  isCanceled: boolean
}

type CompareExchangeState = (toState: PublishLoopState, fromState: PublishLoopState) => boolean

type ProcessError = (err: Error) => void

interface PublishLoopState {
  name: string
  onEnter?: () => void

  connectTo: (ars: ConnectToArgs) => void
  trigger: () => {started: boolean}
  destroy: () => Promise<void>

  processError: (err: Error) => void
}

interface PublishLoopStatus {
  state: string
  processingMessageCount: number
  processedMessageCount: number
  failedMessageCount: number
}

interface ConnectToArgs {
  publisher: Publisher
  qState: QState
  messageBalancer: MessageBalancer3
}

export default class PublishLoop {
  private readonly ctx: PublishLoopContext
  private readonly state: {value: PublishLoopState}

  constructor(args: PublishLoopArgs) {
    this.ctx = {
      ...args,

      onError: err => {
        callSafe(() => args.onError(err))
      },

      executor: new ExecutionSerializer(),
      statistics: {processingMessageCount: 0, processedMessageCount: 0, failedMessageCount: 0},
      cancellationToken: {isCanceled: false},

      compareExchangeState: (toState, fromState) => {
        return compareExchangeState(this.state, toState, fromState)
      },
      processError: (err: Error) => {
        this.state.value.processError(err)
      }
    }

    this.state = {value: new InitialState(this.ctx)}

    if (this.state.value.onEnter) {
      this.state.value.onEnter()
    }
  }

  public status(): PublishLoopStatus {
    const {processingMessageCount, processedMessageCount, failedMessageCount} = this.ctx.statistics

    return {
      state: this.state.value.name,
      processingMessageCount,
      processedMessageCount,
      failedMessageCount
    }
  }

  public connectTo(args: ConnectToArgs) {
    this.state.value.connectTo(args)
  }

  public trigger() {
    return this.state.value.trigger()
  }

  public async destroy() {
    await this.state.value.destroy()
  }
}

class InitialState implements PublishLoopState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: PublishLoopContext) {}

  public connectTo(args: ConnectToArgs) {
    this.ctx.compareExchangeState(new ReadyState(this.ctx, args), this)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  public processError(_: Error) {
    throwUnsupportedSignal(this.processError.name, this.name)
  }
}

class ReadyState implements PublishLoopState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: PublishLoopContext, private readonly args: ConnectToArgs) {}

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, this.name)
  }

  public trigger() {
    this.ctx.compareExchangeState(new LoopInProgressState(this.ctx, this.args), this)
    return {started: true}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {err}), this)
  }
}

class LoopInProgressState implements PublishLoopState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: PublishLoopContext, private readonly args: ConnectToArgs) {}

  public async onEnter() {
    try {
      for (let i = 1; !this.ctx.cancellationToken.isCanceled; i++) {
        const messageRef = this.args.messageBalancer.tryDequeueMessage(partitionGroup =>
          this.args.qState.canRegister(partitionGroup)
        )

        if (!messageRef) {
          this.ctx.compareExchangeState(new ReadyState(this.ctx, this.args), this)
          break
        }

        const {partitionGroup, partitionKey} = messageRef
        const {queueMessageId, queueName} = this.args.qState.registerMessage(partitionGroup, partitionKey)

        this.scheduleMessageProcessing(messageRef, queueMessageId, queueName)

        if (i % 1 === 0) {
          // Let's sometimes give a chance to either
          // messages for other partitionGroup to be enqueued to messageBalancer
          // or empty slots for other partitionGroup to be appeared in QState
          await setImmediateAsync()
        }
      }
    } catch (err) {
      this.ctx.processError(err)
    }
  }

  private async scheduleMessageProcessing(messageRef: MessageRef, queueMessageId: string, queueName: string) {
    this.ctx.statistics.processingMessageCount += 1

    try {
      const {messageId, partitionGroup, partitionKey} = messageRef

      // TODO: Serialize message resolution by partitionGroup
      // TODO: Preconditions:
      // TODO:  * Improve ExecutionSerializer to cleanup serialization key registry to prevent memory leaks
      const [, message] = await this.ctx.executor.serializeResolution(
        'getMessage',
        this.args.messageBalancer.getMessage(messageId)
      )
      const {content, properties} = message

      if (this.ctx.cancellationToken.isCanceled) {
        return
      }

      await Promise.all([
        this.args.publisher.publishAsync('', this.ctx.mirrorQueueName(queueName), emptyBuffer, {
          headers: {
            [this.ctx.partitionGroupHeader]: partitionGroup,
            [this.ctx.partitionKeyHeader]: partitionKey
          },
          persistent: true,
          messageId: queueMessageId
        }),
        this.args.publisher.publishAsync('', queueName, content, {
          ...(properties as MessageProperties),
          persistent: true,
          messageId: queueMessageId
        })
      ])

      await this.args.messageBalancer.removeMessage(messageId)

      this.ctx.statistics.processingMessageCount -= 1
      this.ctx.statistics.processedMessageCount += 1
    } catch (err) {
      this.ctx.statistics.processingMessageCount -= 1
      this.ctx.statistics.failedMessageCount += 1

      this.ctx.processError(err)
    }
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, this.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {err}), this)
  }
}

class ErrorState implements PublishLoopState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: PublishLoopContext, private readonly args: {err: Error}) {}

  public onEnter() {
    console.error(this.args.err)

    this.ctx.cancellationToken.isCanceled = true
    this.ctx.onError(this.args.err)
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, this.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  public processError(err: Error) {
    console.error(err)
    this.ctx.onError(err)
  }
}

class DestroyedState implements PublishLoopState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: PublishLoopContext, private readonly args: {onDestroyed: (err?: Error) => void}) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    await waitFor(() => !this.ctx.statistics.processingMessageCount)
    this.args.onDestroyed()
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, this.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await waitFor(() => !this.ctx.statistics.processingMessageCount)
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}
