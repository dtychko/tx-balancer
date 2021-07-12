import {Channel, Message} from 'amqplib'
import {waitFor} from './utils'
import {callSafe, compareExchangeState, deferred, throwUnsupportedSignal} from './stateMachine'

interface ConsumerContext extends ConsumerArgs {
  statistics: ConsumerStatistics
  cancellationToken: CancellationToken

  compareExchangeState: (toState: ConsumerState, fromState: ConsumerState) => boolean
  processError: (err: Error) => void
}

interface ConsumerArgs {
  ch: Channel
  queueName: string
  processMessage: (msg: Message) => Promise<ProcessMessageResult | void>
  onError: (err: Error) => void
}

type ProcessMessageResult = {ack: true} | {ack: false; requeue: boolean}

interface ConsumerStatistics {
  processingMessageCount: number
  processedMessageCount: number
  failedMessageCount: number
}

interface CancellationToken {
  isCanceled: boolean
}

interface ConsumerState {
  name: string
  onEnter?: () => void

  init: () => Promise<void>
  destroy: () => Promise<void>
  processError: (err: Error) => void
}

interface ConsumerStatus {
  state: string
  processingMessageCount: number
  processedMessageCount: number
  failedMessageCount: number
}

export default class QueueConsumer {
  private readonly ctx: ConsumerContext
  private readonly state: {value: ConsumerState}

  constructor(args: ConsumerArgs) {
    this.ctx = {
      ...args,

      onError: err => {
        callSafe(() => args.onError(err))
      },

      statistics: {processingMessageCount: 0, processedMessageCount: 0, failedMessageCount: 0},
      cancellationToken: {isCanceled: false},

      compareExchangeState: (toState, fromState) => {
        return compareExchangeState(this.state, toState, fromState)
      },
      processError: err => {
        this.state.value.processError(err)
      }
    }

    this.state = {value: new InitialState(this.ctx)}

    if (this.state.value.onEnter) {
      this.state.value.onEnter()
    }
  }

  public status(): ConsumerStatus {
    const {processingMessageCount, processedMessageCount, failedMessageCount} = this.ctx.statistics

    return {
      state: this.state.value.name,
      processingMessageCount,
      processedMessageCount,
      failedMessageCount
    }
  }

  public init() {
    return this.state.value.init()
  }

  public destroy() {
    return this.state.value.destroy()
  }
}

class InitialState implements ConsumerState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: ConsumerContext) {}

  public async init() {
    await deferred(onInitialized => {
      this.ctx.compareExchangeState(new InitializedState(this.ctx, {onInitialized}), this)
    })
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

class InitializedState implements ConsumerState {
  public readonly name = this.constructor.name
  private consumerTag!: Promise<string>

  constructor(private readonly ctx: ConsumerContext, private readonly args: {onInitialized: (err?: Error) => void}) {}

  public async onEnter() {
    // TODO: move to ErrorState when channel is closed
    // this.ctx.ch.once('close', () => {})
    // this.ctx.ch.once('error', () => {})

    this.consumerTag = (async () => {
      const {consumerTag} = await this.ctx.ch.consume(this.ctx.queueName, msg => this.onMessage(msg), {
        noAck: false
      })
      return consumerTag
    })()

    try {
      await this.consumerTag
      this.args.onInitialized()
    } catch (err) {
      this.ctx.processError(err)
      this.args.onInitialized(err)
    }
  }

  private async onMessage(msg: Message | null) {
    if (this.ctx.cancellationToken.isCanceled) {
      return
    }

    if (!msg) {
      this.ctx.processError(new Error('Consumer was canceled by broker'))
      return
    }

    this.ctx.statistics.processingMessageCount += 1

    try {
      const result = await this.ctx.processMessage(msg)

      if (result) {
        if (result.ack) {
          this.ctx.ch.ack(msg)
        } else {
          this.ctx.ch.nack(msg, false, result.requeue)
        }
      }

      this.ctx.statistics.processingMessageCount -= 1
      this.ctx.statistics.processedMessageCount += 1
    } catch (err) {
      this.ctx.statistics.processingMessageCount -= 1
      this.ctx.statistics.failedMessageCount += 1

      this.ctx.processError(err)
    }
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, InitializedState.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {consumerTag: this.consumerTag, onDestroyed}), this)
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {consumerTag: this.consumerTag, err}), this)
  }
}

class ErrorState implements ConsumerState {
  public readonly name = this.constructor.name

  constructor(
    private readonly ctx: ConsumerContext,
    private readonly args: {consumerTag: Promise<string>; err: Error}
  ) {}

  public onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    this.ctx.onError(this.args.err)
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, ErrorState.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {consumerTag: this.args.consumerTag, onDestroyed}),
        this
      )
    })
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}

class DestroyedState implements ConsumerState {
  public readonly name = this.constructor.name

  constructor(
    private readonly ctx: ConsumerContext,
    private readonly args: {consumerTag?: Promise<string>; onDestroyed: (err?: Error) => void}
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true

    // TODO: Think about timeout for waitFor operation
    // Wait for all active message processing operations are completed
    await waitFor(() => !this.ctx.statistics.processingMessageCount)

    try {
      const consumerTag = await this.args.consumerTag

      if (consumerTag) {
        await this.ctx.ch.cancel(consumerTag)
      }

      this.args.onDestroyed()
    } catch (err) {
      this.ctx.processError(err)
      this.args.onDestroyed(err)
    }
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, DestroyedState.name)
  }

  public async destroy() {
    throwUnsupportedSignal(this.destroy.name, DestroyedState.name)
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}
