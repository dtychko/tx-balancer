import {Channel, Message} from 'amqplib'
import MessageBalancer3 from './balancing/MessageBalancer3'
import {waitFor} from './utils'

interface ConsumerContext {
  readonly ch: Channel
  readonly messageBalancer: MessageBalancer3
  readonly onError: (err: Error) => void
  readonly inputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string

  readonly compareExchangeState: (toState: ConsumerState, fromState: ConsumerState) => boolean
  readonly handleMessage: (msg: Message | null) => void
}

interface ConsumerState {
  readonly onEnter: () => void

  readonly init: () => Promise<void>
  readonly destroy: () => Promise<void>
  readonly handleMessage: (msg: Message | null) => void
}

export default class InputQueueConsumer {
  private state: ConsumerState

  constructor(params: {
    ch: Channel
    messageBalancer: MessageBalancer3
    onError: (err: Error) => void
    inputQueueName: string
    partitionGroupHeader: string
    partitionKeyHeader: string
  }) {
    const {ch, messageBalancer, onError, inputQueueName, partitionGroupHeader, partitionKeyHeader} = params

    const ctx = {
      ch,
      messageBalancer,
      onError,
      inputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,

      compareExchangeState: (toState, fromState) => {
        if (this.state === fromState) {
          this.state = toState
          this.state.onEnter()
          return true
        }

        return false
      },
      handleMessage: msg => {
        this.state.handleMessage(msg)
      }
    } as ConsumerContext

    this.state = new InitialState(ctx)
    this.state.onEnter()
  }

  init() {
    return this.state.init()
  }

  destroy() {
    return this.state.destroy()
  }
}

class InitialState implements ConsumerState {
  constructor(private readonly ctx: ConsumerContext) {}

  public onEnter() {}

  public async init() {
    await deferred(onInitialized => {
      this.ctx.compareExchangeState(new InitializedState(this.ctx, {onInitialized}), this)
    })
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {
          consumerTag: undefined,
          statistics: {processingMessageCount: 0},
          onDestroyed
        }),
        this
      )
    })
  }

  public handleMessage() {
    throwUnsupportedSignal(this.handleMessage.name, InitialState.name)
  }
}

class InitializedState implements ConsumerState {
  private statistics: {processingMessageCount: number} = {processingMessageCount: 0}
  private consumerTag?: Promise<string>

  constructor(private readonly ctx: ConsumerContext, private readonly args: {onInitialized: (err?: Error) => void}) {}

  public async onEnter() {
    // TODO: move to ErrorState when channel is closed
    // this.ctx.ch.once('close', () => {})
    // this.ctx.ch.once('error', () => {})

    this.consumerTag = (async () => {
      return (await this.ctx.ch.consume(this.ctx.inputQueueName, msg => this.ctx.handleMessage(msg), {noAck: false}))
        .consumerTag
    })()

    try {
      await this.consumerTag
      this.args.onInitialized()
    } catch (err) {
      this.ctx.compareExchangeState(
        new ErrorState(this.ctx, {consumerTag: this.consumerTag, statistics: this.statistics, err}),
        this
      )
      this.args.onInitialized(err)
    }
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, InitializedState.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {consumerTag: this.consumerTag, statistics: this.statistics, onDestroyed}),
        this
      )
    })
  }

  public async handleMessage(msg: Message | null) {
    if (!msg) {
      this.ctx.compareExchangeState(
        new ErrorState(this.ctx, {
          consumerTag: this.consumerTag,
          statistics: this.statistics,
          err: new Error('Consumer was canceled by broker')
        }),
        this
      )
      return
    }

    this.statistics.processingMessageCount += 1

    try {
      const {content, properties} = msg
      const partitionGroup = msg.properties.headers[this.ctx.partitionGroupHeader]
      const partitionKey = msg.properties.headers[this.ctx.partitionKeyHeader]
      await this.ctx.messageBalancer.storeMessage({partitionGroup, partitionKey, content, properties})
      this.ctx.ch.ack(msg)
    } catch (err) {
      this.ctx.compareExchangeState(
        new ErrorState(this.ctx, {consumerTag: this.consumerTag!, statistics: this.statistics, err}),
        this
      )
    } finally {
      this.statistics.processingMessageCount -= 1
    }
  }
}

class ErrorState implements ConsumerState {
  constructor(
    private readonly ctx: ConsumerContext,
    private readonly args: {
      consumerTag: Promise<string> | undefined
      statistics: {processingMessageCount: number}
      err: Error
    }
  ) {}

  public onEnter() {
    this.ctx.onError(this.args.err)
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, ErrorState.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {
          consumerTag: this.args.consumerTag,
          statistics: this.args.statistics,
          onDestroyed
        }),
        this
      )
    })
  }

  public handleMessage() {
    // Ignore all messages after the error occurred
  }
}

class DestroyedState implements ConsumerState {
  constructor(
    private readonly ctx: ConsumerContext,
    private readonly args: {
      consumerTag: Promise<string> | undefined
      statistics: {processingMessageCount: number}
      onDestroyed: (err?: Error) => void
    }
  ) {}

  public async onEnter() {
    try {
      // Wait for all active message processing operations are completed
      // TODO: Think about timeout for waitFor operation
      await waitFor(() => !this.args.statistics.processingMessageCount)

      const cTag = await this.args.consumerTag
      if (cTag !== undefined) {
        await this.ctx.ch.cancel(cTag)
      }

      this.args.onDestroyed()
    } catch (err) {
      this.args.onDestroyed(err)
    }
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, DestroyedState.name)
  }

  public async destroy() {
    throwUnsupportedSignal(this.destroy.name, DestroyedState.name)
  }

  public handleMessage() {
    // Ignore all messages after the consumer was destroyed
  }
}

function throwUnsupportedSignal(signal: string, state: string) {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}

function deferred(executor: (fulfil: (err?: Error) => void) => void): Promise<void> {
  return new Promise<void>((res, rej) => {
    executor(err => {
      if (err) {
        rej(err)
      } else {
        res()
      }
    })
  })
}
