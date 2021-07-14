import {ConfirmChannel, Message} from 'amqplib'
import {QState} from './QState'
import {callSafe, compareExchangeState, deferred, throwUnsupportedSignal} from './stateMachine'
import {partitionGroupHeader, partitionKeyHeader} from './config'
import {nanoid} from 'nanoid'
import {publishAsync} from './amqp/publishAsync'
import {emptyBuffer} from './constants'

interface ConsumerContext extends ConsumerArgs {
  cancellationToken: CancellationToken

  compareExchangeState: (toState: ConsumerState, fromState: ConsumerState) => boolean
  processMessage: (msg: Message) => void
  processError: (err: Error) => void
}

interface ConsumerArgs {
  ch: ConfirmChannel
  qState: QState
  onError: (err: Error) => void
  mirrorQueueName: string
  outputQueueName: string
  partitionGroupHeader: string
  partitionKeyHeader: string
}

interface CancellationToken {
  isCanceled: boolean
}

interface ConsumerState {
  name: string
  onEnter?: () => void

  init: () => Promise<void>
  destroy: () => Promise<void>
  processMessage: (msg: Message) => void
  processError: (err: Error) => void
}

export default class MirrorQueueConsumer {
  private readonly ctx: ConsumerContext
  private readonly state: {value: ConsumerState}

  constructor(args: ConsumerArgs) {
    this.ctx = {
      ...args,

      onError: err => {
        callSafe(() => args.onError(err))
      },

      cancellationToken: {isCanceled: false},

      compareExchangeState: (toState, fromState) => {
        return compareExchangeState(this.state, toState, fromState)
      },
      processMessage: msg => {
        this.state.value.processMessage(msg)
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

  public status() {
    return {
      state: this.state.value.name
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
      this.ctx.compareExchangeState(new InitializingState(this.ctx, {onInitialized}), this)
    })
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  public processMessage(_: Message) {
    throwUnsupportedSignal(this.processMessage.name, this.name)
  }

  public processError(_: Error) {
    throwUnsupportedSignal(this.processError.name, this.name)
  }
}

class InitializingState implements ConsumerState {
  public readonly name = this.constructor.name

  private readonly markerMessageId: string = `__marker/${nanoid()}`
  private consumerTag!: Promise<string>

  constructor(private readonly ctx: ConsumerContext, private readonly args: {onInitialized: (err?: Error) => void}) {}

  public async onEnter() {
    this.consumerTag = (async () => {
      const {consumerTag} = await this.ctx.ch.consume(this.ctx.mirrorQueueName, msg => this.onMessage(msg), {
        noAck: false
      })
      return consumerTag
    })()

    try {
      await publishAsync(this.ctx.ch, '', this.ctx.mirrorQueueName, emptyBuffer, {
        persistent: true,
        messageId: this.markerMessageId
      })
      await this.consumerTag

      this.args.onInitialized()
    } catch (err) {
      this.ctx.processError(err)
    }
  }

  private onMessage(msg: Message | null) {
    if (this.ctx.cancellationToken.isCanceled) {
      return
    }

    if (!msg) {
      this.ctx.processError(new Error('Consumer was canceled by broker'))
      return
    }

    this.ctx.processMessage(msg)
  }

  public async init() {
    throwUnsupportedSignal(this.init.name, this.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {consumerTag: this.consumerTag, onDestroyed}), this)
    })
  }

  public processMessage(msg: Message) {
    try {
      const messageId = msg.properties.messageId

      if (messageId === this.markerMessageId) {
        this.ctx.ch.ack(msg)
        this.ctx.compareExchangeState(new InitializedState(this.ctx, {consumerTag: this.consumerTag}), this)
        this.args.onInitialized()
        return
      }

      const deliveryTag = msg.fields.deliveryTag
      const partitionGroup = msg.properties.headers[partitionGroupHeader]
      const partitionKey = msg.properties.headers[partitionKeyHeader]

      this.ctx.qState.restoreMessage(messageId, partitionGroup, partitionKey, this.ctx.outputQueueName)
      this.ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)
    } catch (err) {
      this.ctx.processError(err)
    }
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {consumerTag: this.consumerTag, err}), this)
    this.args.onInitialized(err)
  }
}

class InitializedState implements ConsumerState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: ConsumerContext, private readonly args: {consumerTag: Promise<string>}) {}

  public async init() {
    throwUnsupportedSignal(this.init.name, this.name)
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {consumerTag: this.args.consumerTag, onDestroyed}),
        this
      )
    })
  }

  public processMessage(msg: Message) {
    try {
      const deliveryTag = msg.fields.deliveryTag
      const messageId = msg.properties.messageId
      const {registered} = this.ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)

      if (!registered) {
        this.ctx.ch.ack(msg)
      }
    } catch (err) {
      this.ctx.processError(err)
    }
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {consumerTag: this.args.consumerTag, err}), this)
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
    throwUnsupportedSignal(this.init.name, this.name)
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

  public processMessage(_: Message) {}
}

class DestroyedState implements ConsumerState {
  public readonly name = this.constructor.name

  constructor(
    private readonly ctx: ConsumerContext,
    private readonly args: {consumerTag?: Promise<string>; onDestroyed: (err?: Error) => void}
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true

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
    throwUnsupportedSignal(this.init.name, this.name)
  }

  public async destroy() {
    throwUnsupportedSignal(this.destroy.name, this.name)
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }

  public processMessage(_: Message) {}
}
