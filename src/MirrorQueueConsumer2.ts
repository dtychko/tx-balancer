import {ConfirmChannel, Message} from 'amqplib'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {QState} from './QState'
import {nanoid} from 'nanoid'
import {publishAsync} from './amqp/publishAsync'
import {emptyBuffer} from './constants'
import {setImmediateAsync} from './utils'

interface ConsumerContext {
  readonly ch: ConfirmChannel
  readonly qState: QState
  readonly onError: (err: Error) => void
  readonly mirrorQueueName: string
  readonly outputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string

  readonly handleMessage: (msg: Message | null) => void

  readonly setState: (state: ConsumerState) => void
  readonly compareAndExchangeState: (comparand: ConsumerState, state: () => ConsumerState) => boolean
}

interface ConsumerState {
  readonly init: () => Promise<void>
  readonly destroy: () => Promise<void>
  readonly handleMessage: (msg: Message | null) => void
}

export default class MirrorQueueConsumer {
  private state: ConsumerState

  constructor(params: {
    ch: ConfirmChannel
    qState: QState
    onError: (err: Error) => void
    mirrorQueueName: string
    outputQueueName: string
    partitionGroupHeader: string
    partitionKeyHeader: string
  }) {
    const {ch, qState, onError, mirrorQueueName, outputQueueName, partitionGroupHeader, partitionKeyHeader} = params

    this.state = initialState({
      ch,
      qState,
      onError,
      mirrorQueueName,
      outputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,

      handleMessage: msg => {
        this.state.handleMessage(msg)
      },

      setState: (state: ConsumerState) => {
        this.state = state
      },
      compareAndExchangeState: (comparand, getState) => {
        if (this.state !== comparand) {
          return false
        }
        this.state = getState()
        return true
      }
    })
  }

  async init() {
    await this.state.init()
  }

  async destroy() {
    await this.state.destroy()
  }
}

function initialState(ctx: ConsumerContext) {
  return {
    async init() {
      await promise(onInitialized => {
        ctx.setState(consumingState(ctx, onInitialized))
      })
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, Promise.resolve(''), onDestroyed))
      })
    },
    handleMessage() {
      throwUnsupportedSignal(this.handleMessage.name, initialState.name)
    }
  }
}

function consumingState(ctx: ConsumerContext, onInitialized: (err?: Error) => void) {
  const consumerTag = (async () => {
    return (await ctx.ch.consume(inputQueueName, msg => ctx.handleMessage(msg), {noAck: false})).consumerTag
  })()

  const state = {
    async init() {
      throwUnsupportedSignal(this.init.name, consumingState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, consumerTag, onDestroyed))
        onInitialized(new Error('Consumer was destroyed'))
      })
    },
    handleMessage(msg: Message | null) {
      if (!msg) {
        const err = new Error('Consumer was canceled by broker')
        ctx.setState(errorState(ctx, consumerTag, err))
        onInitialized(err)
        return
      }

      try {
        const deliveryTag = msg.fields.deliveryTag
        const messageId = msg.properties.messageId
        const partitionGroup = msg.properties.headers[partitionGroupHeader]
        const partitionKey = msg.properties.headers[partitionKeyHeader]

        ctx.qState.restoreMessage(messageId, partitionGroup, partitionKey, ctx.outputQueueName)
        ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)
      } catch (err) {
        ctx.setState(errorState(ctx, consumerTag, err))
        onInitialized(err)
      }
    }
  }

  ;(async () => {
    try {
      await consumerTag

      if (ctx.compareAndExchangeState(state, () => initializingState(ctx, consumerTag, onInitialized))) {
        onInitialized()
      }
    } catch (err) {
      if (ctx.compareAndExchangeState(state, () => errorState(ctx, consumerTag, err))) {
        onInitialized(err)
      }
    }
  })()

  return state
}

function initializingState(ctx: ConsumerContext, consumerTag: Promise<string>, onInitialized: (err?: Error) => void) {
  const markerMessageId = `__marker/${nanoid()}`
  const state = {
    async init() {
      throwUnsupportedSignal(this.init.name, initializingState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, consumerTag, onDestroyed))
        onInitialized(new Error('Consumer was destroyed'))
      })
    },
    handleMessage(msg: Message | null) {
      if (!msg) {
        const err = new Error('Consumer was canceled by broker')
        ctx.setState(errorState(ctx, consumerTag, err))
        onInitialized(err)
        return
      }

      const messageId = msg.properties.messageId

      if (messageId === markerMessageId) {
        ctx.ch.ack(msg)
        ctx.setState(initializedState(ctx, consumerTag))
        onInitialized()
        return
      }

      const deliveryTag = msg.fields.deliveryTag
      const partitionGroup = msg.properties.headers[partitionGroupHeader]
      const partitionKey = msg.properties.headers[partitionKeyHeader]

      ctx.qState.restoreMessage(messageId, partitionGroup, partitionKey, ctx.outputQueueName)
      ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)
    }
  }

  ;(async () => {
    try {
      await publishAsync(ctx.ch, '', ctx.mirrorQueueName, emptyBuffer, {
        persistent: true,
        messageId: markerMessageId
      })
    } catch (err) {
      if (ctx.compareAndExchangeState(state, () => errorState(ctx, consumerTag, err))) {
        onInitialized(err)
      }
    }
  })()

  return state
}

function initializedState(ctx: ConsumerContext, consumerTag: Promise<string>) {
  return {
    async init() {
      throwUnsupportedSignal(this.init.name, initializedState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, consumerTag, onDestroyed))
      })
    },
    handleMessage(msg: Message | null) {
      if (!msg) {
        const err = new Error('Consumer was canceled by broker')
        ctx.setState(errorState(ctx, consumerTag, err))
        return
      }

      const deliveryTag = msg.fields.deliveryTag
      const messageId = msg.properties.messageId
      const {registered} = ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)

      if (!registered) {
        ctx.ch.ack(msg)
      }
    }
  }
}

function errorState(ctx: ConsumerContext, consumerTag: Promise<string>, err: Error) {
  ;(async () => {
    await setImmediateAsync()
    ctx.onError(err)
  })()

  return {
    async init() {
      throwUnsupportedSignal(this.init.name, destroyedState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, consumerTag, onDestroyed))
      })
    },
    handleMessage() {}
  }
}

function destroyedState(ctx: ConsumerContext, consumerTag: Promise<string>, onDestroyed: (err?: Error) => void) {
  ;(async () => {
    try {
      const cTag = await consumerTag
      if (cTag) {
        await ctx.ch.cancel(cTag)
      }

      onDestroyed()
    } catch (err) {
      onDestroyed(err)
    }
  })()

  return {
    async init() {
      throwUnsupportedSignal(this.init.name, destroyedState.name)
    },
    async destroy() {
      throwUnsupportedSignal(this.destroy.name, destroyedState.name)
    },
    handleMessage() {}
  }
}

function throwUnsupportedSignal(signal: string, state: string) {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}

function promise(executor: (fulfil: (err?: Error) => void) => void): Promise<void> {
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
