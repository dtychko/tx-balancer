import {Channel, Message} from 'amqplib'
import MessageBalancer3 from './balancing/MessageBalancer3'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {setImmediateAsync, waitFor} from './utils'
import {on} from 'cluster'

interface ConsumerContext {
  readonly ch: Channel
  readonly messageBalancer: MessageBalancer3
  readonly onError: (err: Error) => void
  readonly inputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string

  readonly handleMessage: (msg: Message | null) => void

  readonly setState: (state: ConsumerState) => void
  readonly compareAndExchangeState: (comparand: ConsumerState, state: () => ConsumerState) => boolean

  processingMessageCount: number
}

interface ConsumerState {
  readonly init: () => Promise<void>
  readonly destroy: () => Promise<void>
  readonly handleMessage: (msg: Message | null) => Promise<void>
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

    this.state = initialState({
      ch,
      messageBalancer,
      onError,
      inputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,

      handleMessage: msg => {
        this.state.handleMessage(msg)
      },

      setState: async state => {
        this.state = state
      },
      compareAndExchangeState: (comparand, getState) => {
        if (this.state !== comparand) {
          return false
        }
        this.state = getState()
        return true
      },

      processingMessageCount: 0
    })
  }

  init() {
    return this.state.init()
  }

  destroy() {
    return this.state.destroy()
  }
}

function initialState(ctx: ConsumerContext): ConsumerState {
  return {
    async init() {
      await promise(onInitialized => {
        ctx.setState(startedState(ctx, onInitialized))
      })
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, Promise.resolve(''), onDestroyed))
      })
    },
    async handleMessage() {
      throwUnsupportedSignal(this.handleMessage.name, initialState.name)
    }
  }
}

function startedState(ctx: ConsumerContext, onInitialized: (err?: Error) => void): ConsumerState {
  const consumerTag = (async () => {
    return (await ctx.ch.consume(inputQueueName, msg => ctx.handleMessage(msg), {noAck: false})).consumerTag
  })()

  const state = {
    async init() {
      throwUnsupportedSignal(this.init.name, startedState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, Promise.resolve(''), onDestroyed))
        onInitialized(new Error('Consumer was destroyed'))
      })
    },
    async handleMessage(msg: Message | null) {
      if (!msg) {
        ctx.setState(errorState(ctx, consumerTag, new Error('Consumer was canceled by broker')))
        return
      }

      ctx.processingMessageCount += 1

      try {
        const {content, properties} = msg
        const partitionGroup = msg.properties.headers[partitionGroupHeader]
        const partitionKey = msg.properties.headers[partitionKeyHeader]
        await ctx.messageBalancer.storeMessage({partitionGroup, partitionKey, content, properties})
        ctx.ch.ack(msg)
      } catch (err) {
        ctx.compareAndExchangeState(state, () => errorState(ctx, consumerTag, err))
      } finally {
        ctx.processingMessageCount -= 1
      }
    }
  }

  ;(async () => {
    try {
      await consumerTag
      onInitialized()
    } catch (err) {
      if (ctx.compareAndExchangeState(state, () => errorState(ctx, consumerTag, err))) {
        onInitialized(err)
      }
    }
  })()

  return state
}

function errorState(ctx: ConsumerContext, consumerTag: Promise<string>, err: Error): ConsumerState {
  ;(async () => {
    await setImmediateAsync()
    ctx.onError(err)
  })()

  return {
    async init() {
      throwUnsupportedSignal(this.init.name, errorState.name)
    },
    async destroy() {
      await promise(onDestroyed => {
        ctx.setState(destroyedState(ctx, consumerTag, onDestroyed))
      })
    },
    async handleMessage() {}
  }
}

function destroyedState(
  ctx: ConsumerContext,
  consumerTag: Promise<string>,
  onDestroyed: (err?: Error) => void
): ConsumerState {
  ;(async () => {
    try {
      const cTag = await consumerTag
      if (cTag) {
        await ctx.ch.cancel(cTag)
      }

      await waitFor(() => !ctx.processingMessageCount)
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
    async handleMessage() {}
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
