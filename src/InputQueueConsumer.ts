import {Channel, Message} from 'amqplib'
import MessageBalancer3 from './balancing/MessageBalancer3'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {setImmediateAsync, waitFor} from './utils'

interface ConsumerContext {
  readonly ch: Channel
  readonly messageBalancer: MessageBalancer3
  readonly onError: (err: Error) => void
  readonly inputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string

  readonly handleMessage: (msg: Message | null) => void

  readonly setState: (state: ConsumerState) => Promise<void>
  readonly compareAndExchangeState: (comparand: ConsumerState, state: () => ConsumerState) => boolean

  processingMessageCount: number
}

interface ConsumerState {
  readonly consume: () => Promise<void>
  readonly cancel: () => Promise<void>
  readonly handleMessage: (msg: Message | null) => Promise<void>
}

export default class InputQueueConsumer {
  private readonly ctx: ConsumerContext
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

    this.ctx = {
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
      processingMessageCount: 0,
    }

    this.state = initialState(this.ctx)
  }

  consume() {
    return this.state.consume()
  }

  async cancel() {
    return this.state.cancel()
  }
}

function initialState(ctx: ConsumerContext) {
  return {
    async consume() {
      await promise(onInitialized => {
        ctx.setState(startedState(ctx, onInitialized))
      })
    },
    async cancel() {
      await promise(onDestroyed => {
        ctx.setState(canceledState(ctx, Promise.resolve(''), onDestroyed))
      })
    },
    async handleMessage() {
      throwUnsupportedSignal(this.handleMessage.name, initialState.name)
    }
  }
}

function startedState(ctx: ConsumerContext, onInitialized: (err?: Error) => void) {
  const consumerTag = (async () => {
    return (await ctx.ch.consume(inputQueueName, msg => ctx.handleMessage(msg), {noAck: false})).consumerTag
  })()

  const state = {
    async consume() {
      throwUnsupportedSignal(this.consume.name, startedState.name)
    },
    async cancel() {
      await promise(onDestroyed => {
        ctx.setState(canceledState(ctx, Promise.resolve(''), onDestroyed))
        onInitialized(new Error("Consumer was destroyed"))
      })
    },
    async handleMessage(msg: Message | null) {
      if (!msg) {
        const err = new Error('Consumer was canceled by broker')
        ctx.setState(errorState(ctx, consumerTag, err))
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

  return state
}

function errorState(ctx: ConsumerContext, consumerTag: Promise<string>, err: Error) {
  ;(async () => {
    await setImmediateAsync()
    ctx.onError(err)
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, errorState.name)
    },
    async cancel() {
      await promise(onDestroyed => {
        ctx.setState(canceledState(ctx, consumerTag, onDestroyed))
      })
    },
    async handleMessage() {}
  }
}

function canceledState(ctx: ConsumerContext, consumerTagPromise: Promise<string>, onDestroyed: (err?: Error) => void) {
  const canceled = (async () => {
    await ctx.ch.cancel(await consumerTagPromise)
    await waitFor(() => !ctx.processingMessageCount)
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, canceledState.name)
    },
    async cancel() {
      throwUnsupportedSignal(this.cancel.name, canceledState.name)
    },
    async handleMessage() {
    }
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
