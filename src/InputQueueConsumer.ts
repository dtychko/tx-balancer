import {Channel, Message} from 'amqplib'
import MessageBalancer3 from './balancing/MessageBalancer3'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'

interface ConsumerContext {
  readonly ch: Channel
  readonly messageBalancer: MessageBalancer3
  readonly onError: (err: Error) => void
  readonly setState: (state: ConsumerState) => Promise<void>
  readonly inputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string
  processingMessageCount: number
  isStopped: boolean
}

interface ConsumerState {
  readonly consume: () => Promise<void>
  readonly cancel: () => Promise<void>
  readonly waitForEnter?: () => Promise<void>
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
      setState: async state => {
        this.state = state
        if (this.state.waitForEnter) {
          await this.state.waitForEnter()
        }
      },
      inputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,
      processingMessageCount: 0,
      isStopped: false
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
      await ctx.setState(startedState(ctx))
    },
    async cancel() {
      throwUnsupportedSignal(this.cancel.name, initialState.name)
    }
  }
}

function startedState(ctx: ConsumerContext) {
  const consumerTagPromise = (async () => {
    return (await ctx.ch.consume(inputQueueName, msg => handleMessage(ctx, msg), {noAck: false})).consumerTag
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, startedState.name)
    },
    async cancel() {
      await ctx.setState(canceledState(ctx, consumerTagPromise))
    },
    async waitForEnter() {
      await consumerTagPromise
    }
  }
}

async function handleMessage(ctx: ConsumerContext, msg: Message | null) {
  if (ctx.isStopped) {
    return
  }

  if (!msg) {
    ctx.isStopped = true
    ctx.onError(new Error('Input queue consumer was canceled by broker'))
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
    ctx.isStopped = true
    ctx.onError(err)
  } finally {
    ctx.processingMessageCount -= 1
  }
}

function canceledState(ctx: ConsumerContext, consumerTagPromise: Promise<string>) {
  const canceled = (async () => {
    ctx.isStopped = true
    await ctx.ch.cancel(await consumerTagPromise)
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, canceledState.name)
    },
    async cancel() {
      throwUnsupportedSignal(this.cancel.name, canceledState.name)
    },
    async waitForEnter() {
      await canceled
    }
  }
}

function throwUnsupportedSignal(signal: string, state: string) {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}
