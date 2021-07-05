import {ConfirmChannel, Message} from 'amqplib'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {waitFor} from './utils'
import {QState} from './QState'
import {nanoid} from 'nanoid'
import {publishAsync} from './amqp/publishAsync'
import {emptyBuffer} from './constants'

interface ConsumerContext {
  readonly ch: ConfirmChannel
  readonly qState: QState
  readonly onError: (err: Error) => void
  readonly mirrorQueueName: string
  readonly outputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string
  readonly markerMessageId: string
  readonly setState: (state: ConsumerState) => Promise<void>
  processingMessageCount: number
  isInitialized: boolean
  isStopped: boolean
}

interface ConsumerState {
  readonly consume: () => Promise<void>
  readonly cancel: () => Promise<void>
  readonly waitForEnter?: () => Promise<void>
}

export default class MirrorQueueConsumer {
  private readonly ctx: ConsumerContext
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

    this.ctx = {
      ch,
      qState,
      onError,
      mirrorQueueName,
      outputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,
      markerMessageId: `__marker/${nanoid()}`,
      setState: async state => {
        this.state = state
        if (this.state.waitForEnter) {
          await this.state.waitForEnter()
        }
      },
      processingMessageCount: 0,
      isInitialized: false,
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
  let onInitialized: (err?: Error) => void
  const initializedPromise = new Promise<void>((res, rej) => {
    onInitialized = (err?: Error) => {
      if (err) {
        rej(err)
      } else {
        ctx.isInitialized = true
        res()
      }
    }
  })
  const consumerTagPromise = (async () => {
    return (await ctx.ch.consume(inputQueueName, msg => handleMessage(ctx, onInitialized, msg), {noAck: false}))
      .consumerTag
  })()
  ;(async () => {
    await consumerTagPromise
    await publishAsync(ctx.ch, '', ctx.mirrorQueueName, emptyBuffer, {
      persistent: true,
      messageId: ctx.markerMessageId
    })
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, startedState.name)
    },
    async cancel() {
      await ctx.setState(canceledState(ctx, consumerTagPromise))
    },
    async waitForEnter() {
      await initializedPromise
    }
  }
}

function canceledState(ctx: ConsumerContext, consumerTagPromise: Promise<string>) {
  const canceled = (async () => {
    ctx.isStopped = true
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
    async waitForEnter() {
      await canceled
    }
  }
}

async function handleMessage(ctx: ConsumerContext, onInitialized: (err?: Error) => void, msg: Message | null) {
  if (ctx.isStopped) {
    return
  }

  if (!msg) {
    ctx.isStopped = true
    onInitialized(new Error('Input queue consumer was canceled by broker'))
    ctx.onError(new Error('Input queue consumer was canceled by broker'))
    return
  }
  const messageId = msg.properties.messageId
  const deliveryTag = msg.fields.deliveryTag

  if (ctx.isInitialized) {
    const {registered} = ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)
    if (!registered) {
      ctx.ch.ack(msg)
    }
    return
  }

  if (messageId === ctx.markerMessageId) {
    ctx.ch.ack(msg)
    onInitialized()
    return
  }

  const partitionGroup = msg.properties.headers[partitionGroupHeader]
  const partitionKey = msg.properties.headers[partitionKeyHeader]
  ctx.qState.restoreMessage(messageId, partitionGroup, partitionKey, ctx.outputQueueName)
  ctx.qState.registerMirrorDeliveryTag(messageId, deliveryTag)
}

function throwUnsupportedSignal(signal: string, state: string) {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}
