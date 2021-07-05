import {ConfirmChannel, Message} from 'amqplib'
import {inputQueueName, partitionGroupHeader, partitionKeyHeader} from './config'
import {QState} from './QState'
import {nanoid} from 'nanoid'
import {publishAsync} from './amqp/publishAsync'
import {emptyBuffer} from './constants'

interface ConsumerContext {
  readonly ch: ConfirmChannel
  readonly qState: QState
  readonly mirrorQueueName: string
  readonly outputQueueName: string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string

  readonly setState: (state: ConsumerState) => Promise<void>
  readonly onError: (err: Error) => void
  readonly onInitialized: () => void
  readonly onCanceled: () => void
  readonly waitForInitialized: () => Promise<void>

  readonly markerMessageId: string
  readonly isInitialized: boolean
  readonly isStopped: boolean
}

interface ConsumerState {
  readonly consume: () => Promise<void>
  readonly cancel: () => Promise<void>
  readonly waitForEnter?: () => Promise<void>
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

    let initializedRes: () => void
    let initializedRej: () => void
    const initialized = new Promise<void>((res, rej) => {
      initializedRes = res
      initializedRej = rej
    })

    const ctx = {
      ch,
      qState,
      mirrorQueueName,
      outputQueueName,
      partitionGroupHeader,
      partitionKeyHeader,

      setState: async (state: ConsumerState) => {
        this.state = state
        if (this.state.waitForEnter) {
          await this.state.waitForEnter()
        }
      },
      onError: (err: Error) => {
        ctx.isStopped = true
        if (!ctx.isInitialized) {
          initializedRej()
        }
        onError(err)
      },
      onInitialized: () => {
        ctx.isInitialized = true
        initializedRes()
      },
      onCanceled: () => {
        ctx.isStopped = true
      },
      waitForInitialized: () => {
        return initialized
      },

      markerMessageId: `__marker/${nanoid()}`,
      isInitialized: false,
      isStopped: false
    }

    this.state = initialState(ctx)
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
      await ctx.setState(initializedState(ctx))
    },
    async cancel() {
      throwUnsupportedSignal(this.cancel.name, initialState.name)
    }
  }
}

function initializedState(ctx: ConsumerContext) {
  const consumerTagPromise = (async () => {
    // TODO: handle errors
    return (await ctx.ch.consume(inputQueueName, msg => handleMessage(ctx, msg), {noAck: false})).consumerTag
  })()

  ;(async () => {
    // TODO: handle errors
    await publishAsync(ctx.ch, '', ctx.mirrorQueueName, emptyBuffer, {
      persistent: true,
      messageId: ctx.markerMessageId
    })
  })()

  return {
    async consume() {
      throwUnsupportedSignal(this.consume.name, initializedState.name)
    },
    async cancel() {
      await ctx.setState(canceledState(ctx, consumerTagPromise))
    },
    async waitForEnter() {
      await ctx.waitForInitialized()
    }
  }
}

function canceledState(ctx: ConsumerContext, consumerTagPromise: Promise<string>) {
  const canceled = (async () => {
    ctx.onCanceled()
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

async function handleMessage(ctx: ConsumerContext, msg: Message | null) {
  if (ctx.isStopped) {
    return
  }

  if (!msg) {
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
    ctx.onInitialized()
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
