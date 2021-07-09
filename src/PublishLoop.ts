import ExecutionSerializer from '@targetprocess/balancer-core/bin/MessageBalancer.Serializer'
import {MessageProperties} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import MessageBalancer3, {MessageRef} from './balancing/MessageBalancer3'
import {emptyBuffer} from './constants'
import {QState} from './QState'
import {setImmediateAsync, waitFor} from './utils'

interface PublishLoopContext extends ConnectToParams {
  executor: ExecutionSerializer
  statistics: PublishLoopStatistics
  cancellationToken: CancellationToken

  compareExchangeState: CompareExchangeState
}

interface PublishLoopStatistics {
  processingMessageCount: number
  processedMessageCount: number
}

interface CancellationToken {
  isCanceled: boolean
}

type CompareExchangeState = (toState: PublishLoopState, fromState: PublishLoopState) => boolean

interface PublishLoopState {
  onEnter?: () => void

  status: () => PublishLoopStatus
  connectTo: (params: ConnectToParams) => void
  trigger: () => {started: boolean}
  destroy: () => Promise<void>
}

interface PublishLoopStatus {
  state: 'Initial' | 'Ready' | 'LoopInProgress' | 'Error' | 'Destroyed'
  processingMessageCount: number
  processedMessageCount: number
}

interface ConnectToParams {
  publisher: Publisher
  qState: QState
  messageBalancer: MessageBalancer3
  mirrorQueueName: (queueName: string) => string
  partitionGroupHeader: string
  partitionKeyHeader: string
  onError: (err: Error) => void
}

export default class PublishLoop {
  private state: PublishLoopState

  constructor() {
    this.state = new InitialState({
      compareExchangeState: (toState, fromState) => {
        return this.state !== (this.state = compareExchangeState(this.state, toState, fromState))
      }
    })

    if (this.state.onEnter) {
      this.state.onEnter()
    }
  }

  public status() {
    return this.state.status()
  }

  public connectTo(params: ConnectToParams) {
    this.state.connectTo(params)
  }

  public trigger() {
    return this.state.trigger()
  }

  public async destroy() {
    await this.state.destroy
  }
}

class InitialState implements PublishLoopState {
  constructor(private readonly ctx: {compareExchangeState: CompareExchangeState}) {}

  public status() {
    return {
      state: 'Initial',
      processingMessageCount: 0,
      processedMessageCount: 0
    } as PublishLoopStatus
  }

  public connectTo(params: ConnectToParams) {
    this.ctx.compareExchangeState(
      new ReadyState({
        ...params,
        executor: new ExecutionSerializer(),
        statistics: {processingMessageCount: 0, processedMessageCount: 0},
        cancellationToken: {isCanceled: false},
        compareExchangeState: this.ctx.compareExchangeState
      }),
      this
    )
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(
          {
            statistics: {processingMessageCount: 0, processedMessageCount: 0},
            cancellationToken: {isCanceled: false}
          },
          {
            onDestroyed
          }
        ),
        this
      )
    })
  }
}

class ReadyState implements PublishLoopState {
  constructor(private readonly ctx: PublishLoopContext) {}

  public status() {
    const {processingMessageCount, processedMessageCount} = this.ctx.statistics
    return {
      state: 'Ready',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStatus
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ReadyState.name)
  }

  public trigger() {
    this.ctx.compareExchangeState(new LoopInProgressState(this.ctx), this)
    return {started: true}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }
}

class LoopInProgressState implements PublishLoopState {
  constructor(private readonly ctx: PublishLoopContext) {}

  public async onEnter() {
    try {
      for (let i = 1; !this.ctx.cancellationToken.isCanceled; i++) {
        const messageRef = this.ctx.messageBalancer.tryDequeueMessage(partitionGroup =>
          this.ctx.qState.canRegister(partitionGroup)
        )

        if (!messageRef) {
          break
        }

        const {partitionGroup, partitionKey} = messageRef
        const {queueMessageId, queueName} = this.ctx.qState.registerMessage(partitionGroup, partitionKey)

        this.scheduleMessageProcessing(messageRef, queueMessageId, queueName)

        if (i % 1 === 0) {
          // Let's sometimes give a chance to either
          // messages for other partitionGroup to be enqueued to messageBalancer
          // or empty slots for other partitionGroup to be appeared in QState
          await setImmediateAsync()
        }
      }
    } catch (err) {
      this.ctx.compareExchangeState(new ErrorState(this.ctx, {err}), this)
    } finally {
      this.ctx.compareExchangeState(new ReadyState(this.ctx), this)
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
        this.ctx.messageBalancer.getMessage(messageId)
      )
      const {content, properties} = message

      if (this.ctx.cancellationToken.isCanceled) {
        return
      }

      await Promise.all([
        this.ctx.publisher.publishAsync('', this.ctx.mirrorQueueName(queueName), emptyBuffer, {
          headers: {
            [this.ctx.partitionGroupHeader]: partitionGroup,
            [this.ctx.partitionKeyHeader]: partitionKey
          },
          persistent: true,
          messageId: queueMessageId
        }),
        this.ctx.publisher.publishAsync('', queueName, content, {
          ...(properties as MessageProperties),
          persistent: true,
          messageId: queueMessageId
        })
      ])

      await this.ctx.messageBalancer.removeMessage(messageId)
    } catch (err) {
      this.ctx.compareExchangeState(new ErrorState(this.ctx, {err}), this)
    } finally {
      this.ctx.statistics.processingMessageCount -= 1
      this.ctx.statistics.processedMessageCount += 1
    }
  }

  public status() {
    const {processingMessageCount, processedMessageCount} = this.ctx.statistics
    return {
      state: 'LoopInProgress',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStatus
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ErrorState.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }
}

class ErrorState implements PublishLoopState {
  constructor(private readonly ctx: PublishLoopContext, private readonly args: {err: Error}) {}

  public onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    this.ctx.onError(this.args.err)
  }

  public status() {
    const {processingMessageCount, processedMessageCount} = this.ctx.statistics
    return {
      state: 'Error',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStatus
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ErrorState.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }
}

class DestroyedState implements PublishLoopState {
  constructor(
    private readonly ctx: {
      statistics: PublishLoopStatistics
      cancellationToken: CancellationToken
    },
    private readonly args: {
      onDestroyed: (err?: Error) => void
    }
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    await waitFor(() => !this.ctx.statistics.processingMessageCount)
    this.args.onDestroyed()
  }

  public status() {
    const {processingMessageCount, processedMessageCount} = this.ctx.statistics
    return {
      state: 'Destroyed',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStatus
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, DestroyedState.name)
  }

  public trigger() {
    return {started: false}
  }

  public async destroy() {
    await waitFor(() => !this.ctx.statistics.processingMessageCount)
  }
}

function compareExchangeState<TState extends {onEnter?: () => void; onExit?: () => void}>(
  currentState: TState,
  toState: TState,
  fromState: TState
): TState {
  if (fromState !== currentState) {
    return currentState
  }

  if (fromState.onExit) {
    fromState.onExit()
  }

  if (toState.onEnter) {
    toState.onEnter()
  }

  return toState
}

function throwUnsupportedSignal(signal: string, state: string): never {
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
