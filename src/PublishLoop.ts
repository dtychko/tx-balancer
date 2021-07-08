import ExecutionSerializer from '@targetprocess/balancer-core/bin/MessageBalancer.Serializer'
import {MessageProperties} from 'amqplib'
import {Publisher} from './amqp/Publisher'
import MessageBalancer3, {MessageRef} from './balancing/MessageBalancer3'
import {emptyBuffer} from './constants'
import {QState} from './QState'
import {setImmediateAsync, waitFor} from './utils'

interface PublishLoopContext {
  readonly publisher: Publisher
  readonly qState: QState
  readonly messageBalancer: MessageBalancer3
  readonly mirrorQueueName: (queueName: string) => string
  readonly partitionGroupHeader: string
  readonly partitionKeyHeader: string
  readonly onError: (err: Error) => void
  readonly executor: ExecutionSerializer

  readonly compareExchangeState: (toState: PublishLoopState, fromState: PublishLoopState) => boolean
}

interface PublishLoopState {
  readonly onEnter?: () => void
  readonly onExit?: () => void

  readonly stats: () => PublishLoopStats
  readonly connectTo: (params: ConnectToParams) => void
  readonly trigger: () => {alreadyStarted: boolean}
  readonly destroy: () => Promise<void>
}

interface PublishLoopStats {
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
        if (fromState !== this.state) {
          return false
        }

        if (this.state.onExit) {
          this.state.onExit()
        }

        this.state = toState

        if (this.state.onEnter) {
          this.state.onEnter()
        }

        return true
      }
    })

    if (this.state.onEnter) {
      this.state.onEnter()
    }
  }

  public stats() {
    return this.state.stats()
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
  constructor(
    private readonly ctx: {compareExchangeState: (toState: PublishLoopState, fromState: PublishLoopState) => boolean}
  ) {}

  public stats() {
    return {
      state: 'Initial',
      processingMessageCount: 0,
      processedMessageCount: 0
    } as PublishLoopStats
  }

  public connectTo(params: ConnectToParams) {
    this.ctx.compareExchangeState(
      new ReadyState(
        {
          ...params,
          executor: new ExecutionSerializer(),
          compareExchangeState: this.ctx.compareExchangeState
        },
        {
          statistics: {processingMessageCount: 0, processedMessageCount: 0}
        }
      ),
      this
    )
  }

  public trigger() {
    return {alreadyStarted: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState({statistics: {processingMessageCount: 0, processedMessageCount: 0}, onDestroyed}),
        this
      )
    })
  }
}

class ReadyState implements PublishLoopState {
  constructor(
    private readonly ctx: PublishLoopContext,
    private readonly args: {statistics: {processingMessageCount: number; processedMessageCount: number}}
  ) {}

  public stats() {
    const {processingMessageCount, processedMessageCount} = this.args.statistics
    return {
      state: 'Ready',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStats
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ReadyState.name)
  }

  public trigger() {
    this.ctx.compareExchangeState(new LoopInProgressState(this.ctx, {statistics: this.args.statistics}), this)
    return {alreadyStarted: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState({statistics: {processingMessageCount: 0, processedMessageCount: 0}, onDestroyed}),
        this
      )
    })
  }
}

class LoopInProgressState implements PublishLoopState {
  private isActive: boolean = true

  constructor(
    private readonly ctx: PublishLoopContext,
    private readonly args: {statistics: {processingMessageCount: number; processedMessageCount: number}}
  ) {}

  public async onEnter() {
    try {
      for (let i = 1; this.isActive; i++) {
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
      this.ctx.compareExchangeState(new ErrorState(this.ctx, {statistics: this.args.statistics, err}), this)
    } finally {
      this.ctx.compareExchangeState(new ReadyState(this.ctx, {statistics: this.args.statistics}), this)
    }
  }

  private async scheduleMessageProcessing(messageRef: MessageRef, queueMessageId: string, queueName: string) {
    this.args.statistics.processingMessageCount += 1

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

      // TODO: Replace with check like 'this.args.statistics.isStopped'
      if (!this.isActive) {
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
      this.ctx.compareExchangeState(new ErrorState(this.ctx, {statistics: this.args.statistics, err}), this)
    } finally {
      this.args.statistics.processingMessageCount -= 1
      this.args.statistics.processedMessageCount += 1
    }
  }

  public onExit() {
    this.isActive = false
  }

  public stats() {
    const {processingMessageCount, processedMessageCount} = this.args.statistics
    return {
      state: 'LoopInProgress',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStats
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ErrorState.name)
  }

  public trigger() {
    return {alreadyStarted: true}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState({statistics: this.args.statistics, onDestroyed}), this)
    })
  }
}

class ErrorState implements PublishLoopState {
  constructor(
    private readonly ctx: PublishLoopContext,
    private readonly args: {statistics: {processingMessageCount: number; processedMessageCount: number}; err: Error}
  ) {}

  public onEnter() {
    this.ctx.onError(this.args.err)
  }

  public stats() {
    const {processingMessageCount, processedMessageCount} = this.args.statistics
    return {
      state: 'Error',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStats
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, ErrorState.name)
  }

  public trigger() {
    return {alreadyStarted: false}
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState({statistics: this.args.statistics, onDestroyed}), this)
    })
  }
}

class DestroyedState implements PublishLoopState {
  constructor(
    private readonly args: {
      statistics: {processingMessageCount: number; processedMessageCount: number}
      onDestroyed: (err?: Error) => void
    }
  ) {}

  public async onEnter() {
    await waitFor(() => !this.args.statistics.processingMessageCount)
    this.args.onDestroyed()
  }

  public stats() {
    const {processingMessageCount, processedMessageCount} = this.args.statistics
    return {
      state: 'Destroyed',
      processingMessageCount,
      processedMessageCount
    } as PublishLoopStats
  }

  public connectTo() {
    throwUnsupportedSignal(this.connectTo.name, DestroyedState.name)
  }

  public trigger() {
    return {alreadyStarted: false}
  }

  public async destroy() {
    await waitFor(() => !this.args.statistics.processingMessageCount)
  }
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
