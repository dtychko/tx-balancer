import {createDependencies, destroyDependencies, ServiceDependencies} from './Service.dependencies'
import {callSafe, CancellationToken, compareExchangeState, deferred} from './stateMachine'

interface ServiceContext {
  onError: (err: Error) => void
  cancellationToken: CancellationToken

  compareExchangeState: (toState: ServiceState, fromState: ServiceState) => boolean
  processError: (err: Error) => void
}

interface ServiceState {
  name: string
  onEnter?: () => void

  status?: () => ReturnType<typeof getServiceStatus>
  start: () => Promise<void>
  stop: () => Promise<void>
  destroy: () => Promise<void>
  processError: (err: Error) => void
}

export default class Service {
  private readonly ctx: ServiceContext
  private readonly state: {value: ServiceState}

  constructor(args: {onError: (err: Error) => void}) {
    this.ctx = {
      onError: err => callSafe(() => args.onError(err)),
      cancellationToken: {isCanceled: false},

      compareExchangeState: (toState, fromState) => {
        return compareExchangeState(this.state, toState, fromState)
      },
      processError: err => {
        this.state.value.processError(err)
      }
    }

    this.state = {value: new StoppedState(this.ctx)}

    if (this.state.value.onEnter) {
      this.state.value.onEnter()
    }
  }

  public status() {
    return {
      state: this.state.value.name,
      ...(this.state.value.status && this.state.value.status())
    }
  }

  public start() {
    return this.state.value.start()
  }

  public stop() {
    return this.state.value.stop()
  }

  public destroy() {
    return this.state.value.destroy()
  }
}

class StartingState implements ServiceState {
  public readonly name = this.constructor.name
  private dependencies!: Promise<ServiceDependencies>

  constructor(private readonly ctx: ServiceContext, private readonly args: {onStarted: (err?: Error) => void}) {}

  public async onEnter() {
    try {
      this.dependencies = createDependencies({
        onError: err => this.ctx.processError(err),
        cancellationToken: this.ctx.cancellationToken
      })
      const dependencies = await this.dependencies

      this.ctx.compareExchangeState(new StartedState(this.ctx, {dependencies}), this)
      this.args.onStarted()
    } catch (err) {
      this.ctx.processError(err)
      this.args.onStarted(err)
    }
  }

  public async start() {
    throw new Error('Unable to start. Service is already starting')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is still starting')
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {dependencies: this.dependencies, onDestroyed}), this)
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {dependencies: this.dependencies, err}), this)
  }
}

class StartedState implements ServiceState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: ServiceContext, private readonly args: {dependencies: ServiceDependencies}) {}

  public status() {
    return getServiceStatus(this.args.dependencies)
  }

  public async start() {
    throw new Error('Unable to start. Service is already started')
  }

  public async stop() {
    await deferred(onStopped => {
      this.ctx.compareExchangeState(
        new StoppingState(this.ctx, {dependencies: this.args.dependencies, onStopped}),
        this
      )
    })
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {dependencies: Promise.resolve(this.args.dependencies), onDestroyed}),
        this
      )
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(
      new ErrorState(this.ctx, {dependencies: Promise.resolve(this.args.dependencies), err}),
      this
    )
  }
}

class StoppingState implements ServiceState {
  public readonly name = this.constructor.name
  private destroyDependenciesPromise!: Promise<undefined>

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {dependencies: ServiceDependencies; onStopped: (err?: Error) => void}
  ) {}

  public async onEnter() {
    try {
      this.destroyDependenciesPromise = this.destroyDependencies()
      await this.destroyDependenciesPromise

      this.ctx.compareExchangeState(new StoppedState(this.ctx), this)
      this.args.onStopped()
    } catch (err) {
      this.ctx.processError(err)
      this.args.onStopped(err)
    }
  }

  private async destroyDependencies() {
    await destroyDependencies(this.args.dependencies)
    return undefined
  }

  public status() {
    return getServiceStatus(this.args.dependencies)
  }

  public async start() {
    throw new Error('Unable to start. Service is still stopping')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is already stopping')
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {dependencies: this.destroyDependenciesPromise, onDestroyed}),
        this
      )
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {dependencies: this.destroyDependenciesPromise, err}), this)
  }
}

class StoppedState implements ServiceState {
  public readonly name = this.constructor.name

  constructor(private readonly ctx: ServiceContext) {}

  public async start() {
    await deferred(onStarted => {
      this.ctx.compareExchangeState(new StartingState(this.ctx, {onStarted}), this)
    })
  }

  public async stop() {
    throw new Error("Unable to stop. Service wasn't started")
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(new DestroyedState(this.ctx, {onDestroyed}), this)
    })
  }

  processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {err}), this)
  }
}

class ErrorState implements ServiceState {
  public readonly name = this.constructor.name
  private dependencies?: ServiceDependencies

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {
      dependencies?: Promise<ServiceDependencies | undefined>
      err: Error
    }
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    ;(async () => {
      try {
        this.dependencies = await this.dependencies
      } catch {}
    })()

    this.ctx.onError(this.args.err)
  }

  public status() {
    return getServiceStatus(this.dependencies)
  }

  public async start() {
    throw new Error('Unable to start. Service is failed')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is failed')
  }

  public async destroy() {
    await deferred(onDestroyed => {
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {dependencies: this.args.dependencies, onDestroyed}),
        this
      )
    })
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}

class DestroyedState implements ServiceState {
  public readonly name = this.constructor.name
  private destroyDependenciesPromise!: Promise<void>
  private dependencies?: ServiceDependencies

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {
      dependencies?: Promise<ServiceDependencies | undefined>
      onDestroyed: (err?: Error) => void
    }
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true

    try {
      this.destroyDependenciesPromise = this.destroyDependencies()
      await this.destroyDependenciesPromise

      this.args.onDestroyed()
    } catch (err) {
      this.args.onDestroyed(err)
    }
  }

  private async destroyDependencies() {
    const dependencies = await this.tryGetDependencies()
    if (dependencies) {
      await destroyDependencies(dependencies)
    }
  }

  private async tryGetDependencies() {
    try {
      return (this.dependencies = await this.args.dependencies)
    } catch {
      return undefined
    }
  }

  public status() {
    return getServiceStatus(this.dependencies)
  }

  public async start() {
    throw new Error('Unable to start. Service is destroyed')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is destroyed')
  }

  public async destroy() {
    await this.destroyDependenciesPromise
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}

function getServiceStatus(dependencies?: ServiceDependencies) {
  if (!dependencies) {
    return undefined
  }

  return {
    qState: dependencies.qState?.status(),
    messageBalancer: dependencies.messageBalancer?.status(),
    publishLoop: dependencies.publishLoop?.status(),
    inputQueueConsumer: dependencies.inputQueueConsumer?.status(),
    responseQueueConsumer: dependencies.responseQueueConsumer?.status(),
    mirrorQueueConsumers: dependencies.mirrorQueueConsumers?.map(consumer => consumer.status())
  }
}
