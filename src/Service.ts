import {CancellationToken, createDependencies, destroyDependencies, ServiceDependencies} from './Service.dependencies'
import {callSafe, compareExchangeState, deferred} from './stateMachine'

interface ServiceContext {
  onError: (err: Error) => void
  cancellationToken: CancellationToken

  compareExchangeState: (toState: ServiceState, fromState: ServiceState) => boolean
  processError: (err: Error) => void
}

interface ServiceState {
  onEnter?: () => void

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
  private dependencies!: Promise<ServiceDependencies>

  constructor(private readonly ctx: ServiceContext, private readonly args: {onStarted: (err?: Error) => void}) {}

  public async onEnter() {
    this.dependencies = (async () =>
      await createDependencies({
        onError: err => this.ctx.processError(err),
        cancellationToken: this.ctx.cancellationToken
      }))()

    try {
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
  constructor(private readonly ctx: ServiceContext, private readonly args: {dependencies: ServiceDependencies}) {}

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
  private destroyDependencies!: Promise<void>

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {dependencies: ServiceDependencies; onStopped: (err?: Error) => void}
  ) {}

  public async onEnter() {
    this.destroyDependencies = (async () => {
      await destroyDependencies(this.args.dependencies)
    })()

    try {
      await this.destroyDependencies

      this.ctx.compareExchangeState(new StoppedState(this.ctx), this)
      this.args.onStopped()
    } catch (err) {
      this.ctx.processError(err)
      this.args.onStopped(err)
    }
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
        new DestroyedState(this.ctx, {dependencies: this.waitForDestroyed(), onDestroyed}),
        this
      )
    })
  }

  public processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {dependencies: this.waitForDestroyed(), err}), this)
  }

  private async waitForDestroyed() {
    await this.destroyDependencies
    return {}
  }
}

class StoppedState implements ServiceState {
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
      this.ctx.compareExchangeState(
        new DestroyedState(this.ctx, {dependencies: Promise.resolve({}), onDestroyed}),
        this
      )
    })
  }

  processError(err: Error) {
    this.ctx.compareExchangeState(new ErrorState(this.ctx, {dependencies: Promise.resolve({}), err}), this)
  }
}

class ErrorState implements ServiceState {
  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {dependencies: Promise<ServiceDependencies>; err: Error}
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    this.ctx.onError(this.args.err)
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
  private destroyDependencies!: Promise<void>

  constructor(
    private readonly ctx: ServiceContext,
    private readonly args: {dependencies: Promise<ServiceDependencies>; onDestroyed: (err?: Error) => void}
  ) {}

  public async onEnter() {
    this.ctx.cancellationToken.isCanceled = true
    this.destroyDependencies = (async () => {
      const dependencies = await this.args.dependencies
      await destroyDependencies(dependencies)
    })()

    try {
      await this.destroyDependencies

      this.args.onDestroyed()
    } catch (err) {
      this.args.onDestroyed(err)
    }
  }

  public async start() {
    throw new Error('Unable to start. Service is destroyed')
  }

  public async stop() {
    throw new Error('Unable to stop. Service is destroyed')
  }

  public async destroy() {
    await this.destroyDependencies
  }

  public processError(err: Error) {
    this.ctx.onError(err)
  }
}
