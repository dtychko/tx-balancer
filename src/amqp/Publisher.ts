import {ConfirmChannel, Options} from 'amqplib'
import {publishAsync} from './publishAsync'

interface PublisherState {
  publishAsync(exchange: string, routingKey: string, content: Buffer, options: Options.Publish): Promise<void>
}

interface PublisherContext {
  ch: ConfirmChannel
  setState: (state: PublisherState) => void
}

interface PublishJob {
  exchange: string
  routingKey: string
  content: Buffer
  options: Options.Publish
  res: () => void
  rej: (err: Error) => void
}

export class Publisher2 {
  constructor(private readonly ch: ConfirmChannel) {}

  public publishAsync(exchange: string, routingKey: string, content: Buffer, options: Options.Publish) {
    return publishAsync(this.ch, exchange, routingKey, content, options)
  }
}

export class Publisher {
  private state: PublisherState

  constructor(ch: ConfirmChannel) {
    this.state = new OpenState({ch, setState: next => (this.state = next)})
  }

  public publishAsync(exchange: string, routingKey: string, content: Buffer, options: Options.Publish) {
    return this.state.publishAsync(exchange, routingKey, content, options)
  }
}

class OpenState implements PublisherState {
  private readonly ctx: PublisherContext

  constructor(ctx: PublisherContext) {
    this.ctx = ctx
  }

  public publishAsync(exchange: string, routingKey: string, content: Buffer, options: Options.Publish) {
    return new Promise<void>((res, rej) => {
      const result = publish(this.ctx.ch, {exchange, routingKey, content, options, res, rej})

      if (!result) {
        console.log('PUBLISHER going to closed state')
        this.ctx.setState(new ClosedState(this.ctx))
      }
    })
  }
}

class ClosedState implements PublisherState {
  private readonly ctx: PublisherContext
  private readonly jobQueue = [] as PublishJob[]

  constructor(ctx: PublisherContext) {
    this.ctx = ctx
    this.waitForDrain()
  }

  public publishAsync(exchange: string, routingKey: string, content: Buffer, options: Options.Publish) {
    return new Promise<void>((res, rej) => {
      this.jobQueue.push({exchange, routingKey, content, options, res, rej})
    })
  }

  private waitForDrain() {
    this.ctx.ch.once('drain', () => {
      console.log('PUBLISHER drain')
      while (this.jobQueue.length) {
        const job = this.jobQueue.shift()!
        const result = publish(this.ctx.ch, job)

        if (!result) {
          break
        }
      }

      if (this.jobQueue.length) {
        this.waitForDrain()
      } else {
        console.log('PUBLISHER going to open state')
        this.ctx.setState(new OpenState(this.ctx))
      }
    })
  }
}

function publish(ch: ConfirmChannel, args: PublishJob) {
  return ch.publish(args.exchange, args.routingKey, args.content, args.options, err => {
    if (err) {
      args.rej(err)
    } else {
      args.res()
    }
  })
}
