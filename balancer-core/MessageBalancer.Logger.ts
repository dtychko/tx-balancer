export interface MessageBalancerLogger {
  log: (args: MessageBalancerLoggerArgs) => void
}

interface MessageBalancerLoggerArgs {
  level: MessageBalancerLoggerLevel
  message: string
  eventType: string
  duration?: number
  error?: Error
  meta?: unknown
}

type MessageBalancerLoggerLevel = 'debug' | 'info' | 'warn' | 'error'

type MethodPerLevelLogger = {
  [L in MessageBalancerLoggerLevel]: MethodPerLevelLoggerMethod
}

// tslint:disable-next-line:no-any
type MethodPerLevelLoggerMethod = (msg: string | Error, meta?: any) => MethodPerLevelLogger

const noop = () => undefined

export const emptyLogger: MessageBalancerLogger = {
  log: noop
}

export function createMethodPerLevelLoggerAdapter(logger: MethodPerLevelLogger): MessageBalancerLogger {
  return {
    log(args: MessageBalancerLoggerArgs) {
      const {level, message, eventType, duration, error, meta} = args

      // eslint-disable-next-line security/detect-object-injection
      logger[level](message, {
        eventType,
        duration,
        ...(error ? dumpError(error) : undefined),
        ...(typeof meta === 'object' ? meta : undefined)
      })
    }
  }
}

function dumpError(e: Error) {
  return {
    error: {
      name: e.name,
      message: e.message,
      callstack: e.stack
    }
  }
}
