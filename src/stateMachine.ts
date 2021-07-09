export function throwUnsupportedSignal(signal: string, state: string): never {
  throw new Error(`Unsupported signal '${signal}' in state '${state}'`)
}

export function compareExchangeState<TState extends {onEnter?: () => void; onExit?: () => void}>(
  currentState: {value: TState},
  toState: TState,
  fromState: TState
): boolean {
  if (fromState !== currentState.value) {
    return false
  }

  if (fromState.onExit) {
    fromState.onExit()
  }

  currentState.value = toState

  if (toState.onEnter) {
    toState.onEnter()
  }

  return true
}

export function deferred(executor: (fulfil: (err?: Error) => void) => void): Promise<void> {
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

export function callSafe(action: () => void) {
  try {
    action()
  } catch {
    // Ignore any error
  }
}
