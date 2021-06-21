import {ConsumeMessage} from 'amqplib'

export function handleMessage(callback: (msg: ConsumeMessage) => void) {
  return (msg: ConsumeMessage | null) => {
    if (!msg) {
      const errorMessage = 'Consumer is cancelled by server'
      console.error(errorMessage)
      throw new Error(errorMessage)
    }

    try {
      callback(msg)
    } catch (err) {
      console.error(err)
      throw err
    }
  }
}
