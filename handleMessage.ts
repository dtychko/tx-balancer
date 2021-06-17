import {ConsumeMessage} from 'amqplib'

export function handleMessage(callback: (msg: ConsumeMessage) => void) {
  return (msg: ConsumeMessage | null) => {
    if (!msg) {
      throw new Error('Consumer is cancelled by server')
    }

    try {
      callback(msg)
    } catch (err) {
      console.error(err)
    }
  }
}
