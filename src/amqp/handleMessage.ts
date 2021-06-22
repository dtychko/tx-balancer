import {ConsumeMessage} from 'amqplib'

export function handleMessage(callback: (msg: ConsumeMessage) => void) {
  return (msg: ConsumeMessage | null) => {
    if (!msg) {
      console.error('Consumer is cancelled by server')
      process.exit(1)
    }

    try {
      callback(msg)
    } catch (err) {
      console.error(err)
      process.exit(1)
    }
  }
}
