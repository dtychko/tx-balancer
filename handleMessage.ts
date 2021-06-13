import {ConsumeMessage} from 'amqplib'

export function handleMessage(callback: (msg: ConsumeMessage) => void) {
  return (msg: ConsumeMessage | null) => {
    if (!msg) {
      console.error('ERROR!')
      return
    }

    try {
      callback(msg)
    } catch (err) {
      console.error(err)
    }
  }
}
