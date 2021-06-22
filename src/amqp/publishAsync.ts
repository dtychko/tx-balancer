import {ConfirmChannel, Options} from 'amqplib'

export function publishAsync(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: Options.Publish
) {
  return new Promise<void>((res, rej) => {
    // TODO: Handle "false" with listening for 'drain' event
    ch.publish(exchange, routingKey, content, options, err => {
      if (err) {
        rej(err)
      } else {
        res()
      }
    })
  })
}
