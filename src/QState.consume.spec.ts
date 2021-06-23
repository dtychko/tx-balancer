import {Channel, ConfirmChannel, Message, MessageProperties, Options} from 'amqplib'
import {MessagePropertyHeaders} from 'amqplib/properties'
import {nanoid} from 'nanoid'
import {QState} from './QState'
import {consumeMirrorQueues, consumeResponseQueue} from './QState.consume'

test('consumeResponseQueue', async () => {
  let messageHandler: (msg: Message) => void
  const acks = [] as Message[]
  const registrations = [] as {messageId: string; deliveryTag: number}[]

  const ch = {
    consume: (_: string, onMessage: (msg: Message) => void) => {
      messageHandler = onMessage
    },
    ack: (msg: Message) => {
      acks.push(msg)
    }
  } as Channel
  const qState = {
    registerResponseDeliveryTag: (messageId: string, deliveryTag: number) => {
      registrations.push({messageId, deliveryTag})
      return {registered: messageId !== 'decline'}
    }
  } as QState

  await consumeResponseQueue({ch, qState, responseQueueName: 'response'})

  messageHandler!({fields: {deliveryTag: 101}, properties: {messageId: 'message/101'}} as Message)

  expect(acks).toEqual([])
  expect(registrations).toEqual([{messageId: 'message/101', deliveryTag: 101}])

  messageHandler!({fields: {deliveryTag: 102}, properties: {messageId: 'decline'}} as Message)

  expect(acks).toEqual([{fields: {deliveryTag: 102}, properties: {messageId: 'decline'}}])
  expect(registrations).toEqual([
    {messageId: 'message/101', deliveryTag: 101},
    {messageId: 'decline', deliveryTag: 102}
  ])
})

test('consumeMirrorQueues', async () => {
  const markerMessages = new Map<string, string>()
  const messageHandlers = new Map<string, (msg: Message) => void>()
  const acks = [] as Message[]
  const restores = [] as {messageId: string; partitionGroup: string; partitionKey: string; queueName: string}[]
  const registrations = [] as {messageId: string; deliveryTag: number}[]

  const ch = {
    publish: (exchange: string, queue: string, content: Buffer, options: Options.Publish, callback: () => void) => {
      markerMessages.set(queue, options.messageId!)
      setTimeout(callback, 0)
      return true
    },
    consume: (queue: string, onMessage: (msg: Message) => void) => {
      messageHandlers.set(queue, onMessage)
    },
    ack: (msg: Message) => {
      acks.push(msg)
    }
  } as ConfirmChannel
  const qState = {
    restoreMessage(messageId: string, partitionGroup: string, partitionKey: string, queueName: string) {
      restores.push({messageId, partitionGroup, partitionKey, queueName})
    },
    registerMirrorDeliveryTag: (messageId: string, deliveryTag: number) => {
      registrations.push({messageId, deliveryTag})
      return {registered: messageId !== 'decline'}
    }
  } as QState

  let initialized = false
  const consumePromise = consumeMirrorQueues({
    ch,
    qState,
    outputQueueCount: 3,
    outputQueueName: i => `queue/${i}`,
    mirrorQueueName: i => `mirror/${i}`,
    partitionGroupHeader: 'x-partition-group',
    partitionKeyHeader: 'x-partition-key'
  }).then(() => (initialized = true))

  await waitFor(() => markerMessages.size === 3)
  await waitFor(() => messageHandlers.size === 3)

  let deliveryTag = 1
  for (const [queue, markerMessageId] of markerMessages) {
    const handler = messageHandlers.get(queue)!
    handler({
      fields: {deliveryTag: deliveryTag++},
      properties: {
        messageId: nanoid(),
        headers: {
          ['x-partition-group']: `${queue}/group/1`,
          ['x-partition-key']: 'key/1'
        } as MessagePropertyHeaders
      }
    } as Message)
    handler({
      fields: {deliveryTag: deliveryTag++},
      properties: {messageId: markerMessageId}
    } as Message)
  }

  expect(initialized).toBeFalsy()

  await consumePromise

  expect(restores.length).toBe(3)
  for (const x of restores) {
    expect(x.queueName).toBeTruthy()
    expect(x.partitionGroup).toEqual(`${x.queueName}/group/1`)
    expect(x.partitionKey).toEqual(`key/1`)
  }

  expect(registrations.length).toBe(3)
})

async function waitFor(condition: () => boolean) {
  while (!condition()) {
    await new Promise(res => {
      setTimeout(res, 10)
    })
  }
}
