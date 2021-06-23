import {Channel, ConfirmChannel, Message, Options} from 'amqplib'
import {MessagePropertyHeaders} from 'amqplib/properties'
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
  const deliveryTagRegistrations = [] as {messageId: string; deliveryTag: number}[]

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
      deliveryTagRegistrations.push({messageId, deliveryTag})
      return {registered: messageId !== 'decline'}
    }
  } as QState

  let initialized = false
  const consumePromise = consumeMirrorQueues({
    ch,
    qState,
    outputQueueCount: 3,
    outputQueueName: i => `queue/${i}`,
    mirrorQueueName: qName => `${qName}/mirror`,
    partitionGroupHeader: 'x-partition-group',
    partitionKeyHeader: 'x-partition-key'
  }).then(() => (initialized = true))

  // Waiting for all consumers started and all marker messages published
  await waitFor(() => markerMessages.size === 3)
  await waitFor(() => messageHandlers.size === 3)

  // Delivering a single message preceding the marker to each queue,
  // then delivering all marker messages back
  for (let i = 1; i <= 3; i++) {
    const markerMessageId = markerMessages.get(`queue/${i}/mirror`)!
    const handler = messageHandlers.get(`queue/${i}/mirror`)!

    handler({
      fields: {deliveryTag: i},
      properties: {
        messageId: `message/${i}`,
        headers: {
          ['x-partition-group']: `queue/${i}/group/1`,
          ['x-partition-key']: 'key/1'
        } as MessagePropertyHeaders
      }
    } as Message)
    handler({
      fields: {deliveryTag: i * 10},
      properties: {messageId: markerMessageId}
    } as Message)
  }

  expect(initialized).toBeFalsy()

  await consumePromise

  // Make sure all marker messages were acked
  expect(acks.length).toBe(3)
  for (let i = 0; i < 3; i++) {
    expect(acks[i].fields.deliveryTag).toBe((i + 1) * 10)
  }
  acks.splice(0)

  // Make sure all messages preceding markers were restored in QState
  expect(restores.length).toBe(3)
  for (let i = 0; i < 3; i++) {
    expect(restores[i].messageId).toBe(`message/${i + 1}`)
    expect(restores[i].queueName).toBe(`queue/${i + 1}`)
    expect(restores[i].partitionGroup).toEqual(`queue/${i + 1}/group/1`)
    expect(restores[i].partitionKey).toEqual(`key/1`)
  }

  // Make sure delivery tags for all messages preceding markers were registered in QState
  expect(deliveryTagRegistrations.length).toBe(3)
  for (let i = 0; i < 3; i++) {
    expect(deliveryTagRegistrations[i].messageId).toBe(`message/${i + 1}`)
    expect(deliveryTagRegistrations[i].deliveryTag).toBe(i + 1)
  }
  deliveryTagRegistrations.splice(0)

  // Delivering a single valid message + a single invalid message
  // to each queue after consumers initialization is completed
  for (let i = 1; i <= 3; i++) {
    const handler = messageHandlers.get(`queue/${i}/mirror`)!

    handler({
      fields: {deliveryTag: 10 + i},
      properties: {
        messageId: `message/${10 + i}`,
        headers: {
          ['x-partition-group']: `queue/${i}/group/1`,
          ['x-partition-key']: 'key/1'
        } as MessagePropertyHeaders
      }
    } as Message)
    handler({
      fields: {deliveryTag: 20 + i},
      properties: {
        messageId: 'decline',
        headers: {
          ['x-partition-group']: `queue/${i}/group/1`,
          ['x-partition-key']: 'key/1'
        } as MessagePropertyHeaders
      }
    } as Message)
  }

  // Waiting for all delivered messages are processed
  await waitFor(() => deliveryTagRegistrations.length === 6)

  // Make sure that delivery tags were registered for all delivered messages
  for (let i = 0; i < 3; i++) {
    expect(deliveryTagRegistrations[2 * i].messageId).toBe(`message/${10 + i + 1}`)
    expect(deliveryTagRegistrations[2 * i].deliveryTag).toBe(10 + i + 1)
    expect(deliveryTagRegistrations[2 * i + 1].messageId).toBe('decline')
    expect(deliveryTagRegistrations[2 * i + 1].deliveryTag).toBe(20 + i + 1)
  }

  // Make sure that all invalid messages were acked immediately
  expect(acks.length).toBe(3)
  for (let i = 0; i < 3; i++) {
    expect(acks[i].fields.deliveryTag).toBe(20 + i + 1)
  }
})

async function waitFor(condition: () => boolean) {
  while (!condition()) {
    await new Promise(res => {
      setTimeout(res, 10)
    })
  }
}
