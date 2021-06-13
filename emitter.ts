import {maxMessageCountPerQueue, queueStateByName, registerMessage, sessionByPartitionKey} from './state'
import {publishAsync} from './publishAsync'
import BalancedQueue from './balancedQueue'
import {ConfirmChannel} from 'amqplib'

const messageIdPrefix = Date.now().toString()
let messageIdCounter = 0

let ch: ConfirmChannel
let balancedQueue: BalancedQueue<unknown>

export function initEmitter(ch: ConfirmChannel, queue: BalancedQueue<unknown>) {
  balancedQueue = queue
}

export async function checkOutputQueues() {
  const queueNames = [...queueStateByName.entries()]
    .filter(x => x[1].messageCount < maxMessageCountPerQueue)
    .sort((x, y) => x[1].messageCount - y[1].messageCount)
    .map(x => x[0])

  const promises = [] as Promise<void>[]

  while (queueNames.length) {
    const queueName = queueNames.shift()!
    const queueState = queueStateByName.get(queueName)!

    const result = balancedQueue.tryDequeue(pk => !sessionByPartitionKey.has(pk) || queueState.partitionKeys.has(pk))
    if (!result) {
      continue
    }

    const messageId = `${messageIdPrefix}/${messageIdCounter++}`
    registerMessage(messageId, result.partitionKey, queueName)

    if (queueState.messageCount < maxMessageCountPerQueue) {
      queueNames.push(queueName)
    }

    promises.push(
      publishAsync(ch, 'output', queueName, result.value as Buffer, {
        messageId,
        persistent: true,
        headers: {
          ['x-partition-key']: result.partitionKey
        }
      })
    )
  }

  await Promise.all(promises)
}
