import {QStateWithLimits} from './qState'
import BalancedQueue from './balancedQueue'
import * as uuid from 'uuid'
import {ConfirmChannel} from 'amqplib'
import {publishAsync} from './publishAsync'
import {outputMirrorQueueName} from './config'

export async function startLoop(ch: ConfirmChannel, qState: QStateWithLimits, balancerQueue: BalancedQueue<Buffer>) {
  while (true) {
    const dequeueResult = balancerQueue.tryDequeue(qState.canPublish)
    if (!dequeueResult) {
      return
    }

    const {value, partitionKey} = dequeueResult
    const messageId = uuid.v4()
    const {queueName} = qState.registerOutputMessage(messageId, partitionKey)

    await publishAsync(ch, '', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId})
    await publishAsync(ch, '', queueName, value, {persistent: true, messageId})

    // TODO: Looks like publishing could be started concurrently for performance reason
    // await ch.tx(tx => {
    //   tx.publish('', queueName, value, {persistent: true, messageId})
    //   tx.publish('', outputMirrorQueueName(queueName), Buffer.from(''), {persistent: true, messageId})
    // })
  }
}
