import * as amqp from 'amqplib';
import {amqpUri, outputQueueName, responseQueueName} from './config.mjs';
import {handleMessage, txCommitSchedule, txSelect} from './amqp.mjs';

export async function startClient({outputQueueIndex, prefetchCount}) {
  const conn = await amqp.connect(amqpUri);
  const ch = await conn.createChannel();

  await txSelect(ch);
  await ch.prefetch(prefetchCount);

  await ch.consume(outputQueueName(outputQueueIndex), handleMessage(async msg => {
    const messageId = msg.properties.messageId;

    await txCommitSchedule(ch, () => {
      ch.ack(msg);
      ch.publish('', responseQueueName, Buffer.from(''), {persistent: true, messageId});
    });
  }), {noAck: false});
}