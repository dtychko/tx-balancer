import * as amqp from 'amqplib';
import {amqpUri, inputQueueName} from './config.mjs';
import {txCommit, txSelect} from './amqp.mjs';

export async function startPublisher({messageCountLimit, messageSize}) {
  const conn = await amqp.connect(amqpUri);
  const ch = await conn.createChannel();

  await txSelect(ch);

  await ch.assertQueue(inputQueueName, {durable: true});

  let messageCountToSend = messageCountLimit;
  const content = generateString(messageSize);

  while (true) {
    if (messageCountToSend) {
      for (let i = 0; i < messageCountToSend; i++) {
        ch.publish('', inputQueueName, Buffer.from(content), {persistent: true});
      }
      await txCommit(ch);
    }

    const {messageCount} = await ch.checkQueue(inputQueueName);
    messageCountToSend = messageCountLimit - messageCount;
  }
}

function generateString(length) {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}
