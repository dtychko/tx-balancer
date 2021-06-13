import * as amqp from 'amqplib';
import {handleMessage, txCommitSchedule, txSelect} from './amqp.mjs';
import {amqpUri, inputQueueName, outputMirrorQueueName, outputQueueName, responseQueueName} from './config.mjs';
import {promisify} from 'util';

const setTimeoutAsync = promisify(setTimeout);

const messageDeliveryTagByMessageId = new Map();
const responseDeliveryTagByMessageId = new Map();

export async function startBalancer({queueCount, queueSizeLimit, outputPrefetchCount, inputPrefetchCount}) {
  const conn = await amqp.connect(amqpUri);

  const ch = await conn.createChannel();
  await txSelect(ch);
  await ch.prefetch(outputPrefetchCount);

  await assertResources(ch, queueCount);
  await consumeMirrorQueues(ch, queueCount);
  await consumeResponseQueue(ch);

  const inputCh = await conn.createChannel();
  await txSelect(inputCh);
  await inputCh.prefetch(inputPrefetchCount);

  await consumeInputQueue(inputCh, queueCount, queueSizeLimit);
}

async function consumeMirrorQueues(ch, queueCount) {
  const queueNames = Array.from({length: queueCount}, (_, i) => outputMirrorQueueName(i + 1));

  for (const queueName of queueNames) {
    await ch.consume(queueName, handleMessage(async msg => {
      const messageId = msg.properties.messageId;
      const deliveryTag = msg.fields.deliveryTag;

      const responseDeliveryTag = responseDeliveryTagByMessageId.get(messageId);
      if (responseDeliveryTag !== undefined) {
        await txCommitSchedule(ch, () => {
          ch.ack({fields: {deliveryTag}});
          ch.ack({fields: {deliveryTag: responseDeliveryTag}});
        });
        onMessageProcessed();
      } else {
        messageDeliveryTagByMessageId.set(messageId, deliveryTag);
      }
    }), {noAck: false});
  }
}

async function consumeResponseQueue(ch) {
  await ch.consume(responseQueueName, handleMessage(async msg => {
    const messageId = msg.properties.messageId;
    const deliveryTag = msg.fields.deliveryTag;

    const messageDeliveryTag = messageDeliveryTagByMessageId.get(messageId);
    if (messageDeliveryTag !== undefined) {
      await txCommitSchedule(ch, () => {
        ch.ack({fields: {deliveryTag: messageDeliveryTag}});
        ch.ack({fields: {deliveryTag}});
      });
      onMessageProcessed();
    } else {
      responseDeliveryTagByMessageId.set(messageId, deliveryTag);
    }
  }), {noAck: false});
}

async function consumeInputQueue(ch, queueCount, queueSizeLimit) {
  await ch.consume(inputQueueName, handleMessage(async msg => {
    const messageId = generateMessageId();
    const deliveryTag = msg.fields.deliveryTag;
    const outputQueueIndex = deliveryTag % queueCount + 1;

    while (processingMessageCount >= queueCount * queueSizeLimit) {
      await setTimeoutAsync(0);
    }

    onMessageProcessing();

    await txCommitSchedule(ch, () => {
      ch.ack(msg);
      ch.publish('', outputQueueName(outputQueueIndex), msg.content, {persistent: true, messageId});
      ch.publish('', outputMirrorQueueName(outputQueueIndex), Buffer.from(''), {persistent: true, messageId});
    });
  }), {noAck: false});
}

async function assertResources(ch, queueCount) {
  const outputQueueNames = Array.from({length: queueCount}, (_, i) => outputQueueName(i + 1));
  const outputMirrorQueueNames = Array.from({length: queueCount}, (_, i) => outputMirrorQueueName(i + 1));
  const queueNames = [
    inputQueueName,
    responseQueueName,
    ...outputQueueNames,
    ...outputMirrorQueueNames
  ];

  for (const queueName of queueNames) {
    await Promise.all([
      ch.assertQueue(queueName, {durable: true}),
      ch.purgeQueue(queueName)
    ]);
  }
}

let startedAt;
let publishedAt;
let processedMessageCount = 0;
let processingMessageCount = 0;

function onMessageProcessing() {
  processingMessageCount += 1;
}

function onMessageProcessed() {
  processingMessageCount -= 1;
  processedMessageCount += 1;

  if (processedMessageCount % 10000 === 0) {
    console.log(`[Balancer] Processed ${processedMessageCount} messages`);
  }
  // if (processedMessageCount === 10000) {
  //   console.log(`completed ${Date.now() - startedAt} ${Date.now() - publishedAt}`);
  // }
}

const messageIdPrefix = Date.now().toString();

let messageIdCounter = 0;

function generateMessageId() {
  return `${messageIdPrefix}/${messageIdCounter++}`;
}
