export const amqpUri = 'amqp://guest:guest@localhost:5672/';
export const inputQueueName = 'input';
export const responseQueueName = 'response';

export const outputQueueName = index => `output_${index}`;
export const outputMirrorQueueName = index => `${outputQueueName(index)}.mirror`;
