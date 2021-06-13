export const amqpUri = 'amqp://guest:guest@localhost:5672/';
export const inputQueueName = '_tx_balancer_input';
export const responseQueueName = '_tx_balancer_response';

export const outputQueueName = index => `_tx_balancer_output_${index}`;
export const outputMirrorQueueName = index => `${outputQueueName(index)}.mirror`;
