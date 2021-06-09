import {startBalancer} from './balancer.mjs';
import {startClient} from './client.mjs';
import {startPublisher} from './publisher.mjs';

async function main() {
  const queueCount = 5;
  const queueSizeLimit = 100;
  const command = (process.argv[2] || '').toLowerCase();

  if (command === 'balancer' || command === 'all') {
    await startBalancer({
      queueCount,
      queueSizeLimit,
      inputPrefetchCount: queueCount * queueSizeLimit * 2,
      outputPrefetchCount: queueCount * queueSizeLimit * 2
    });
  }

  if (command === 'client' || command === 'all') {
    for (let i = 0; i < queueCount; i++) {
      await startClient({
        outputQueueIndex: i + 1,
        prefetchCount: queueSizeLimit
      });
    }
  }

  if (command === 'publisher' || command === 'all') {
    await startPublisher({
      messageCountLimit: 1000,
      messageSize: 16 * 1024
    });
  }
}

main();
