import {startBalancer} from './balancer.mjs';
import {startClient} from './client.mjs';
import {startPublisher} from './publisher.mjs';

async function main() {
  console.error('Use cli.mjs');
  process.exit(1);

  // const queueCount = 3;
  // const queueSizeLimit = 100;
  //
  // await startBalancer({
  //   queueCount,
  //   queueSizeLimit,
  //   inputPrefetchCount: queueCount * queueSizeLimit * 2,
  //   outputPrefetchCount: queueCount * queueSizeLimit * 2
  // });
  //
  // for (let i = 0; i < queueCount; i++) {
  //   await startClient({
  //     outputQueueIndex: i + 1,
  //     prefetchCount: queueSizeLimit
  //   });
  // }
  //
  // await startPublisher({
  //   messageCountLimit: 1000,
  //   messageSize: 8 * 1024
  // });
}

main();