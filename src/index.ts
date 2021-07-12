import {Db, migrateDb} from '@targetprocess/balancer-core'
import {assertResources} from './assertResources'
import {amqpUri, outputQueueCount, postgresConnectionString, postgresPoolMax} from './config'
import {Pool} from 'pg'
import {connect} from './amqp/connect'
import Service from './Service'
import {startFakePublisher} from './fake.publisher'
import {startFakeClients} from './fake.client'

process.on('uncaughtException', err => {
  console.error('[CRITICAL] uncaughtException: ' + err)
})

process.on('unhandledRejection', res => {
  console.error('[CRITICAL] unhandledRejection: ' + res)
})

async function main() {
  const fakeConn = await connect(amqpUri)
  const fakeCh = await fakeConn.createChannel()

  // Queues purge should be configurable
  await assertResources(fakeCh, true)

  const pool = new Pool({
    connectionString: postgresConnectionString,
    max: postgresPoolMax
  })
  await migrateDb({pool})
  console.log('migrated DB')

  // DB clean up should be configurable
  await pool.query('delete from messages')
  console.log('cleaned DB')

  let service = new Service()
  await service.start(pool)

  setInterval(async () => {
    const startedAt = Date.now()
    await service.destroy()
    console.log(`destroyed service in ${Date.now() - startedAt} ms`)
    service = new Service()
    await service.start(pool)
  }, 5000)

  await startFakeClients(fakeConn, outputQueueCount)
  console.log('started fake clients')

  const publishedCount = await startFakePublisher(fakeConn)
  console.log(`started fake publisher ${publishedCount}`)

  const db = new Db({pool, useQueryCache: true})

  setInterval(async () => {
    console.log({
      dbMessageCount: (await db.readStats()).messageCount,
      ...service.status()
    })
  }, 3000)
}

main()
