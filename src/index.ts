import {Db, migrateDb} from '@targetprocess/balancer-core'
import {assertResources} from './assertResources'
import {amqpUri, outputQueueCount, postgresConnectionString, postgresPoolMax} from './config'
import {Pool} from 'pg'
import {connect} from './amqp/connect'
import Service from './Service'
import {startFakePublisher} from './fake.publisher'
import {startFakeClients} from './fake.client'

const service = new Service({onError: err => console.error(` [Service/onError] ${err}`)})

process.on('uncaughtException', async err => {
  console.error('[CRITICAL] uncaughtException: ' + err)
  await service.destroy()
  process.exit(1)
})

process.on('unhandledRejection', async res => {
  console.error('[CRITICAL] unhandledRejection: ' + res)
  await service.destroy()
  process.exit(1)
})

process.on('SIGTERM', async () => {
  console.log('[SIGTERM]')
  await service.destroy()
  process.exit(0)
})

async function main() {
  const fakeConn = await connect(amqpUri)
  const fakeCh = await fakeConn.createChannel()
  await assertResources(fakeCh, true)
  console.log('purged all queues')

  const pool = new Pool({connectionString: postgresConnectionString, max: 1})
  await migrateDb({pool})
  console.log('migrated DB')

  await pool.query('delete from messages')
  console.log('cleaned DB')

  await service.start()
  console.log('started service')

  setInterval(async () => {
    try {
      const startedAt = Date.now()
      await service.stop()
      await service.start()
      console.log(`restarted service in ${Date.now() - startedAt} ms`)
    } catch (err) {
      console.error(` [Service] Unable to restart service: ${err}`)
    }
  }, 10000)

  await startFakeClients(fakeConn, outputQueueCount)
  console.log('started fake clients')

  const publishedCount = await startFakePublisher(fakeConn)
  console.log(`started fake publisher ${publishedCount}`)

  const db = new Db({pool, useQueryCache: true})

  setInterval(async () => {
    console.log({
      dbMessageCount: (await db.readStats()).messageCount
      // ...service.status()
    })
  }, 3000)
}

main()
