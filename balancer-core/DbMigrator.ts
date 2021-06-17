import {Pool} from './Db'
import {emptyLogger, MessageBalancerLogger} from './MessageBalancer.Logger'
import {setTimeoutAsync} from './utils'

interface DbMigratorParams {
  pool: Pool
  logger?: MessageBalancerLogger
  attemptLimit?: number
  attemptTimeout?: number
}

const defaultAttemptLimit = 20
const defaultAttemptTimeout = 1000

export function migrateDb(params: DbMigratorParams) {
  return new DbMigrator(params).migrate()
}

export function waitForPostgres(params: DbMigratorParams) {
  return new DbMigrator(params).waitForPostgres()
}

class DbMigrator {
  private readonly pool: Pool
  private readonly logger: MessageBalancerLogger
  private readonly attemptLimit: number
  private readonly attemptTimeout: number

  constructor(params: DbMigratorParams) {
    const {pool, logger, attemptLimit, attemptTimeout} = {
      attemptLimit: defaultAttemptLimit,
      attemptTimeout: defaultAttemptTimeout,
      logger: emptyLogger,
      ...params
    }

    this.pool = pool
    this.logger = logger
    this.attemptLimit = attemptLimit
    this.attemptTimeout = attemptTimeout
  }

  public async migrate() {
    try {
      await this.waitForPostgres()

      const query = `
CREATE TABLE IF NOT EXISTS messages (
  message_id SERIAL PRIMARY KEY,
  partition_group TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  content BYTEA NOT NULL,
  properties JSON NULL,
  received_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  create_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS messages_partition_group_partition_key_message_id_idx ON messages (partition_group, partition_key, message_id);
`

      await this.pool.query(query)
    } catch (error) {
      this.logger.log({
        level: 'error',
        message: 'Unable to migrate postgres database',
        eventType: 'db-migrator/migrate/unexpected-error',
        error
      })

      throw error
    }
  }

  public async waitForPostgres(): Promise<void> {
    let attempt = 0

    while (true) {
      try {
        attempt += 1
        await this.ping()
        return
      } catch (error) {
        if (attempt >= this.attemptLimit) {
          this.logger.log({
            level: 'error',
            message: `Postgres is not available. Unable to ping postgres during ${attempt} attempt`,
            eventType: 'db-migrator/wait-for-postgres/postgres-unavailable',
            error
          })

          throw new Error(`Postgres is not available`)
        }

        await setTimeoutAsync(this.attemptTimeout)
      }
    }
  }

  private async ping() {
    await this.pool.query(`SELECT 'pong' AS response`)
  }
}
