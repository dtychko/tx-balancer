export interface Pool {
  // tslint:disable-next-line:no-any
  query: <TRow = any>(queryText: string, values?: unknown[]) => Promise<{rows: TRow[]; rowCount: number}>
}

export type FullOrPartialMessage<TProp = unknown> =
  | ({
      type: 'full'
    } & Message<TProp>)
  | {
      type: 'partial'
      messageId: number
      partitionGroup: string
      partitionKey: string
    }

export interface Message<TProp = unknown> extends MessageData<TProp> {
  messageId: number
}

export interface MessageData<TProp = unknown> {
  partitionGroup: string
  partitionKey: string
  content: Buffer
  properties?: TProp
  receivedDate: Date
}

interface DbMessage {
  message_id: number
  partition_group: string
  partition_key: string
  content: Buffer
  properties: unknown
  received_date: Date
}

export interface DbStats {
  readonly messageCount: number
  readonly messageSize: number
}

export interface ReadMessagesOrderedByIdSpec {
  partitionGroup: string
  partitionKey: string
  minMessageId: number
  limit: number
}

export default class Db {
  private readonly pool: Pool
  private readonly useQueryCache: boolean

  constructor(params: {pool: Pool; useQueryCache?: boolean}) {
    this.pool = params.pool
    this.useQueryCache = params.useQueryCache || false
  }

  public async readPartitionMessagesOrderedById(
    fromRow: number,
    toRow: number,
    totalContentSizeLimit: number
  ): Promise<FullOrPartialMessage[]> {
    const [query, values] = totalContentSizeLimit ? fullQuery() : partialQuery()
    const result = await this.pool.query(query, values)
    return result.rows.map(buildFullOrPartialMessage)

    function partialQuery() {
      return psql`
SELECT message_id,
       partition_group,
       partition_key,
       True AS is_partial
  FROM (
         SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY partition_group, partition_key
                    ORDER BY message_id
                ) AS row_number
           FROM messages
       ) AS t1
 WHERE row_number >= ${fromRow}
   AND row_number <= ${toRow}
ORDER BY message_id;
`
    }

    function fullQuery() {
      return psql`
SELECT message_id,
       partition_group,
       partition_key,
       CASE WHEN total_content_size < ${totalContentSizeLimit} THEN False ELSE True END AS is_partial,
       CASE WHEN total_content_size < ${totalContentSizeLimit} THEN received_date ELSE NULL END AS received_date,
       CASE WHEN total_content_size < ${totalContentSizeLimit} THEN content ELSE NULL END AS content,
       CASE WHEN total_content_size < ${totalContentSizeLimit} THEN properties ELSE NULL END AS properties
  FROM (
         SELECT *,
                SUM(octet_length(content)) OVER (
                    ORDER BY row_number, message_id
                ) AS total_content_size
           FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (
                             PARTITION BY partition_group, partition_key
                             ORDER BY message_id
                         ) AS row_number
                    FROM messages
                ) AS t1
         WHERE row_number >= ${fromRow}
           AND row_number <= ${toRow}
     ) AS t2
ORDER BY message_id;
`
    }

    function buildFullOrPartialMessage(row: any): FullOrPartialMessage {
      const isPartial = row.is_partial!!

      if (isPartial) {
        return {
          type: 'partial',
          messageId: row.message_id,
          partitionGroup: row.partition_group,
          partitionKey: row.partition_key
        }
      }

      return {
        type: 'full',
        messageId: row.message_id,
        partitionGroup: row.partition_group,
        partitionKey: row.partition_key,
        content: row.content,
        properties: row.properties === null ? undefined : row.properties,
        receivedDate: row.received_date
      }
    }
  }

  public async readAllPartitionMessagesOrderedById(
    partitionGroup: string,
    partitionSize: number
  ): Promise<Map<string, Message[]>> {
    const param = valueCollector()
    const query = `
SELECT message_id,
       partition_group,
       partition_key,
       content,
       properties,
       received_date
  FROM (
           SELECT *,
                  ROW_NUMBER() OVER (
                      PARTITION BY partition_key
                      ORDER BY message_id
                  ) AS row_number
             FROM messages
            WHERE partition_group = ${param(partitionGroup)}
       ) AS t
 WHERE row_number <= ${param(partitionSize)}
`
    const result = await this.pool.query(query, param.values())
    const messages = result.rows.map(buildMessage)

    return messages.reduce((acc, message) => {
      const {partitionKey} = message
      let bucket = acc.get(partitionKey)
      if (bucket === undefined) {
        bucket = []
        acc.set(partitionKey, bucket)
      }

      bucket.push(message)

      return acc
    }, new Map<string, Message[]>())
  }

  public async createMessages(payloads: MessageData[]): Promise<number[]> {
    if (this.useQueryCache) {
      return this.createMessagesWithQueryCache(payloads)
    }

    const param = valueCollector()
    const values = payloads
      .map(payload => {
        const {partitionGroup, partitionKey, content, properties, receivedDate} = payload
        return `(${[
          param(partitionGroup),
          param(partitionKey),
          param(content),
          param(properties),
          param(receivedDate)
        ].join(', ')})`
      })
      .join(', ')

    const query = `
INSERT INTO messages (
  partition_group,
  partition_key,
  content,
  properties,
  received_date
)
VALUES ${values}
RETURNING message_id
`
    const result = await this.pool.query(query, param.values())
    return result.rows.map(row => row.message_id as number)
  }

  private async createMessagesWithQueryCache(payloads: MessageData[]): Promise<number[]> {
    const paramsPerPayload = 5
    const totalParams = payloads.length * paramsPerPayload

    const query = getOrCreateQuery(`${this.createMessages.name}(${payloads.length})`, () => {
      const {params, count} = parameterGenerator()
      const valueParams = Array.from({length: payloads.length})
        .map(() => `(${params(paramsPerPayload).join(', ')})`)
        .join(', ')

      if (count() !== totalParams) {
        throw new Error(`Expected total number of params: ${totalParams}, actual number: ${count()}`)
      }

      return `
INSERT INTO messages (
  partition_group,
  partition_key,
  content,
  properties,
  received_date
)
VALUES ${valueParams}
RETURNING message_id
`
    })

    const values = payloads.reduce((acc, x) => {
      return [...acc, x.partitionGroup, x.partitionKey, x.content, x.properties, x.receivedDate]
    }, [] as unknown[])

    if (values.length !== totalParams) {
      throw new Error(`Expected total number of values: ${totalParams}, actual number: ${values.length}`)
    }

    const result = await this.pool.query(query, values)
    return result.rows.map(row => row.message_id as number)
  }

  public async updateMessageProperties(message: {messageId: number; properties?: unknown}): Promise<void> {
    const {messageId, properties} = message
    const param = valueCollector()
    const query = `
UPDATE messages
   SET properties = ${param(properties)}
 WHERE message_id = ${param(messageId)}
`
    await this.pool.query(query, param.values())
  }

  public async readMessages(messageIds: number[]): Promise<Message[]> {
    const param = valueCollector()
    const query = `
SELECT message_id,
       partition_group,
       partition_key,
       content,
       properties,
       received_date
  FROM messages
 WHERE message_id IN (${messageIds.map(messageId => param(messageId)).join(',')})
`
    const result = await this.pool.query(query, param.values())
    return result.rows.map(buildMessage)
  }

  public async readMessagesOrderedById(specs: ReadMessagesOrderedByIdSpec[]): Promise<Message[][]> {
    if (this.useQueryCache) {
      return this.readMessagesOrderedByIdWithQueryCache(specs)
    }

    const param = valueCollector()
    const query = specs
      .map((spec, index) => {
        const {partitionGroup, partitionKey, minMessageId, limit} = spec
        return `
(SELECT ${index} AS index,
        message_id,
        partition_group,
        partition_key,
        content,
        properties,
        received_date
   FROM messages
  WHERE partition_group = ${param(partitionGroup)}
    AND partition_key = ${param(partitionKey)}
    AND message_id >= ${param(minMessageId)}
  ORDER BY message_id
  LIMIT ${param(limit)})
`
      })
      .join(' UNION ALL ')

    const result = await this.pool.query(query, param.values())
    return result.rows.reduce<Message[][]>(
      (acc, row) => {
        const index = row.index as number
        // eslint-disable-next-line security/detect-object-injection
        acc[index].push(buildMessage(row))
        return acc
      },
      Array.from({length: specs.length}, () => [] as Message[])
    )
  }

  private async readMessagesOrderedByIdWithQueryCache(specs: ReadMessagesOrderedByIdSpec[]): Promise<Message[][]> {
    const totalParams = specs.length * 4

    const query = getOrCreateQuery(`${this.readMessagesOrderedById.name}(${specs.length})`, () => {
      const {param, count} = parameterGenerator()
      const resultQuery = Array.from({length: specs.length})
        .map((_, index) => {
          return `
(SELECT ${index} AS index,
        message_id,
        partition_group,
        partition_key,
        content,
        properties,
        received_date
   FROM messages
  WHERE partition_group = ${param()}
    AND partition_key = ${param()}
    AND message_id >= ${param()}
  ORDER BY message_id
  LIMIT ${param()})
`
        })
        .join(' UNION ALL ')

      if (count() !== totalParams) {
        throw new Error(`Expected total number of params: ${totalParams}, actual number: ${count()}`)
      }

      return resultQuery
    })

    const values = specs.reduce((acc, x) => {
      return [...acc, x.partitionGroup, x.partitionKey, x.minMessageId, x.limit]
    }, [] as unknown[])

    if (values.length !== totalParams) {
      throw new Error(`Expected total number of values: ${totalParams}, actual number: ${values.length}`)
    }

    const result = await this.pool.query(query, values)
    return result.rows.reduce<Message[][]>(
      (acc, row) => {
        const index = row.index as number
        // eslint-disable-next-line security/detect-object-injection
        acc[index].push(buildMessage(row))
        return acc
      },
      Array.from({length: specs.length}, () => [] as Message[])
    )
  }

  public async removeMessages(messageIds: number[]): Promise<void> {
    if (this.useQueryCache) {
      return this.removeMessagesWithQueryCache(messageIds)
    }

    const param = valueCollector()
    const query = `
DELETE FROM messages
      WHERE message_id IN (${messageIds.map(id => param(id)).join(', ')})
  RETURNING message_id
`
    await this.pool.query(query, param.values())
  }

  private async removeMessagesWithQueryCache(messageIds: number[]): Promise<void> {
    const query = getOrCreateQuery(`${this.removeMessages.name}(${messageIds.length})`, () => {
      const {params} = parameterGenerator()
      return `
DELETE FROM messages
      WHERE message_id IN (${params(messageIds.length).join(', ')})
  RETURNING message_id
`
    })

    await this.pool.query(query, messageIds)
  }

  public async readStats(): Promise<DbStats> {
    const query = `
SELECT COUNT(message_id) AS message_count,
       SUM(octet_length(content)) as message_size
  FROM messages
`
    const result = await this.pool.query(query)
    return {
      messageCount: Number(result.rows[0].message_count),
      messageSize: Number(result.rows[0].message_size)
    }
  }

  public async readPartitionStats(partitionGroup: string, partitionKey: string): Promise<DbStats> {
    const param = valueCollector()
    const query = `
SELECT COUNT(message_id) AS message_count,
       SUM(octet_length(content)) as message_size
  FROM messages
 WHERE partition_group = ${param(partitionGroup)}
   AND partition_key = ${param(partitionKey)}
`
    const result = await this.pool.query(query, param.values())
    return {
      messageCount: Number(result.rows[0].message_count),
      messageSize: Number(result.rows[0].message_size)
    }
  }

  public async readStatsByPartition(): Promise<Map<string, Map<string, DbStats>>> {
    const query = `
SELECT partition_group,
       partition_key,
       COUNT(message_id) AS message_count,
       SUM(octet_length(content)) as message_size
  FROM messages
 GROUP BY partition_group,
          partition_key
`
    const result = await this.pool.query(query)

    return result.rows.reduce((acc, row) => {
      const partitionGroup = row.partition_group as number
      const partitionKey = row.partition_key as number

      let partitionMap = acc.get(partitionGroup)
      if (partitionMap === undefined) {
        partitionMap = new Map<string, DbStats>()
        acc.set(partitionGroup, partitionMap)
      }

      partitionMap.set(partitionKey, {
        messageCount: Number(row.message_count),
        messageSize: Number(row.message_size)
      })

      return acc
    }, new Map<string, Map<string, DbStats>>())
  }
}

const queryCache = new Map<string, string>()
function getOrCreateQuery(key: string, create: () => string) {
  let cachedQuery = queryCache.get(key)
  if (cachedQuery === undefined) {
    cachedQuery = create()
    queryCache.set(key, cachedQuery)
  }
  return cachedQuery
}

// tslint:disable-next-line:no-any
function buildMessage(row: DbMessage): Message {
  return {
    messageId: row.message_id,
    partitionGroup: row.partition_group,
    partitionKey: row.partition_key,
    content: row.content,
    properties: row.properties === null ? undefined : row.properties,
    receivedDate: row.received_date
  }
}

function valueCollector() {
  const values = [] as unknown[]
  const collector = (value: unknown) => `$${values.push(value)}`
  collector.values = () => values

  return collector
}

function parameterGenerator() {
  let nextIndex = 1
  const param = () => `$${nextIndex++}`
  const params = (n: number) => Array.from({length: n}).map(() => param())
  const count = () => nextIndex - 1
  return {
    param,
    params,
    count
  }
}

function psql(strings: TemplateStringsArray, ...args: unknown[]): [string, unknown[]] {
  const values = []
  let sql = ''

  for (let i = 0; i < strings.length - 1; i++) {
    sql += strings[i] + `$${values.push(args[i])}`
  }

  sql += strings[strings.length - 1]

  return [sql, values]
}
