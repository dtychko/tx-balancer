import {Message} from '@targetprocess/balancer-core'
import {Pool} from 'pg'

export type FullOrPartialMessage =
  | ({
      type: 'full'
    } & Message)
  | {
      type: 'partial'
      messageId: number
      partitionGroup: string
      partitionKey: string
    }

export class Db3 {
  private readonly pool: Pool

  constructor(params: {pool: Pool}) {
    this.pool = params.pool
  }

  public async readPartitionMessagesOrderedById(
    fromRow: number,
    toRow: number,
    totalContentSizeLimit: number
  ): Promise<FullOrPartialMessage[]> {
    const [query, values] = totalContentSizeLimit
      ? fullQuery(fromRow, toRow, totalContentSizeLimit)
      : partialQuery(fromRow, toRow)
    const result = await this.pool.query(query, values)
    return result.rows.map(buildFullOrPartialMessage)
  }
}

function partialQuery(fromRow: number, toRow: number) {
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

function fullQuery(fromRow: number, toRow: number, totalContentSizeLimit: number) {
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

function psql(strings: TemplateStringsArray, ...args: unknown[]): [string, unknown[]] {
  const values = []
  let sql = ''

  for (let i = 0; i < strings.length - 1; i++) {
    sql += strings[i] + `$${values.push(args[i])}`
  }

  sql += strings[strings.length - 1]

  return [sql, values]
}
