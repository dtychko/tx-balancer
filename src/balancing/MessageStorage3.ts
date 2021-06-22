import {Db3, FullOrPartialMessage} from './Db3'

export default class MessageStorage3 {
  private readonly db3: Db3

  constructor(params: {db3: Db3}) {
    this.db3 = params.db3
  }

  public async readPartitionMessagesOrderedById(spec: {
    fromRow: number
    toRow: number
    contentSizeLimit: number
  }): Promise<FullOrPartialMessage[]> {
    // TODO: Add logging/diagnostics/errorHandling
    const {fromRow, toRow, contentSizeLimit} = spec
    return await this.db3.readPartitionMessagesOrderedById(fromRow, toRow, contentSizeLimit)
  }
}
