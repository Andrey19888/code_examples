module Coins
  class CcCoinHistorySynchronizer
    CONVERT_TO = 'USD'.freeze

    def initialize(coin_id)
      @coin_id = coin_id
      @symbol = DB[:coins].where(id: coin_id).limit(1).select_map(:symbol).first
    end

    def perform
      recent_timestamp = DB[:coins_history].where(coin_id: @coin_id, data_source: CcHistoryWriter::DATA_SOURCE).max(:timestamp)
      history = CcHistoryFetcher.new.fetch(symbol: @symbol, convert_to: CONVERT_TO, from: recent_timestamp)
      writer = CcHistoryWriter.new(coin_id: @coin_id, converted_to: CONVERT_TO)
      writer.write(history).size
    end
  end
end
