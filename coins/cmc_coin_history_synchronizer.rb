module Coins
  class CmcCoinHistorySynchronizer

    def initialize(coin_id:)
      @coin = Coin.find(coin_id)
      @meta = @coin.coin_meta
    end

    def perform
      return unless @meta

      recent_timestamp = DB[:coin_cmc_history].where(cmc_id: @coin.cmc_id).max(:timestamp)
      history = CmcCoinHistoryFetcher.new.fetch(cmc_slug: @meta.cmc_slug, from: recent_timestamp)

      writer = CmcCoinHistoryWriter.new(cmc_id: @coin.cmc_id)
      writer.write(history)
    end
  end
end
