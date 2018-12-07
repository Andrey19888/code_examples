module Coins
  class CmcMarketCapHistoryFetcher
    ENDPOINT = 'https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/historical'.freeze

    DEFAULT_PARAMS = {
      interval: '5m'.freeze
    }.freeze

    def fetch(from:)
      time_end = Time.current.utc
      params = DEFAULT_PARAMS.merge(time_start: from, time_end: time_end)

      CmcClient.new.get(ENDPOINT, params)
    end
  end
end
