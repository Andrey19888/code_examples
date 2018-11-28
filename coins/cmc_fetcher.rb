module Coins
  class CmcFetcher
    ENDPOINT = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'.freeze

    DEFAULT_PARAMS = {
      cryptocurrency_type: 'all'.freeze,
      sort: 'market_cap'.freeze,
      sort_dir: 'asc'.freeze,
      limit: 5_000
    }.freeze

    def fetch(*quotes)
      quotes = quotes.flatten

      all_quotes_data = quotes.map { |quote| fetch_data(quote) }
      grouped_data = all_quotes_data.flatten.group_by { |coin| coin.fetch('id') }

      coins = grouped_data.values.map { |quote_coins| merge_coins(quote_coins) }
      coins.sort_by { |coin| coin.fetch('id') }
    end

    private

    def fetch_data(convert)
      start = 1
      data = []

      loop do
        params = DEFAULT_PARAMS.merge(convert: convert, start: start)
        page_data = CmcClient.new.get(ENDPOINT, params)
        break if page_data.blank?

        data.concat(page_data)
        start += DEFAULT_PARAMS.fetch(:limit)
      end

      data
    end

    def merge_coins(coins)
      uniq_ids = coins.map { |coin| coin.fetch('id') }.uniq
      raise "couldn't merge different CMC ids: #{uniq_ids.inspect}" if uniq_ids.size > 1

      sorted_coins = coins.sort_by { |coin| coin.fetch('last_updated') }
      merged = {}

      sorted_coins.each do |coin|
        attributes = coin.except('quote')

        coin.fetch('quote').each do |symbol, symbol_attributes|
          symbol_attributes.each do |attribute, value|
            quoted_attribute_name = "#{attribute}_#{symbol.downcase}"
            attributes[quoted_attribute_name] = value
          end
        end

        merged.merge!(attributes)
      end

      merged
    end
  end
end
