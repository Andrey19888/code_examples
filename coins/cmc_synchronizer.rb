module Coins
  class CmcSynchronizer

    # CMC field -> database field
    MAPPING = {
      symbol:                 :symbol,
      name:                   :name,
      total_supply:           :total_supply,
      circulating_supply:     :available_supply,

      price_usd:              :price_usd,
      market_cap_usd:         :market_capital_usd,
      volume_24h_usd:         :volume_24h_usd,
      percent_change_1h_usd:  :change_percent_1h_usd,
      percent_change_24h_usd: :change_percent_24h_usd,
      percent_change_7d_usd:  :change_percent_7d_usd,

      price_btc:              :price_btc,
      market_cap_btc:         :market_capital_btc,
      volume_24h_btc:         :volume_24h_btc,
      percent_change_1h_btc:  :change_percent_1h_btc,
      percent_change_24h_btc: :change_percent_24h_btc,
      percent_change_7d_btc:  :change_percent_7d_btc,

      id:                     :cmc_id,
      cmc_rank:               :cmc_rank,
      last_updated:           :cmc_timestamp
    }.freeze

    CONFLICT_KEY_COLUMNS = %i[cmc_id].freeze

    # [NOTE] we can update only attributes which cmc returns,
    # all other we can't touch to prevent nullify them
    CONFLICT_UPDATE_COLUMNS = (MAPPING.values + %i[updated_at]).freeze

    class InvalidCoin < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def perform
      raw_data = CmcFetcher.new.fetch('USD', 'BTC')
      attributes = build_attributes(raw_data)

      save(attributes)
    end

    private

    def build_attributes(raw_data)
      raw_data.map do |coin|
        params = {}

        MAPPING.each do |cmc_attribute, attribute|
          value = coin.fetch(cmc_attribute.to_s)
          params[attribute] = value
        end

        operation = Coins::CmcBuild.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidCoin.new(params: params, errors: result.errors)
        end
      end
    end

    def save(attributes = [])
      columns_to_update = CONFLICT_UPDATE_COLUMNS.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h

      DB[:coins].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:coins][:updated_at]
      ).multi_insert(
        attributes
      )
    end
  end
end
