module Coins
  class CmcCoinHistoryWriter

    BATCH_SIZE = 5_000
    CONFLICT_KEY_COLUMNS = %i[cmc_id timestamp].freeze

    def initialize(cmc_id:)
      @cmc_id = cmc_id
    end

    def write(data = [])
      attributes = data.map { |point| build_attributes_for(point) }
      attributes.in_groups_of(BATCH_SIZE, false).each { |batch| save(batch) }
    end

    private

    def build_attributes_for(point)
      {
        cmc_id: @cmc_id,

        market_cap_by_available_supply:  point.fetch('market_cap_by_available_supply'),
        price_btc:  point.fetch('price_btc'),
        price_usd:  point.fetch('price_usd'),
        volume_usd: point.fetch('volume_usd'),
        timestamp:  point.fetch('timestamp')
      }
    end

    def save(attributes = [])
      DB[:coin_cmc_history]
        .insert_conflict(target: CONFLICT_KEY_COLUMNS)
        .multi_insert(attributes)
    end
  end
end
