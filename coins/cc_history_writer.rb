module Coins
  class CcHistoryWriter

    DATA_SOURCE = 'CC'.freeze
    BATCH_SIZE = 5_000
    CONFLICT_KEY_COLUMNS = %i[coin_id timestamp data_source].freeze

    def initialize(coin_id:, converted_to:)
      @coin_id = coin_id
      @converted_to = converted_to
    end

    def write(data = [])
      attributes = data.map { |point| build_attributes_for(point) }
      attributes.in_groups_of(BATCH_SIZE, false).each { |batch| save(batch) }
    end

    private

    def build_attributes_for(point)
      {
        coin_id: @coin_id,

        high:  point.fetch('high'),
        low:   point.fetch('low'),
        open:  point.fetch('open'),
        close: point.fetch('close'),

        volume_from: point.fetch('volumefrom'),
        volume_to:   point.fetch('volumeto'),

        converted_to: @converted_to,
        data_source: DATA_SOURCE,
        timestamp: Time.at(point.fetch('time'))
      }
    end

    def save(attributes = [])
      DB[:coins_history]
        .insert_conflict(target: CONFLICT_KEY_COLUMNS)
        .multi_insert(attributes)
    end
  end
end
