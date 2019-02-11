module Coins
  class CmcMarketCapHistorySynchronizer

    CMC_PRO_API_PLAN_HISTORY_LIMIT = ENV.fetch('CMC_PRO_API_PLAN_HISTORY_LIMIT').freeze

    class InvalidMarketCapHistory < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def perform
      from = last_history_point + 1.minute
      raw_data = CmcMarketCapHistoryFetcher.new.fetch(from: from)
      attributes = build_attributes(raw_data)

      save(attributes)
    end

    private

    def last_history_point
      history_point = DB[:market_cap_history].order(Sequel.desc(:timestamp)).first
      history_point&.fetch(:timestamp) || Time.current.utc - days_left
    end

    def days_left
      CMC_PRO_API_PLAN_HISTORY_LIMIT.to_i.days
    end

    def build_attributes(raw_data)
      raw_data.map do |history_point|
        params = {
          total_market_cap: history_point.fetch('market_cap_by_available_supply'),
          total_volume_24h: history_point.fetch('volume_usd'),
          timestamp:        history_point.fetch('timestamp')
        }

        operation = Coins::BuildMarketCapHistory.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidMarketCapHistory.new(params: params, errors: result.errors)
        end
      end
    end

    def save(attributes = [])
      DB[:market_cap_history].insert_conflict(target: :timestamp).multi_insert(attributes)
    end
  end
end
