# This class performs synchronization of account's position (balance coins)
# Then returns positions hash from local database (synced data).

module Positions
  class PositionsSynchronizer
    MODEL = Position

    class InvalidPosition < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account:, exchange: nil, only_positive: false)
      @account = account
      @exchange = exchange || account.exchange
      @only_positive = only_positive
    end

    def perform
      synced_positions_ids = sync_positions_data

      dataset = DB[:positions].where(exchange_id: @exchange.id, account_id: @account.id)
      dataset = dataset.where { (id =~ synced_positions_ids) }
      dataset = dataset.where { total > 0 } if @only_positive

      coins = dataset.order(Sequel.desc(:usd_value, nulls: :last))
      build_result(coins.all)
    end

    def sync_positions_data
      positions_data = fetch_positions_data(exchange: @exchange, account: @account)
      entities = positions_data.values

      current_timestamp = Time.current
      attributes = build_attributes(account: @account, entities: entities, timestamp: current_timestamp)

      save(attributes)
    end

    private

    def fetch_positions_data(exchange:, account:)
      broker = BrokersInstances.for(exchange.name)
      broker.balance(account.credentials_hash).fetch(:balance)
    end

    def build_attributes(account:, entities:, timestamp:)
      entities.map do |entity|
        params = entity.to_h.merge(
          account_id: account.id,
          user_id: account.user_id,
          exchange_id: account.exchange_id
        )

        operation = Positions::Build.new(params)
        result = operation.perform

        if result.success?
          result.data.merge(
            synchronized_at: timestamp
          )
        else
          raise InvalidPosition.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def build_result(coins)
      {
        total_usd: BigDecimal(0),
        total_btc: BigDecimal(0),
        coins: coins
      }.tap do |result|

        coins.each do |coin|
          if (usd = coin.fetch(:usd_value))
            result[:total_usd] += usd
          end

          if (btc = coin.fetch(:btc_value))
            result[:total_btc] += btc
          end
        end
      end
    end

    def save(attributes = [])
      DB[:positions].returning(:id).multi_insert(attributes)
    end
  end
end
