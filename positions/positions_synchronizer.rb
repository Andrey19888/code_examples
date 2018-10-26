# This class performs synchronization of account's position (balance coins)
# Then returns positions hash from local database (synced data).

# TODO: catch error while fetching balance from exchange's API
# TODO: consider using advisory locks

module Positions
  class PositionsSynchronizer
    CONFLICT_KEY_COLUMNS    = %i[exchange_id account_id coin].freeze
    CONFLICT_UPDATE_COLUMNS = %i[total available on_orders usd_value btc_value updated_at].freeze

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
      positions_data = fetch_positions_data(exchange: @exchange, account: @account)
      entities = positions_data.values

      current_timestamp = Time.current
      attributes = build_attributes(account: @account, entities: entities, timestamp: current_timestamp)

      synced_positions_ids = save(attributes)

      dataset = DB[:positions].where(exchange_id: @exchange.id, account_id: @account.id)
      dataset = dataset.where { (id =~ synced_positions_ids) | (updated_at > current_timestamp) }
      dataset = dataset.where { total > 0 } if @only_positive

      coins = dataset.order(Sequel.desc(:usd_value, nulls: :last))
      build_result(coins.all)
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
          exchange_id: account.exchange_id,
          created_at: timestamp,
          updated_at: timestamp
        )

        operation = Positions::Build.new(params)
        result = operation.perform

        if result.success?
          result.data
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
      columns_to_update = CONFLICT_UPDATE_COLUMNS.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h

      DB[:positions].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:positions][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
    end
  end
end
