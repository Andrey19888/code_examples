# This class performs synchronization of account's open orders into local database.
# Then returns array of orders' attributes from local database (synced data).
#
# If an error occurred during synchronization then all orders from local database will be returned.
#
# If all was ok then ONLY recently synchronized orders will be returned.
# (all other orders with status "open/partial" are considered as "completed/closed" and we need to check their status in background)

# TODO: consider using advisory locks

module Orders
  class OpenOrdersSynchronizer
    CONFLICT_KEY_COLUMNS    = %i[exchange_id oid].freeze
    CONFLICT_UPDATE_COLUMNS = %i[status filled_qty updated_at].freeze

    MODEL = Order

    class InvalidOrder < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account:, exchange: nil, pairs_ids: [])
      @account = account
      @exchange = exchange || account.exchange
      @pairs_ids = pairs_ids
    end

    def perform
      params = { exchange: @exchange, account: @account, pairs_ids: @pairs_ids }

      base_dataset = build_base_dataset(params)
      open_orders_data = fetch_open_orders(params)
      status = open_orders_data.fetch(:status)

      entities = open_orders_data.fetch(:orders)
      attributes = build_attributes(params.slice(:exchange, :account).merge(entities: entities))
      synced_orders_ids = save(attributes)

      if status == Brokers::STATUS_OK
        results_dataset = base_dataset.where(id: synced_orders_ids)
      else
        results_dataset = base_dataset
      end

      check_old_open_orders(
        account: @account,
        base_dataset: base_dataset,
        synced_orders_ids: synced_orders_ids
      )

      results_dataset.order(Sequel.desc(:timestamp)).all
    end

    private

    def build_base_dataset(exchange:, account:, pairs_ids:)
      dataset = DB[:orders].where(
        exchange_id: exchange.id,
        account_id: account.id,
        status: MODEL::Statuses::ALL_ACTIVE
      )

      if pairs_ids.present?
        dataset = dataset.where(pair_id: pairs_ids)
      end

      dataset
    end

    def fetch_open_orders(exchange:, account:, pairs_ids:)
      broker = BrokersInstances.for(exchange.name)
      params = {}

      if pairs_ids.present?
        aw_symbols = DB[:pairs].where(exchange_id: exchange.id, id: pairs_ids).select_map(:symbol)
        params[:pair] = aw_symbols
      end

      broker.open_orders(account.credentials_hash, params)
    end

    def build_attributes(exchange:, account:, entities:)
      symbols = entities.map(&:symbol)
      symbol_pair_id_map = build_symbol_pair_id_map(exchange: exchange, symbols: symbols)
      current_timestamp = Time.current

      entities.map do |entity|
        pair_id = symbol_pair_id_map[entity.symbol]
        next unless pair_id

        status = entity.partial? ? MODEL::Statuses::PARTIAL : MODEL::Statuses::OPEN

        params = entity.to_h.merge(
          status: status,
          account_id: account.id,
          user_id: account.user_id,
          exchange_id: account.exchange_id,
          pair_id: pair_id
        )

        operation = Orders::Build.new(params)
        result = operation.perform

        if result.success?
          result.data.merge(
            created_at: current_timestamp,
            updated_at: current_timestamp
          )
        else
          raise InvalidOrder.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def build_symbol_pair_id_map(exchange:, symbols: [])
      exchange.pairs.where(symbol: symbols.uniq).pluck(:symbol, :id).to_h
    end

    def save(attributes = [])
      columns_to_update = CONFLICT_UPDATE_COLUMNS.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h

      DB[:orders].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:orders][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
    end

    def check_old_open_orders(account:, base_dataset:, synced_orders_ids: [])
      return if synced_orders_ids.blank?

      old_open_orders_ids = base_dataset.exclude(id: synced_orders_ids).select_map(:id)
      return if old_open_orders_ids.blank?

      Orders::ActualizeOrdersStatusWorker.perform_async(
        account_id: account.id,
        internal_orders_ids: old_open_orders_ids
      )
    end
  end
end
