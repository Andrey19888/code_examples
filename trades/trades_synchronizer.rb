# This class performs synchronization of account's trades into local database.
# Then returns array of trades' attributes from local database (synced data).

# TODO: consider using advisory locks

module Trades
  class TradesSynchronizer
    CONFLICT_KEY_COLUMNS = %i[exchange_id oid params_digest].freeze

    MODEL = Trade

    class InvalidTrade < StandardError
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

      trades_data = fetch_trades(params)
      entities = trades_data.fetch(:trades)

      attributes = build_attributes(params.slice(:exchange, :account).merge(entities: entities))

      # TODO: for recently synced trades
      #       1) we need to check status of corresponding orders in background.
      #       2) we need to sync related order (using method info), also in background.
      _synced_trades_ids = save(attributes)

      dataset = DB[:trades].where(exchange_id: @exchange.id, account_id: @account.id)
      dataset = dataset.where(pair_id: @pairs_ids) if @pairs_ids.present?
      dataset.order(Sequel.desc(:timestamp)).all
    end

    private

    def fetch_trades(exchange:, account:, pairs_ids:)
      broker = BrokersInstances.for(exchange.name)
      params = {}

      # TODO: DRY / See OpenOrdersSynchronizer
      if pairs_ids.present?
        aw_symbols = DB[:pairs].where(exchange_id: exchange.id, id: pairs_ids).select_map(:symbol)
        params[:pair] = aw_symbols
      end

      broker.trades(account.credentials_hash, params)
    end

    def build_attributes(exchange:, account:, entities:)
      symbols = entities.map(&:symbol)
      symbol_pair_id_map = build_symbol_pair_id_map(exchange: exchange, symbols: symbols)

      oids = entities.map(&:oid)
      order_oid_internal_id_map = build_order_oid_internal_id_map(exchange: exchange, oids: oids)

      current_timestamp = Time.current

      entities.map do |entity|
        pair_id = symbol_pair_id_map[entity.symbol]
        next unless pair_id

        params = entity.to_h.merge(
          account_id: account.id,
          user_id: account.user_id,
          exchange_id: account.exchange_id,
          pair_id: pair_id,
          order_id: order_oid_internal_id_map[entity.oid],
          created_at: current_timestamp,
          updated_at: current_timestamp
        )

        operation = Trades::Build.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidTrade.new(params: params, errors: result.errors)
        end
      end.compact
    end

    # TODO: DRY / See OpenOrdersSynchronizer
    def build_symbol_pair_id_map(exchange:, symbols: [])
      exchange.pairs.where(symbol: symbols.uniq).pluck(:symbol, :id).to_h
    end

    def build_order_oid_internal_id_map(exchange:, oids: [])
      # TODO: we also need to filter by account_id to increase security,
      # but we need to check if index will be used before add additional condition
      DB[:orders].where(exchange_id: exchange.id, oid: oids.uniq).select_hash(:oid, :id)
    end

    def save(attributes = [])
      DB[:trades]
        .insert_conflict(target: CONFLICT_KEY_COLUMNS)
        .returning(:id).multi_insert(attributes)
    end
  end
end
