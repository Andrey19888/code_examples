# This class performs synchronization of account's trades into local database.
# Then returns array of trades' attributes from local database (synced data).

module Trades
  class TradesSynchronizer
    CONFLICT_KEY_COLUMNS = %i[account_id exchange_id oid params_digest].freeze

    MODEL = Trade

    class InvalidTrade < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account:, pairs_ids: [], sync_id: nil)
      @account = account
      @exchange = account.exchange
      @pairs_ids = pairs_ids
      @sync_id = sync_id
      @synchronizer = Synchronization::Synchronizer
                          .new(sync_type: 'trade', account: @account, sync_id: @sync_id)
    end

    def perform
      if @account.deactivated_at
        @synchronizer.sync_failed(I18n.t('accounts.deactivated')) if @sync_id.present?
        return
      end

      params = { exchange: @exchange, account: @account, pairs_ids: @pairs_ids }

      begin
        trades_data = fetch_trades(params)
        entities = trades_data.fetch(:trades)

        attributes = build_attributes(params.slice(:exchange, :account).merge(entities: entities))

        # TODO: for recently synced trades
        #       1) we need to check status of corresponding orders in background.
        #       2) we need to sync related order (using method info), also in background.
        _synced_trades_ids = save(attributes)
      rescue => error
        @synchronizer.sync_failed(error.message, error.backtrace) if @sync_id.present?
      end
    end

    private

    def fetch_trades(exchange:, account:, pairs_ids:)
      broker = BrokersInstances.for(exchange.name)
      params = {}

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
      order_oid_internal_id_map = build_order_oid_internal_id_map(account: account, exchange: exchange, oids: oids)

      current_timestamp = Time.current

      entities.map do |entity|
        pair_id = symbol_pair_id_map[entity.symbol]
        next unless pair_id

        params = entity.to_h.merge(
          account_id: account.id,
          user_id: account.user_id,
          exchange_id: account.exchange_id,
          pair_id: pair_id,
          order_id: order_oid_internal_id_map[entity.oid]
        )

        operation = Trades::Build.new(params)
        result = operation.perform

        if result.success?
          result.data.merge(
            created_at: current_timestamp,
            updated_at: current_timestamp
          )
        else
          @synchronizer.sync_failed(result.errors) if @sync_id.present?
          raise InvalidTrade.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def build_symbol_pair_id_map(exchange:, symbols: [])
      exchange.pairs.where(symbol: symbols.uniq).pluck(:symbol, :id).to_h
    end

    def build_order_oid_internal_id_map(account:, exchange:, oids: [])
      DB[:orders].where(account_id: account.id, exchange_id: exchange.id, oid: oids.uniq).select_hash(:oid, :id)
    end

    def save(attributes = [])
      DB[:trades]
        .insert_conflict(target: CONFLICT_KEY_COLUMNS)
        .returning(:id).multi_insert(attributes)
      @synchronizer.sync_succeed if @sync_id.present?
    end
  end
end
