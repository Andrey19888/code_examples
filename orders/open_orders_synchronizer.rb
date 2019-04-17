# This class performs synchronization of account's open orders into local database.
# Then returns array of orders' attributes from local database (synced data).
#
# If an error occurred during synchronization then all orders from local database will be returned.
#
# If all was ok then ONLY recently synchronized orders will be returned.
# (all other orders with status "open/partial" are considered as "completed/closed" and we need to check their status in background)

module Orders
  class OpenOrdersSynchronizer
    CONFLICT_KEY_COLUMNS    = %i[account_id exchange_id oid].freeze
    CONFLICT_UPDATE_COLUMNS = %i[account_id status filled_qty updated_at].freeze

    MODEL = Order

    class InvalidOrder < StandardError
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
                          .new(sync_type: 'order', account: @account, sync_id: @sync_id)
    end

    def perform
      params = { exchange: @exchange, account: @account, pairs_ids: @pairs_ids }

      base_dataset = build_base_dataset(params)
      if @account.deactivated_at
        @synchronizer.sync_failed(I18n.t('accounts.deactivated')) if @sync_id.present?
        return base_dataset.order(Sequel.desc(:timestamp))
      end

      begin
        open_orders_data = fetch_open_orders(params)
        status = open_orders_data.fetch(:status)

        entities = open_orders_data.fetch(:orders)
        current_timestamp = Time.current
        attributes = build_attributes(params.slice(:exchange, :account).merge(entities: entities, timestamp: current_timestamp))
        synced_orders_ids = save(attributes)
      rescue => error
        @synchronizer.sync_failed(error.message, error.backtrace) if @sync_id.present?
      end

      if status == Brokers::STATUS_OK
        results_dataset = base_dataset.where do
          (id =~ synced_orders_ids) | (updated_at > current_timestamp)
        end
      else
        results_dataset = base_dataset
      end

      check_old_open_orders(
        account: @account,
        base_dataset: base_dataset,
        results_dataset: results_dataset
      )

      results_dataset.order(Sequel.desc(:timestamp))
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

    def build_attributes(exchange:, account:, entities:, timestamp:)
      symbols = entities.map(&:symbol)
      symbol_pair_id_map = build_symbol_pair_id_map(exchange: exchange, symbols: symbols)

      entities.map do |entity|
        pair_id = symbol_pair_id_map[entity.symbol]
        next unless pair_id

        status = entity.partial? ? MODEL::Statuses::PARTIAL : MODEL::Statuses::OPEN

        # During synchronization we don't consider orders received exchange API as created by aw-api,
        # that's why we set flag "aw_order" to "false".
        # However this flag will be set to "true" in other code which creates order using aw-api.
        # Besides, if someone set status to "true" this class will never set the value to false,
        # because this column is not updated by INSERT CONFLICT.
        aw_order = false

        params = entity.to_h.merge(
          status: status,
          account_id: account.id,
          user_id: account.user_id,
          exchange_id: account.exchange_id,
          pair_id: pair_id,
          aw_order: aw_order
        )

        operation = Orders::Build.new(params)
        result = operation.perform

        if result.success?
          result.data.merge(
            created_at: timestamp,
            updated_at: timestamp
          )
        else
          @synchronizer.sync_failed(result.errors) if @sync_id.present?
          raise InvalidOrder.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def build_symbol_pair_id_map(exchange:, symbols: [])
      exchange.pairs.where(symbol: symbols.uniq).pluck(:symbol, :id).to_h
    end

    def save(attributes = [])
      columns_to_update = CONFLICT_UPDATE_COLUMNS.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h

      ids = DB[:orders].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:orders][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
      @synchronizer.sync_succeed if @sync_id.present?
      ids
    end

    def check_old_open_orders(account:, base_dataset:, results_dataset: [])
      target_ids = base_dataset.select(:id).except(results_dataset.select(:id)).select_map(:id)
      return if target_ids.blank?

      ActualizeOrdersStatusWorker.perform_async(
        account_id: account.id,
        internal_orders_ids: target_ids
      )
    end
  end
end
