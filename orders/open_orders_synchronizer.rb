module Orders
  class OpenOrdersSynchronizer
    CONFLICT_KEY_COLUMNS = %i[exchange_id oid].freeze

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

      entities = open_orders_data.fetch(:orders)
      attributes = build_attributes(entities)

      # TODO: We also need to check "status" of #open_orders method completion
      #       If error occurred then we maybe should return all open orders from database, not only synced records...
      return [] if attributes.blank?

      synced_orders_ids = save(attributes)

      schedule_status_check_of_old_open_orders(
        account: @account,
        base_dataset: base_dataset,
        synced_orders_ids: synced_orders_ids
      )

      # TODO: We also need to check "status" of #open_orders method completion.
      #       If error occurred then we maybe should return all open orders from database, not only synced records...
      base_dataset.where(id: synced_orders_ids).order(Sequel.desc(:timestamp)).all
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

    def build_attributes(entities)
      pair_id_symbol_map = build_symbol_pair_id_map(exchange: @account.exchange, entities: entities)
      current_timestamp = Time.current

      entities.map do |entity|
        pair_id = pair_id_symbol_map[entity.symbol]
        next unless pair_id

        params = entity.to_h.merge(
          status: MODEL::Statuses::OPEN,
          account_id: @account.id,
          user_id: @account.user_id,
          exchange_id: @account.exchange_id,
          pair_id: pair_id,
          created_at: current_timestamp,
          updated_at: current_timestamp
        )

        operation = Orders::Build.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidOrder.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def build_symbol_pair_id_map(exchange:, entities:)
      symbols = entities.map(&:symbol).uniq
      exchange.pairs.where(symbol: symbols).pluck(:symbol, :id).to_h
    end

    def save(attributes)

      DB[:orders].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: {
          status: Sequel.case(
            { MODEL::Statuses::PARTIAL => MODEL::Statuses::PARTIAL },
              Sequel[:excluded][:status],
              Sequel[:orders][:status]
          ),
          updated_at: Sequel[:excluded][:updated_at]
        },
        update_where: Sequel[:excluded][:updated_at] > Sequel[:orders][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
    end

    def schedule_status_check_of_old_open_orders(account:, base_dataset:, synced_orders_ids: [])
      old_open_orders_ids = base_dataset.exclude(id: synced_orders_ids).select_map(:id)
      return if old_open_orders_ids.blank?

      Orders::ActualizeOrdersStatusWorker.perform_async(
        account_id: account.id,
        internal_orders_ids: old_open_orders_ids
      )
    end
  end
end
