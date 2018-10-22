module Orders
  class OpenOrdersSynchronizer
    CONFLICT_KEY_COLUMNS    = %i[exchange_id oid].freeze
    CONFLICT_UPDATE_COLUMNS = %i[status updated_at].freeze

    MODEL = Order
    SEQUEL_DS_NAME = :orders

    class InvalidOrder < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account:, entities:)
      @account = account
      @entities = entities
    end

    def perform
      attributes = build_attributes(@entities)
      return if attributes.blank?

      current_timestamp = Time.current
      attributes.each do |order|
        order[:created_at] = current_timestamp
        order[:updated_at] = current_timestamp
      end

      save(attributes)
    end

    private

    def build_attributes(entities)
      pair_id_symbol_map = build_symbol_pair_id_map(exchange: @account.exchange, entities: entities)

      entities.map do |entity|
        pair_id = pair_id_symbol_map[entity.symbol]
        next unless pair_id

        params = entity.to_h.merge(
          account_id: @account.id,
          user_id: @account.user_id,
          exchange_id: @account.exchange_id,
          pair_id: pair_id,
          # TODO: don't change "partial" -> "open"
          status: MODEL::Statuses::OPEN
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
      columns_to_update = CONFLICT_UPDATE_COLUMNS

      DB[SEQUEL_DS_NAME].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h,
        update_where: Sequel[:excluded][:updated_at] > Sequel[SEQUEL_DS_NAME][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
    end
  end
end
