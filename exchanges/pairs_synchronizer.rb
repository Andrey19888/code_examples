module Exchanges
  class PairsSynchronizer
    CONFLICT_KEY_COLUMNS  = %i[exchange_id symbol].freeze
    CONFLICT_SKIP_COLUMNS = (CONFLICT_KEY_COLUMNS + %i[quote_coin base_coin created_at]).freeze

    class InvalidPair < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(exchange)
      @exchange = exchange
    end

    def perform
      broker = BrokersInstances.for(@exchange.name)
      raw_pairs = broker.pairs.values

      attributes = build_attributes(raw_pairs)
      return if attributes.blank?

      mark_outdated_pairs(attributes)
      save(attributes)
    end

    private

    def build_attributes(raw_pairs_hash)
      raw_pairs_hash.map do |raw_pair|
        params = raw_pair.to_h.merge(exchange_id: @exchange.id)
        operation = Exchanges::BuildPair.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidPair.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def save(attributes)
      columns_to_update = attributes.first.keys.map(&:to_sym) - CONFLICT_SKIP_COLUMNS

      DB[:pairs].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:pairs][:updated_at]
      ).returning(:id).multi_insert(
        attributes
      )
    end

    def mark_outdated_pairs(attributes)
      actual_symbols = attributes.map{ |attribute| attribute.fetch('symbol') }
      return if actual_symbols.empty?

      DB[:pairs].where(exchange_id: @exchange.id, outdated: false).exclude(symbol: actual_symbols).update(outdated: true)
    end
  end
end
