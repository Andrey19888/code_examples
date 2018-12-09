# TODO: it's better to use threads to sync history.
#       but we should be careful because it seems that
#       ALL REQUESTS SHOULD BE SENT THOUGH PROXIES TO AVOID BAN
#       (we need to implement possibility to send request only using proxy on broker gems side)

module Exchanges
  class TradeHistorySynchronizer
    include ClassLoggable

    CONFLICT_KEY_COLUMNS  = %i[exchange_id pair_id trade_no timestamp].freeze

    class InvalidTradeHistory < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(exchange)
      @exchange = exchange
    end

    def perform
      pairs = @exchange.pairs.order(id: :asc).pluck(:symbol, :id)
      return if pairs.blank?

      broker = BrokersInstances.for(@exchange.name)

      total_pairs = pairs.count

      pairs.each.with_index(1) do |(symbol, pair_id), index|
        log(:debug, "[#{@exchange.name}] [#{index} / #{total_pairs}] Synchronizing trades for '#{symbol}'...")

        begin
          raw_trade_history = broker.trade_history(symbol)
          next if raw_trade_history.blank?

          attributes = build_attributes(raw_trade_history, pair_id)
          next if attributes.blank?

          save(attributes)
        rescue => exception
          log(:error, exception.message)
          log(:error, exception.backtrace)

          next
        end
      end
    end

    private

    def build_attributes(raw_trade_history_hash, pair_id)
      raw_trade_history_hash.fetch(:history).map do |raw_trade_history|
        params = raw_trade_history.to_h.merge(exchange_id: @exchange.id, pair_id: pair_id)
        operation = Exchanges::BuildTradeHistory.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidTradeHistory.new(params: params, errors: result.errors)
        end
      end.compact
    end

    def save(attributes)
      DB[:trade_history]
          .insert_conflict(target: CONFLICT_KEY_COLUMNS)
          .multi_insert(attributes)
    end
  end
end
