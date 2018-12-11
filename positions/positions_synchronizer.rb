# This class performs synchronization of account's position (balance coins)
# Then returns positions hash from local database (synced data).

module Positions
  class PositionsSynchronizer
    include ClassLoggable

    MODEL = Position

    class InvalidPosition < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account:, exchange: nil)
      @account = account
      @exchange = exchange || account.exchange
    end

    def perform
      sync
    end

    private

    def sync
      begin
        broker = BrokersInstances.for(@exchange.name)
        positions_data = broker.balance(@account.credentials_hash).fetch(:balance)
        entities = positions_data.values

      rescue StandardError => exception
        log(:error, "Couldn't fetch balance from broker")
        log(:error, exception.message)
        log(:error, exception.backtrace)
      end

      return unless entities

      current_timestamp = Time.current
      attributes = build_attributes(account: @account, entities: entities, timestamp: current_timestamp)

      save(attributes)
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

    def save(attributes = [])
      DB[:positions].returning(:id).multi_insert(attributes)
    end
  end
end
