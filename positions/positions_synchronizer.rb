# This class performs synchronization of account's position (balance coins)
# Then returns positions hash from local database (synced data).

module Positions
  class PositionsSynchronizer
    include ClassLoggable

    MODEL = Position
    DEACTIVATE_ACCOUNT_ON_FAILED_CREDENTIALS_CHECKS = Integer(ENV.fetch('DEACTIVATE_ACCOUNT_ON_FAILED_CREDENTIALS_CHECKS'))

    class InvalidPosition < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def initialize(account)
      @account = account
      @exchange = account.exchange
    end

    def perform(bad_credentials_check: false)
      return if @account.deactivated_at

      sync(bad_credentials_check)
    end

    private

    def sync(bad_credentials_check)
      begin
        broker = BrokersInstances.for(@exchange.name)
        positions_data = broker.balance(@account.credentials_hash).fetch(:balance)
        entities = positions_data.values

      rescue StandardError => exception
        log(:error, "Couldn't fetch balance from broker")
        log(:error, exception.message)
        log(:error, exception.backtrace)

        if bad_credentials_check
          @account.increment!(:failed_credentials_checks)
          if @account.failed_credentials_checks >= DEACTIVATE_ACCOUNT_ON_FAILED_CREDENTIALS_CHECKS
            operation = Accounts::Deactivate.new(id: @account.id, reason: Accounts::Deactivate::Reasons::BAD_CREDENTIALS)
            operation.perform
          end
        end
      end

      Accounts::FlushDeactivationState.new(account: @account).perform

      return unless entities

      current_timestamp = Time.current
      attributes = build_attributes(account: @account, entities: entities, timestamp: current_timestamp)

      DB.transaction do
        save_to_actual(account: @account, attributes: attributes)
        save_to_history(attributes)
      end
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

    def save_to_history(attributes = [])
      DB[:positions].returning(:id).multi_insert(attributes)
    end

    def save_to_actual(account:, attributes:)
      DB[:actual_positions].where(exchange_id: account.exchange_id, account_id: account.id).delete
      DB[:actual_positions].multi_insert(attributes)
    end
  end
end
