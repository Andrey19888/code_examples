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

    def initialize(account:, sync_id: nil)
      @account = account
      @exchange = account.exchange
      @sync_id = sync_id
      @synchronizer = Synchronization::Synchronizer
          .new(sync_type: 'positions', account: @account, sync_id: @sync_id)
    end

    def perform(bad_credentials_check: false, calculate_usd_btc: false)
      if @account.deactivated_at
        @synchronizer.sync_failed(I18n.t('accounts.deactivated')) if @sync_id.present?
        return
      end

      sync(bad_credentials_check, calculate_usd_btc)
    end

    private

    def sync(bad_credentials_check, calculate_usd_btc)
      entities = nil

      begin
        broker = BrokersInstances.for(@exchange.name)
        positions_data = broker.balance(@account.credentials_hash, calculate_usd_btc: calculate_usd_btc).fetch(:balance)
        entities = positions_data.values

        Accounts::FlushDeactivationState.new(account: @account).perform
      rescue StandardError => exception
        log(:error, "Couldn't fetch balance from broker")
        log(:error, exception.message)
        log(:error, exception.backtrace)

        @synchronizer.sync_failed(exception.message, exception.backtrace) if @sync_id.present?

        if bad_credentials_check
          @account.increment!(:failed_credentials_checks)
          if @account.failed_credentials_checks >= DEACTIVATE_ACCOUNT_ON_FAILED_CREDENTIALS_CHECKS
            operation = Accounts::Deactivate.new(id: @account.id, reason: Accounts::Deactivate::Reasons::BAD_CREDENTIALS)
            operation.perform
          end
        end
      end

      return unless entities

      current_timestamp = Time.current
      attributes = build_attributes(account: @account, entities: entities, timestamp: current_timestamp)

      DB.transaction do
        save_to_actual(account: @account, attributes: attributes)
        save_to_history(attributes)
      end

      @synchronizer.sync_succeed if @sync_id.present?
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
          @synchronizer.sync_failed(result.errors) if @sync_id.present?
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
