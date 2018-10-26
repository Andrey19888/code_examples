# TODO: rewrite this class to actualize all updateable attributes (status, filled_qty, ...)

module Orders
  class OrdersStatusActualizer
    SKIP_PERIOD = 1.minute

    def initialize(account_id:, internal_orders_ids:)
      @account_id = account_id
      @ids = internal_orders_ids
    end

    def perform
      account = Account.find(@account_id)

      broker = BrokersInstances.for(account.exchange.name)
      orders = load_orders(account: account, ids: @ids)

      orders.each do |order|
        params = {
          oid: order.fetch(:oid),
          pair: order.fetch(:symbol),
          op: order.fetch(:op)
        }

        info = broker.order_info(account.credentials_hash, params)
        update(id: order.fetch(:id), status: info.detailed_status)
      end
    end

    private

    def load_orders(account:, ids:)
      base_dataset
        .where(exchange_id: account.exchange_id, account_id: account.id, id: ids)
        .where { updated_at < Time.current - SKIP_PERIOD }
        .order(Sequel.desc(:timestamp))
    end

    def update(id:, status:)
      base_dataset.where(id: id).update(status: status, updated_at: Time.current)
    end

    def base_dataset
      DB[:orders]
    end
  end
end
