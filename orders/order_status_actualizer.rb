module Orders
  class OrderStatusActualizer

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

        update(
          id: order.fetch(:id),
          attributes: {
            status: info.detailed_status,
            filled_qty: info.filled_qty,
            executed_price: info.filled_price
          }
        )
      end
    end

    private

    def load_orders(account:, ids:)
      base_dataset
        .where(exchange_id: account.exchange_id, account_id: account.id, id: ids)
        .order(Sequel.desc(:timestamp))
    end

    def update(id:, attributes: {})
      current_timestamp = Time.current

      dataset = base_dataset.where do
        (Sequel[:id] =~ id) & (Sequel[:updated_at] < current_timestamp)
      end

      dataset.update(attributes.merge(updated_at: current_timestamp))
    end

    def base_dataset
      DB[:orders]
    end
  end
end
