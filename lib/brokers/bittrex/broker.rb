module Brokers
  class Bittrex < BaseBroker
    include CommonHelpers

    OPTIONS = {
      book: {
        type: 'both' # buy, sell or both to identify the type of orderbook to return
      }.freeze,

      trade_history: {
        directions: { buy: 'bid'.freeze, sell: 'ask'.freeze }.freeze
      }.freeze,

      open_orders: {
        kind: 'order'.freeze,
        order_type: { limit_buy: 'buy'.freeze, limit_sell: 'sell'.freeze }.freeze
      }.freeze,

      trades: {
        kind: 'trade'.freeze,
        trade_type: { limit_buy: 'buy'.freeze, limit_sell: 'sell'.freeze }.freeze
      }.freeze
    }.freeze

    enable_logging!

    # Defined in parent class:
    #
    #  def pairs
    #  end
    #
    #  def pairs_cache
    #  end
    #
    #  def validate_credentials(account)
    #  end
    #
    #  def account_info(account, trades_params = {})
    #  end

    # aw_symbol: String
    def book(aw_symbol)
      exchange_symbol = convert_to_bittrex_symbol(aw_symbol)
      endpoint = "public/getorderbook"
      params = {
        type: OPTIONS.fetch(:book).fetch(:type),
        market: exchange_symbol
      }

      orders = Client.v1_1.request(:get, endpoint, params: params)

      {}.tap do |book|
        book[:symbol] = aw_symbol
        book[:exchange] = exchange_name
        book[:bids] = []
        book[:asks] = []

        orders.fetch('result').fetch('buy').each do |bid|
          entity_values = prepare_book_entity(bid)
          book[:bids] << Entities::Public::Bid.new(entity_values)
        end

        orders.fetch('result').fetch('sell').each do |ask|
          entity_values = prepare_book_entity(ask)
          book[:asks] << Entities::Public::Ask.new(entity_values)
        end
      end
    end


    # aw_symbol: String
    def trade_history(aw_symbol)
      exchange_symbol = convert_to_bittrex_symbol(aw_symbol)
      endpoint = 'public/getmarkethistory'
      options = OPTIONS.fetch(:trade_history)
      params = {
        market: exchange_symbol
      }

      trades = Client.v1_1.request(:get, endpoint, params: params)

      {}.tap do |history|
        history[:symbol] = aw_symbol
        history[:exchange] = exchange_name
        history[:history] = []

        trades.fetch('result').each do |trade|
          trade_id  = trade.fetch('Id')
          price     = to_currency(trade.fetch('Price'))
          amount    = to_currency(trade.fetch('Total'))
          quantity  = to_currency(trade.fetch('Quantity'))
          direction = options.fetch(:directions).fetch(trade.fetch('OrderType').downcase.to_sym)
          timestamp = Time.parse(trade.fetch('TimeStamp'))

          trade = Entities::Public::Trade.new(
            qty: quantity,
            price: price,
            value: amount,
            trade_no: trade_id.to_s,
            timestamp: timestamp,
            direction: direction
          )

          history[:history] << trade
        end
      end
    end

    # account: Hash (:key, :secret)
    def balance(account)
      endpoint = 'account/getbalances'
      data = AuthorizedClient.v1_1.auth(account).request(endpoint)

      balance = data.fetch('result').map do |coin_balance|
        coin = coin_balance.fetch('Currency').upcase

        entity           = Entities::Account::Balance.new(coin: coin)
        entity.available = to_currency(coin_balance.fetch('Available'))
        entity.total     = to_currency(coin_balance.fetch('Balance'))
        entity.qty       = to_currency(coin_balance.fetch('Balance'))
        entity.on_orders = entity.total - entity.available

        [coin, entity]
      end.to_h

      calculate_usd_btc_values!(balance)

      {
          account: build_account(account.fetch(:key)),
          exchange: exchange_name,
          balance: balance
      }

    end

    # account: Hash (:key, :secret)
    # params: Hash
    #   * :pair - optional, an array can be passed
    def open_orders(account, params = {})
      endpoint = 'market/getopenorders'
      data = AuthorizedClient.v1_1.auth(account).request(endpoint)
      options = OPTIONS.fetch(:open_orders)
      kind = options.fetch(:kind)

      orders = data.fetch('result').map do |order|
        exchange_symbol = order.fetch('Exchange')
        aw_symbol = convert_to_aw_symbol(exchange_symbol)

        Entities::Account::OpenOrder.new(
          symbol:    aw_symbol,
          kind:      kind,
          oid:       order.fetch('OrderUuid').to_s,
          timestamp: Time.parse(order.fetch('Opened')),
          op:        options.fetch(:order_type).fetch(order.fetch('OrderType').downcase.to_sym),
          qty:       to_currency(order.fetch('QuantityRemaining')),
          price:     to_currency(order.fetch('Limit'))
        )
      end

      if params.key?(:pair)
        aw_symbols = Array(params[:pair]).to_set
        orders.keep_if { |order| aw_symbols.include?(order.symbol) }
      end

      build_exchange_account(exchange_name: exchange_name, key: account.fetch(:key)).merge(orders: orders)
    end

    # account: Hash (:key, :secret)
    # params: Hash
    #   * :pair - optional, an array can be passed
    def trades(account, params = {})
      endpoint = 'account/getorderhistory'
      options = OPTIONS.fetch(:trades)
      kind = options.fetch(:kind)

      begin
        data = AuthorizedClient.v1_1.auth(account).request(endpoint)

        trades = data.fetch('result').map do |trade|
          exchange_symbol = trade.fetch('Exchange')
          aw_symbol = convert_to_aw_symbol(exchange_symbol)

          Entities::Account::Trade.new(
            symbol:    aw_symbol,
            kind:      kind,
            oid:       trade.fetch('OrderUuid').to_s,
            timestamp: Time.parse(trade.fetch('TimeStamp')),
            op:        options.fetch(:trade_type).fetch(trade.fetch('OrderType').downcase.to_sym),
            qty:       to_currency(trade.fetch('Quantity')),
            price:     to_currency(trade.fetch('Limit'))
          )
        end

      # TODO: log error into Redis
      rescue BaseBroker::Errors::AlgowaveError => exception
        STDERR.puts "[#{exchange_name}] #{exception.message}"
      end

      if params.key?(:pair)
        aw_symbols = Array(params[:pair]).to_set
        trades.keep_if { |trade| aw_symbols.include?(trade.symbol) }
      end

      build_exchange_account(exchange_name: exchange_name, key: account.fetch(:key)).merge(trades: trades)
    end

    # account: Hash (:key, :secret)
    # params: Hash (:oid)
    def order_info(account, params)
      endpoint = 'account/getorder'
      uuid = params.fetch(:oid)

      data = AuthorizedClient.v1_1.auth(account).request(endpoint, { uuid: uuid })
      info = data.fetch('result')

      order_header = build_exchange_account(exchange_name: exchange_name, key: account.fetch(:key)).merge(oid: uuid)

      Entities::Account::OrderInfo.new(order_header).tap do |entity|
        entity.qty          = to_currency(info.fetch('Quantity'))
        entity.price        = to_currency(info.fetch('Limit'))
        entity.filled_qty   = entity.qty - to_currency(info.fetch('QuantityRemaining'))
        entity.filled_price = to_currency(info.fetch('Limit'))
        entity.active       = info.fetch('IsOpen')
      end
    end

    # account: Hash (:key, :secret)
    # params: Hash (:pair, :limit, :qty)
    def buy(account, params)
      endpoint = 'market/buylimit'
      create_order(params.merge(account: account, endpoint: endpoint))
    end

    # account: Hash (:key, :secret)
    # params: Hash (:pair, :limit, :qty)
    def sell(account, params)
      endpoint = 'market/selllimit'
      create_order(params.merge(account: account, endpoint: endpoint))
    end

    # account: Hash (:key, :secret)
    # params: Hash (:oid)
    def cancel(account, params)
      endpoint = 'market/cancel'
      uuid = params.fetch(:oid)

      exchange_account = build_exchange_account(exchange_name: exchange_name, key: account.fetch(:key))
      order_operation = Entities::Account::OrderOperation.new(exchange_account)

      begin
        result = AuthorizedClient.v1_1.auth(account).request(endpoint, { uuid: uuid })
        if result.fetch('success')
          order_operation.oid = uuid.to_s
          order_operation.status = STATUS_OK
        else
          error = Entities::Error.new(
            message: result.fetch('message')
          )
          order_operation.error = error
          order_operation.status = STATUS_ERROR
        end
      rescue BaseBroker::Errors::AlgowaveError => exception
        error = Entities::Error.for(exception)
        order_operation.error = error
        order_operation.status = STATUS_ERROR
      end

      order_operation
    end

    private

    def parse_bittrex_symbol(symbol)
      data = {}

      # regular pair (e.g. vee_btc)
      if symbol && symbol.split('-').count == 2
        coins = symbol.upcase.split('-')
        data[:base_coin]  = coins[0]
        data[:quote_coin] = coins[1]
      else
        raise Errors::AlgowaveError, "couldn't parse symbol #{symbol.inspect}"
      end

      data
    end

    def convert_to_bittrex_symbol(aw_symbol)
      pair = parse_aw_symbol(aw_symbol)
      "#{pair.fetch(:base_coin)}-#{pair.fetch(:quote_coin)}".upcase
    end

    def convert_to_aw_symbol(bittrex_symbol)
      info = parse_bittrex_symbol(bittrex_symbol)

      build_aw_symbol(
        base_coin: info.fetch(:base_coin),
        quote_coin: info.fetch(:quote_coin)
      )
    end

    def prepare_book_entity(order)
      price = to_currency(order.fetch('Rate'))
      qty = to_currency(order.fetch('Quantity'))
      {
        price: price,
        qty: qty,
        value: price * qty
      }
    end

    def fetch_pairs
      endpoint = 'public/getmarketsummaries'
      tickers = Client.v1_1.request(:get, endpoint)

      fetched_at = Time.now.utc

      tickers.fetch('result').each.with_object({}) do |ticker, pairs|
        exchange_symbol = ticker.fetch('MarketName')
        exchange_symbol_info = parse_bittrex_symbol(exchange_symbol)
        aw_symbol = build_aw_symbol(exchange_symbol_info.slice(:base_coin, :quote_coin))
        open = to_currency(ticker.fetch('PrevDay'))
        close = to_currency(ticker.fetch('Last'))
        volume = to_currency(ticker.fetch('Volume'))

        pair = Entities::Public::Pair.new(
          symbol:     aw_symbol,
          exchange:   exchange_name,
          ex_id:      exchange_symbol,
          quote_coin: exchange_symbol_info.fetch(:quote_coin),
          base_coin:  exchange_symbol_info.fetch(:base_coin),

          volume:  volume,
          high:    to_currency(ticker.fetch('High')),
          low:     to_currency(ticker.fetch('Low')),
          last:    close,
          bid:     to_currency(ticker.fetch('Bid')),
          ask:     to_currency(ticker.fetch('Ask')),
          open:    open,
          enabled: volume > 0,

          change_percent: calc_change_percent(open: open, close: close),
          actualized_at: fetched_at
        )

        pairs[aw_symbol] = pair
      end
    end

    def create_order(account:, endpoint:, pair:, limit:, qty:)
      quantity = BigDecimal(qty.to_s).to_s('F')
      rate = BigDecimal(limit.to_s).to_s('F')

      params = {
        market: convert_to_bittrex_symbol(pair),
        rate: rate,
        quantity: quantity
      }

      exchange_account = build_exchange_account(exchange_name: exchange_name, key: account.fetch(:key))
      order_operation = Entities::Account::OrderOperation.new(exchange_account)

      begin
        result = AuthorizedClient.v1_1.auth(account).request(endpoint, params)
        if result.fetch('success')
          order_operation.oid = result.fetch('result').fetch('uuid').to_s
          order_operation.status = STATUS_OK
        else
          error = Entities::Error.new(
            message: result.fetch('message')
          )
          order_operation.error = error
          order_operation.status = STATUS_ERROR
        end

      rescue BaseBroker::Errors::AlgowaveError => exception
        error = Entities::Error.for(exception)
        order_operation.error = error
        order_operation.status = STATUS_ERROR
      end

      order_operation
    end
  end
end
