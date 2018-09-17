module Brokers
  class Bittrex < BaseBroker
    include CommonHelpers

    EXCHANGE_NAME = 'Bittrex'.freeze

    OPTIONS = {
      book: {
        type: 'both' # buy, sell or both to identify the type of orderbook to return
      }.freeze,

      trade_history: {
        directions: { buy: 'bid'.freeze, sell: 'ask'.freeze }.freeze
      }.freeze
    }.freeze

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

    private

    def exchange_name
      EXCHANGE_NAME
    end

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

        pair = Entities::Public::Pair.new(
          symbol:     aw_symbol,
          exchange:   exchange_name,
          ex_id:      exchange_symbol,
          quote_coin: exchange_symbol_info.fetch(:quote_coin),
          base_coin:  exchange_symbol_info.fetch(:base_coin),
          usd_symbol: build_usd_symbol(exchange_symbol_info.fetch(:base_coin)),

          volume: to_currency(ticker.fetch('Volume')),
          high:   to_currency(ticker.fetch('High')),
          low:    to_currency(ticker.fetch('Low')),
          last:   to_currency(ticker.fetch('Last')),
          bid:    to_currency(ticker.fetch('Bid')),
          ask:    to_currency(ticker.fetch('Ask')),

          actualized_at: fetched_at
        )

        pairs[aw_symbol] = pair
      end
    end
  end
end
