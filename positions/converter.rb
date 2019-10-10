require 'rgl/adjacency'
require 'rgl/dijkstra'

module Positions
  class Converter
    include ClassLoggable
    include Brokers::Helpers

    def initialize(pairs_hash)
      @pairs = pairs_hash
      @symbols = pairs_hash.keys
    end

    def calculate_usd_btc_values!(coins)
      return coins if coins.empty?

      coins.each do |coin_info|
        coin_info[:usd_value] = to_usd(qty: coin_info[:total], from: coin_info[:coin])
        coin_info[:btc_value] = to_btc(qty: coin_info[:total], from: coin_info[:coin])
      end
    end

    def to_usd(qty:, from:)
      %w[USD USDT].each do |usd_variant|
        converted = direct_convert(qty: qty, from: from, to: usd_variant)
        return converted if converted

        converted = chain_convert(qty: qty, from: from, to: usd_variant)
        return converted if converted
      end

      nil
    end

    def to_btc(qty:, from:)
      converted = direct_convert(qty: qty, from: from, to: 'BTC')
      return converted if converted

      chain_convert(qty: qty, from: from, to: 'BTC')
    end

    def graph
      @graph ||= RGL::AdjacencyGraph.new.tap do |graph|
        @pairs.each_value do |pair|
          graph.add_edge(pair[:quote_coin], pair[:base_coin])
        end
      end
    end

    def weights_map
      @weights_map ||= {}.tap do |map|
        @pairs.each_value do |pair|
          weight_key = [pair[:quote_coin], pair[:base_coin]]
          map[weight_key] = 1
        end
      end
    end

    private

    # returns nil if there is no chain which can be used to convert
    def chain_convert(qty:, from:, to:)
      chain = graph.dijkstra_shortest_path(weights_map, from, to)
      return unless chain

      current_qty = qty

      chain.each_cons(2) do |current_from, current_to|
        new_current_qty = direct_convert(qty: current_qty, from: current_from, to: current_to)
        current_qty = new_current_qty
      end

      current_qty

    rescue StandardError => exception
      log(:error, "Couldn't convert from #{from} to #{to} using chain")
      log(:error, "Exception message: #{exception.message}" )

      nil
    end

    # returns nil if there is no pair to converter
    def direct_convert(qty:, from:, to:)
      qty = to_currency(qty)
      from = from.upcase
      to = to.upcase

      return qty if from == to
      return to_currency(0) if qty == 0

      symbol = build_aw_symbol(quote_coin: from, base_coin: to)
      pair = @pairs[symbol]

      if pair
        qty * pair[:last]

      else
        reversed_symbol = build_aw_symbol(quote_coin: to, base_coin: from)
        reversed_pair = @pairs[reversed_symbol]

        qty / reversed_pair[:last] if reversed_pair
      end
    end
  end
end
