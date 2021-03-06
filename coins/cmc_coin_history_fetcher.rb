module Coins
  class CmcCoinHistoryFetcher
    include ClassLoggable

    BASE_ENDPOINT = 'https://graphs2.coinmarketcap.com/currencies/'.freeze
    REDIRECT_HOPS = 2

    DESIRED_ATTRIBUTES = %w[
      market_cap_by_available_supply
      price_btc
      price_usd
      volume_usd
    ].deep_freeze

    def fetch(cmc_slug:, from: nil)
      points = []

      endpoint = build_endpoint(cmc_slug: cmc_slug, from: from)
      proxy_client = Brokers::ProxyClient.new(redirect_hops: REDIRECT_HOPS)

      begin
        response = proxy_client.request(:get, endpoint) { |proxy_response| valid?(proxy_response) }
        data = response.parse

        # { raw_timestamp => { :price_btc => ..., :price_usd => ... }, ... }
        grouped_data = {}

        DESIRED_ATTRIBUTES.each do |desired_attribute|
          attribute_data = data.fetch(desired_attribute)

          attribute_data.each do |(raw_timestamp, raw_value)|
            grouped_data[raw_timestamp] ||= {}
            grouped_data[raw_timestamp][desired_attribute.to_s] = raw_value
          end
        end

        points = grouped_data.map do |raw_timestamp, timestamp_data|
          timestamp = Time.at(raw_timestamp / 1_000.to_f)
          timestamp_data.merge('timestamp' => timestamp)
        end

      rescue StandardError => exception
        log(:error, "Couldn't get history for '#{cmc_slug}'")
        log(:error, response&.body&.to_s)
        log(:error, exception.message)
        log(:error, exception.backtrace)
      end

      points
    end

    private

    def build_endpoint(cmc_slug:, from:)
      endpoint = File.join(BASE_ENDPOINT, cmc_slug, '/')

      if from
        from = to_cmc_timestamp(from).to_s
        to = to_cmc_timestamp(Time.current.to_i - 5.minutes).to_s
        endpoint = File.join(endpoint, from, to, '/')
      end

      endpoint
    end

    def valid?(response)
      return false unless response
      return unless response.status.success?

      response.parse.is_a?(Hash)

    rescue StandardError
      false
    end

    def to_cmc_timestamp(time)
      time.to_i * 1_000
    end
  end
end
