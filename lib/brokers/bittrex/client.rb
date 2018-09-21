module Brokers
  class Bittrex
    class Client
      ENDPOINTS = {
        v1: 'https://bittrex.com/api/v1/'.freeze,     # Stable version
        v1_1: 'https://bittrex.com/api/v1.1/'.freeze  # Beta version
      }.freeze

      def initialize(version)
        @version = version
        @base_endpoint = ENDPOINTS.fetch(version.to_sym)
      end

      ENDPOINTS.each_key do |version|
        define_singleton_method(version) do
          new(version)
        end

        define_method("#{version}?") do
          @version.to_sym == version
        end
      end

      def request(verb, method_endpoint, options = {})
        url = File.join(@base_endpoint, method_endpoint.to_s)
        response = HTTP.request(verb, url, options)

        if response.status.success?
          response.parse
        else
          raise BaseBroker::Errors::ApiRequestError.new(http_status: response.status, body: response.body)
        end
      end
    end
  end
end
