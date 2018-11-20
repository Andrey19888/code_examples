module Brokers
  class Bittrex
    class Client < BaseClient
      REQUEST_TYPE = 'public'.freeze

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

      private

      def endpoint
        @base_endpoint
      end

      def request_type
        REQUEST_TYPE
      end

      def response_body_valid?(response)
        body = parse_response(response)
        body.fetch('success')
      end

      def raise_body_error(response)
        body = parse_response(response)
        raise BaseBroker::Errors::ApiRequestError.new(body: body.fetch('message'))
      end

      def parse_response(response)
        response.parse
      end

      def extract_data(response)
        parse_response(response).fetch('result')
      end
    end
  end
end
