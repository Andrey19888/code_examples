module Brokers
  class Bittrex
    class AuthorizedClient < Client
      ApiTokensNotSpecified = Class.new(StandardError)

      def auth(key:, secret:)
        @key = key&.to_s
        @secret = secret&.to_s

        self
      end

      def request(method_endpoint, options = {})
        unless @key && @secret
          raise ApiTokensNotSpecified, 'use method #auth to specify :key and :secret to send authorized requests'
        end

        params = BaseBroker::CommonHelpers.symbolize_keys(options)
        params.merge!(
          apikey: @key,
          nonce: current_nonce
        )

        params_for_sign = "#{@base_endpoint}#{method_endpoint.to_s}?#{Addressable::URI.form_encode(params, true )}"

        request_options = {
          params: params,
          headers: {
            apisign: signature(params_for_sign, secret: @secret)
          }
        }

        super(:post, method_endpoint, request_options)
      end

      private

      def signature(payload, secret:)
        OpenSSL::HMAC.hexdigest('sha512', secret, payload)
      end

      def current_nonce
        Time.now.to_i.to_s
      end
    end
  end
end
