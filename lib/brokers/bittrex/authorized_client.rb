module Brokers
  class Bittrex
    class AuthorizedClient < Client
      ApiTokensNotSpecified = Class.new(StandardError)

      def auth(key:, secret:)
        @key = key&.to_s
        @secret = secret&.to_s

        self
      end
    end
  end
end
