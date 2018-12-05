module Predictions
  module EndorOptions
    ENDPOINT        = ENV.fetch('ENDOR_PREDICTIONS_ENDPOINT')
    API_KEY         = ENV.fetch('ENDOR_PREDICTIONS_API_KEY')
    PAYMENT_ADDRESS = ENV.fetch('ENDOR_PREDICTIONS_PAYMENT_ADDRESS')
  end
end
