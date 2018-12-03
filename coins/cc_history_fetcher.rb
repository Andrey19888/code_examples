module Coins
  class CcHistoryFetcher
    include ClassLoggable

    ENDPOINT = 'https://min-api.cryptocompare.com/data/histohour'.freeze

    DEFAULT_PARAMS = {
      limit: 2000,
      e: 'CCCAGG'.freeze
    }.freeze

    def fetch(symbol:, convert_to:, from: nil)
      filter_from = from&.to_i
      filter_to   = Time.current.to_i

      params = DEFAULT_PARAMS.merge(
        fsym: symbol,
        tsym: convert_to,
        toTs: filter_to
      )

      points = []

      begin
        loop do
          response = HTTP.get(ENDPOINT, params: params)
          data = response.parse

          batch_points = data.fetch('Data')
          break if batch_points.empty?

          _zero_points, good_points = batch_points.partition { |point| zero_point?(point) }
          break if good_points.blank?

          points.concat(good_points)

          batch_from = data.fetch('TimeFrom')
          break if filter_from && batch_from <= filter_from

          params[:toTs] = batch_from
        end

      rescue StandardError => exception
        log(:error, exception.inspect)
      end

      points.delete_if { |point| point.fetch('time') < filter_from } if filter_from
      points.sort_by { |point| point.fetch('time') }.reverse
    end

    private

    def zero_point?(data)
      data.except('time').values.all?(&:zero?)
    end
  end
end
