class CmcClient
  RequestFailed = Class.new(StandardError)

  CMC_PRO_API_HEADER = 'X-CMC_PRO_API_KEY'.freeze
  CMC_PRO_API_KEY = ENV.fetch('CMC_PRO_API_KEY').freeze

  def get(endpoint, params = {})
    response = HTTParty.get(endpoint, query: params, headers: auth_headers)

    if response.client_error? || response.server_error?
      raise RequestFailed, "endpoint: #{endpoint.inspect}; params: #{params.inspect}; response: #{response.inspect}"
    else
      response.parsed_response.fetch('data')
    end
  end

  private

  def auth_headers
    { CMC_PRO_API_HEADER => CMC_PRO_API_KEY }
  end
end
