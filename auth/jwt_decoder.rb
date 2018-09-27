module Auth
  class JwtDecoder
    include JwtOptions

    EXPIRATION_LEEWAY = 5.minutes

    def decode(token)
      payload, _header = JWT.decode token, SECRET, true, { exp_leeway: EXPIRATION_LEEWAY, algorithm: ALGORITHM }
      payload['data'].with_indifferent_access

    rescue JWT::DecodeError
      nil
    end
  end
end
