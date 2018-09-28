module Auth
  class JwtDecoder
    include JwtOptions

    attr_reader :data

    def decode(token, expiration_leeway: 0)

      # Note that in version '1.5.6' of 'jwt' we use option :leeway (not :exp_leeway)
      # :exp_leeway is available in new versions 2.x
      payload, _header = JWT.decode token, SECRET, true, { leeway: expiration_leeway, algorithm: ALGORITHM }
      @data = payload['data'].with_indifferent_access

    rescue JWT::DecodeError
      nil
    end
  end
end
