module Auth
  class JwtGenerator
    include JwtOptions

    attr_reader :token
    attr_reader :expires_at

    def generate(data, expires_at: Time.current + TOKEN_LIFETIME)
      payload = {
        data: data,
        exp: expires_at.to_i
      }

      @expires_at = expires_at
      @token = JWT.encode(payload, SECRET, ALGORITHM)
    end
  end
end
