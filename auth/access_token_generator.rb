module Auth
  class AccessTokenGenerator
    ACCESS_TOKEN_LIFETIME = Integer(ENV.fetch('AUTH_ACCESS_TOKEN_LIFETIME'))

    def initialize(generated_by:)
      @generated_by = generated_by
    end

    def generate(user_id)
      current_timestamp = Time.current
      refresh_token = SecureRandom.hex(36)

      jwt_generator = Auth::JwtGenerator.new
      jwt_generator.generate(
        data: {
          user_id: user_id,
          refresh_token: refresh_token,
          generated_by: @generated_by,
          generated_at: current_timestamp,
        },

        expires_at: current_timestamp + ACCESS_TOKEN_LIFETIME
      )

      {
        access_token: jwt_generator.token,
        expires_at: jwt_generator.expires_at,
        refresh_token: refresh_token
      }
    end
  end
end
