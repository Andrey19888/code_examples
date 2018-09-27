module Auth
  module JwtOptions
    ALGORITHM = 'HS256'.freeze
    SECRET = ENV.fetch('AUTH_JWT_SECRET')
    TOKEN_LIFETIME = 2.weeks
  end
end
