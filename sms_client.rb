class SMSClient

  ACCOUNT_SID = ENV.fetch('TWILIO_ACCOUNT_SID')
  AUTH_TOKEN = ENV.fetch('TWILIO_AUTH_TOKEN')
  SENDER_PHONE_NUMBER = ENV.fetch('TWILIO_SENDER_PHONE_NUMBER')

  def initialize
    @client = Twilio::REST::Client.new(ACCOUNT_SID, AUTH_TOKEN)
  end

  # Returns nil if mobile phone format is invalid.
  def send_message!(to:, message:)
    begin
      @client.api.account.messages.create(
        from: SENDER_PHONE_NUMBER,
        to: to,
        body: message
      )
    rescue Twilio::REST::RestError => error
      if error.code == 21211
        return nil
      else
        raise error
      end
    end
  end

  # Returns formatted phone number if it's valid and present.
  # Returns nil if format is invalid or number is blank.
  def format_mobile_phone!(mobile_phone)
    mobile_phone = mobile_phone.to_s.gsub(/\s+/, '')
    return if mobile_phone.blank?

    begin
      @client.lookups.phone_numbers(mobile_phone).fetch.phone_number

    rescue Twilio::REST::RestError => error
      if error.code == 20404
        return nil
      else
        raise error
      end
    end
  end
end
