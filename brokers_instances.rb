class BrokersInstances

  def self.instances
    @@instances ||= {}
  end

  def self.for(exchange_name)
    instances[exchange_name.to_sym] ||= Brokers::Factory.build(exchange_name)
  end
end
