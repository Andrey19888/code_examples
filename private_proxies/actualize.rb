require 'open3'

module PrivateProxies
  class Actualize
    ActualizePrivateProxiesFailed = Class.new(StandardError)

    def initialize
      @proxy_port = ENV.fetch('PRIVATE_PROXIES_PORT')
      @proxy_username = ENV.fetch('PRIVATE_PROXIES_USERNAME')
      @proxy_password = ENV.fetch('PRIVATE_PROXIES_PASSWORD')
    end

    def perform
      raw_private_proxies = fetch_private_proxies
      private_proxies = build_proxies(raw_private_proxies)

      Brokers::PrivateProxyManager.actualize_proxies!(private_proxies)
    end

    private

    def fetch_private_proxies
      private_proxy_get_instances_script_path = ENV.fetch('PRIVATE_PROXY_GET_INSTANCES_SCRIPT_PATH').to_s
      private_proxy_tag = ENV.fetch('PRIVATE_PROXY_SERVER_TAG').to_s

      if private_proxy_get_instances_script_path.empty?
        raise ActualizePrivateProxiesFailed, 'The script to get the list of private proxies is not configured.'
      end

      stdout, stderr, status = Open3.capture3(private_proxy_get_instances_script_path, private_proxy_tag)
      error = stderr.to_s.strip

      if error.empty?
        stdout.to_s.split("\n")
      else
        raise ActualizePrivateProxiesFailed, "Error: #{error}"
      end
    end

    def build_proxies(raw_proxies)
      return [] unless raw_proxies

      access_params = "#{@proxy_port}:#{@proxy_username}:#{@proxy_password}"
      raw_proxies.map { |proxy| "#{proxy}:#{access_params}" }
    end
  end
end
