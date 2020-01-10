require 'open3'

module PrivateProxies
  class Terminate
    include ClassLoggable

    TerminateProxiesFailed = Class.new(StandardError)

    def perform
      terminate_blocked_instance_script_path = ENV.fetch('PRIVATE_PROXY_TERMINATE_BLOCKED_INSTANCE_SCRIPT_PATH')

      if terminate_blocked_instance_script_path.empty?
        raise TerminateProxiesFailed, 'The script to terminate a private proxies is not configured.'
      end

      blocked_proxies = Brokers::PrivateProxyManager.blocked_proxies
      return if blocked_proxies.empty?

      blocked_proxies.each do |proxy|
        proxy_address = extract_ip(proxy)
        stdout, stderr = Open3.capture3(terminate_blocked_instance_script_path, proxy_address)
        error = stderr.to_s.strip

        if error.empty?
          log(:info, "Termination work of blocked proxy successful. Result: #{stdout}")
          next
        end

        log(:error, "Error termination work of blocked proxy. Error: #{error}")
      end
    end

    private

    def extract_ip(proxy)
      Brokers::PrivateProxyManager.build_proxy_params(proxy).fetch(:proxy_address)
    end
  end
end
