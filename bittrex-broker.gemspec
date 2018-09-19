
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name     = 'bittrex-broker'
  spec.version  = '0.0.1'
  spec.authors  = ['Andrey Eysmont']
  spec.email    = ['andrey.eysmont@azati.com']

  spec.summary  = 'AlgoWave broker to communicate with Bittrex cryptocurrency exchange'
  spec.homepage = 'https://bitbucket.org/algowave/exchanges'
  spec.license  = 'Nonstandard'

  # to prevent accidental pushes to rubygems.org
  spec.metadata['allowed_push_host'] = 'https://gems.my-company.example'

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.require_paths = ['lib']

  spec.add_dependency 'brokers-core', '~> 0.0'
  spec.add_dependency 'http', '~> 3.0'

  spec.add_development_dependency 'bundler', '~> 1.16'
  spec.add_development_dependency 'rake', '~> 10.0'
end
