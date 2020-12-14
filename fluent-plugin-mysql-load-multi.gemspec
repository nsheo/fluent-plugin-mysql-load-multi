# coding: utf-8
Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mysql-load-multi"
  spec.version       = "0.0.3"
  spec.authors       = ["Tester"]
  spec.email         = ["nsheo@ntels.com"]
  spec.description   = %q{BufferedOutput plugin to mysql import}
  spec.summary       = %q{BufferedOutput plugin to mysql import}
  spec.homepage      = "https://github.com/nsheo/fluent-plugin-mysql-load-multi"
  spec.license       = "Apache-2.0"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake", "~> 13.0.1"
  spec.add_runtime_dependency "fluentd", "~> 1.0"
  spec.add_runtime_dependency "mysql2", "~> 0.5.3"
end
