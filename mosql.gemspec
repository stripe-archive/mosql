# -*- coding: utf-8 -*-
$:.unshift(File.expand_path("lib", File.dirname(__FILE__)))
require 'mosql/version'

Gem::Specification.new do |gem|
  gem.authors       = ["Nelson Elhage"]
  gem.email         = ["nelhage@stripe.com"]
  gem.description   = %q{A library for streaming MongoDB to SQL}
  gem.summary       = %q{MongoDB -> SQL streaming bridge}
  gem.homepage      = "https://github.com/stripe/mosql"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "mosql"
  gem.require_paths = ["lib"]
  gem.version       = MoSQL::VERSION

  %w[sequel pg mongo bson_ext rake log4r json
     ].each { |dep| gem.add_runtime_dependency(dep) }
  gem.add_runtime_dependency "mongoriver", "0.4"

  gem.add_development_dependency "minitest"
  gem.add_development_dependency "mocha"
end
