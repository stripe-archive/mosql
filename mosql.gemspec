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

  gem.add_runtime_dependency "sequel"
  gem.add_runtime_dependency "pg"
  gem.add_runtime_dependency "rake"
  gem.add_runtime_dependency "log4r"
  gem.add_runtime_dependency "json"

  gem.add_runtime_dependency "mongoriver", "0.5"

  gem.add_runtime_dependency "mongo", "~> 2.0"
  gem.add_runtime_dependency "bson", "~> 4.0"
  gem.add_runtime_dependency "bson_ext"

  gem.add_development_dependency "minitest"
  gem.add_development_dependency "mocha"
end
