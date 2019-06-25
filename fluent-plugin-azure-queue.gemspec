# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-azure-queue"
  gem.description = "Fluent input plugin for azure queue input"
  gem.license     = "MIT"
  gem.homepage    = "https://github.com/sbonebrake/fluent-plugin-azure-queue"
  gem.summary     = gem.description
  gem.version     = File.read("VERSION").strip
  gem.authors     = ["Scott Bonebrake"]
  gem.email       = "N/A"
  gem.has_rdoc    = false
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.require_paths = ['lib']

  gem.add_dependency "fluentd", [">= 1.5.0", "< 1.6.0"]
  gem.add_dependency "azure-storage-blob", [">= 1.1.0", "< 1.2.0"]
  gem.add_dependency "azure-storage-queue", [">= 1.1.0", "< 1.2.0"]
  gem.add_dependency "azure-storage-common", [">= 1.1.0", "< 1.2.0"]
  gem.add_dependency "nokogiri", [">=1.6.8", "< 1.11.0"]
  gem.add_dependency "concurrent-ruby", ">=1.1.4"
  gem.add_dependency "avro", ">= 1.8"
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "test-unit", ">= 3.0.8"
  gem.add_development_dependency "flexmock", ">= 1.3.3"
end
