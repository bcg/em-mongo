version = File.read("VERSION").strip

Gem::Specification.new do |s|
  s.name    = 'em-mongo'
  s.version = version

  s.authors = ['bcg', 'PlasticLizard']
  s.email   = 'brenden.grace@gmail.com'
  s.date    = "2010-12-01"

  s.description = 'EventMachine driver for MongoDB.'
  s.homepage = 'https://github.com/bcg/em-mongo'
  s.rubyforge_project = 'em-mongo'

  s.files = `git ls-files`.split("\n")
  s.test_files = `git ls-files -- spec/*`.split("\n")

  s.extra_rdoc_files = ["README.rdoc"]
  s.rdoc_options  = ["--charset=UTF-8"]
  s.require_paths = ["lib"]

  s.rubygems_version = '1.3.6'

  s.summary = 'An EventMachine driver for MongoDB.'

  s.add_dependency 'eventmachine', ['>= 0.12.10']
  s.add_dependency  "bson", ["~> 1.9.2"]
end
