version = File.read("VERSION").strip

Gem::Specification.new do |s|
  s.name    = 'em-mongo'
  s.version = version

  s.authors = ['bcg']
  s.email   = 'brenden.grace@gmail.com'
  s.date    = "2010-11-30"

  s.description = 'EventMachine driver for MongoDB.'
  s.homepage = 'http://github.com/bcg/em-mongo'
  s.rubyforge_project = 'em-mongo'

  s.files      = Dir['VERSION', 'lib/**/*']
  s.test_files = Dir['spec/integration/**/*']

  s.rdoc_options  = ["--charset=UTF-8"]
  s.require_paths = ["lib"]

  s.rubygems_version = '1.3.6'

  s.summary = 'An EventMachine driver for MongoDB.'

  s.add_dependency 'eventmachine', ['>= 0.12.10'] 
  s.add_dependency 'bson', ['>= 1.1.3'] 
  s.add_dependency 'bson_ext', ['>= 1.1.3'] 

end
