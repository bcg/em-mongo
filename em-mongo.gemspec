version = File.read("VERSION").strip

Gem::Specification.new do |s|
  s.name    = 'em-mongo'
  s.version = version

  s.authors = ['bcg']
  s.email   = 'brenden.grace@gmail.com'
  s.date    = '2010-06-21'

  s.description = 'EventMachine drive for MongoDB.'

  s.files      = Dir['lib/**/*']
  s.test_files = Dir['spec/integration/**/*']

  s.rdoc_options  = ["--charset=UTF-8"]
  s.require_paths = ["lib"]

  s.rubygems_version = '1.3.6'

  s.summary = 'EventMachine driver for MongoDB.'

  s.add_dependency("eventmachine", ['>= 0.12.10'])
  s.add_dependency("bson", ['>= 0.20.1'])

end
