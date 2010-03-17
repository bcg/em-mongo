# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name    = 'em-mongo'
  s.version = '0.1.0'

  s.authors = ['tmm1, bcg']
  s.date    = '2010-03-03'

  s.description = 'em-mongo'

  # s.extra_rdoc_files = [
  #   'README'
  # ]

  s.files      = Dir['lib/**/*']
  s.test_files = Dir['spec/**/*']

  s.rdoc_options  = ["--charset=UTF-8"]
  s.require_paths = ["lib"]

  s.rubygems_version = '1.3.6'

  s.summary = 'em-mongo based on rmongo'

  s.add_dependency(%q<eventmachine>, ['>= 0.12.10'])
end
