#!/usr/bin/env gem build

require "base64"
require File.expand_path("lib/em-mongo", File.dirname(__FILE__))

Gem::Specification.new do |s|
  s.name    = EM::Mongo::NAME
  s.version = EM::Mongo::Version::STRING

  s.authors = ['bcg']
  s.email   = Base64.decode64("YnJlbmRlbi5ncmFjZUBnbWFpbC5jb20=\n")
  s.date    = '2010-03-03'

  s.description = 'EventMachine drive for MongoDB.'

  s.files      = Dir['lib/**/*']
  s.test_files = Dir['spec/**/*']

  s.rdoc_options  = ["--charset=UTF-8"]
  s.require_paths = ["lib"]

  s.rubygems_version = '1.3.6'

  s.summary = 'EventMachine drive for MongoDB.'

  s.add_dependency("eventmachine", ['>= 0.12.10'])
  s.add_dependency("bson", ['>= 0.20.1'])
  s.add_dependency("uuid", ['>= 2.3.0'])

end
