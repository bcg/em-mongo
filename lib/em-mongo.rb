
require "eventmachine"
begin; require "bson_ext"; rescue LoadError; require "bson"; end

module EM::Mongo

  module Version
    MAJOR = 0
    MINOR = 2
    TINY  = 10
    STRING = [MAJOR, MINOR, TINY].join('.')
  end

  NAME    = 'em-mongo'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH    = ::File.dirname(LIBPATH) + ::File::SEPARATOR
end

require File.join(EM::Mongo::LIBPATH, "em-mongo/connection")
require File.join(EM::Mongo::LIBPATH, "em-mongo/collection")

EMMongo = EM::Mongo
