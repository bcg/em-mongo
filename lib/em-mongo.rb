
require "eventmachine"
require "uuid"
begin; require "bson_ext"; rescue LoadError; require "bson"; end


module EM::Mongo

  module Version
    MAJOR = 0
    MINOR = 2
    TINY  = 1
    STRING = [MAJOR, MINOR, TINY].join('.')
  end

  NAME    = 'em-mongo'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH    = ::File.dirname(LIBPATH) + ::File::SEPARATOR

  class Util
    def self.unique_id
      UUID.new.generate(:compact).gsub(/^(.{20})(.{8})(.{4})$/){ $1+$3 }
    end
  end
end

require File.join(EM::Mongo::LIBPATH, "em-mongo/connection")
require File.join(EM::Mongo::LIBPATH, "em-mongo/collection")

EMMongo = EM::Mongo
