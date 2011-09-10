require "eventmachine"
begin
  require "bson_ext"
rescue LoadError
  require "bson"
end

module EM::Mongo

  module Version
    STRING = File.read(File.dirname(__FILE__) + '/../VERSION')
    MAJOR, MINOR, TINY = STRING.split('.')
  end

  NAME    = 'em-mongo'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH    = ::File.dirname(LIBPATH) + ::File::SEPARATOR
end

require File.join(EM::Mongo::LIBPATH, "em-mongo/conversions")
require File.join(EM::Mongo::LIBPATH, "em-mongo/support")
require File.join(EM::Mongo::LIBPATH, "em-mongo/database")
require File.join(EM::Mongo::LIBPATH, "em-mongo/connection")
require File.join(EM::Mongo::LIBPATH, "em-mongo/collection")
require File.join(EM::Mongo::LIBPATH, "em-mongo/exceptions")
require File.join(EM::Mongo::LIBPATH, "em-mongo/cursor")
require File.join(EM::Mongo::LIBPATH, "em-mongo/request_response")
require File.join(EM::Mongo::LIBPATH, "em-mongo/server_response")
require File.join(EM::Mongo::LIBPATH, "em-mongo/core_ext")

EMMongo = EM::Mongo
