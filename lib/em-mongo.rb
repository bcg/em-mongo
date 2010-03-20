require 'eventmachine'
require 'pp'

begin
  require 'securerandom'
rescue LoadError
  require 'uuid'
end

module EM::Mongo
  VERSION = '0.0.1'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH = ::File.dirname(LIBPATH) + ::File::SEPARATOR

  class Util
    def self.unique_id
      if defined? SecureRandom
        SecureRandom.hex(12)
      else
        UUID.new.generate(:compact).gsub(/^(.{20})(.{8})(.{4})$/){ $1+$3 }
      end 
    end
  end
end

require File.join(EM::Mongo::LIBPATH, "em-mongo/connection")
require File.join(EM::Mongo::LIBPATH, "em-mongo/buffer")
require File.join(EM::Mongo::LIBPATH, "em-mongo/collection")

EMMongo = EM::Mongo
