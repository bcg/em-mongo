require "rubygems"
require "bundler"
Bundler.setup(:default, :development)
require "eventmachine"
begin
  require "bson_ext"
rescue LoadError
  require "bson"
end

require File.expand_path('../lib/em-mongo', File.dirname(__FILE__))

require "em-spec/rspec"

def connection_and_collection(collection_name=EM::Mongo::DEFAULT_NS)
  conn = EMMongo::Connection.new 
  return conn, collection(conn, collection_name)
end

def collection(conn, name)
  conn.db.collection(name).remove
  conn.db.collection(name)
end

def number_hash
  @numbers = {
    1 => 'one',
    2 => 'two',
    3 => 'three',
    4 => 'four',
    5 => 'five',
    6 => 'six',
    7 => 'seven',
    8 => 'eight',
    9 => 'nine'
  }
end
