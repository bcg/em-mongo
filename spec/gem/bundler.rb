#!/usr/bin/env bundle exec ruby

require "rubygems"
require "bundler"
Bundler.setup(:default)
require "eventmachine"
require "em-mongo"

$return = -1

EM.run do
  @conn = EM::Mongo::Connection.new
  EM.next_tick do
    id = @conn.db.collection('test').insert({:hello => "world"})
    @conn.db.collection('test').first(:_id => id) do |document|
      $return = 0 if document
      EM.stop
    end
  end
end
exit($return)
