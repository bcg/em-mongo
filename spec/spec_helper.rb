require "rubygems"
require "bundler"

Bundler.setup(:test)

require File.expand_path('../lib/em-mongo', File.dirname(__FILE__))

require "em-spec/rspec"

module EM
  module Spec
    module Mongo
      extend EM::SpecHelper 

      @@clean_collection_up = nil

      def self.close
        @@clean_collection_up.call if @@clean_collection_up
        done
      end
    
      def self.collection
        self.database do |database|
          database.collection.remove
          yield database.collection
        end
      end

      def self.database
        self.connection do |connection|
          yield connection.db
        end
      end

      def self.connection
        em do
          connection = EMMongo::Connection.new
          EM.next_tick do
            yield connection
          end
        end
      end

    end
  end
end

