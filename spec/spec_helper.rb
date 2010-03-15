require File.dirname(__FILE__) + '/../lib/em-mongo'

#$LOAD_PATH << File.dirname(__FILE__)+'/../../em-spec/lib'
#require File.dirname(__FILE__)+'/../../em-spec/lib/em/spec'
#require 'spec'
#require File.dirname(__FILE__)+'/../../em-spec/lib/em/spec/rspec'
#EM.spec_backend = EventMachine::Spec::Rspec

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
          EM.next_tick do
            yield EMMongo::Connection.new 
          end
        end
      end

    end
  end
end

