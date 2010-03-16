require File.dirname(__FILE__) + '/spec_helper.rb'

describe EMMongo::Connection do
  include EM::SpecHelper

  it 'should connect' do
    em do
      connection = EMMongo::Connection.new
      EM.next_tick do
        connection.connected?.should == true
        done
      end
    end    
  end
  
  it 'should close' do
    em do
      connection = EMMongo::Connection.new
      EM.next_tick do
        connection.connected?.should == true
        connection.close
      end
      EM.add_timer(1) do
        EM.next_tick do
          connection.connected?.should == false
          done
        end
      end
    end 
  end

  # Support the old RMongo interface for now
  it 'should instantiate a Collection' do
    EM::Spec::Mongo.connection do |connection|
      connection.collection.is_a?(Collection).should == true
      EM::Spec::Mongo.close
    end 
  end

  it 'should instantiate a Databse' do
    EM::Spec::Mongo.connection do |connection|
      db1 = connection.db
      db1.is_a?(Database).should == true
      db2 = connection.db('db2')
      db2.is_a?(Database).should == true
      db2.should_not == db1
      EM::Spec::Mongo.close
    end 
  end


end
