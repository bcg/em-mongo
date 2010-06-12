require File.expand_path('spec_helper', File.dirname(__FILE__))

describe EMMongo::Connection do
  include EM::SpecHelper

  it 'should connect' do
    em do
      connection = EMMongo::Connection.new
      EM.next_tick do
        connection.should be_connected
        done
      end
    end
  end

  it 'should close' do
    em do
      connection = EMMongo::Connection.new

      EM.add_timer(1) do
        connection.should be_connected
        connection.close
      end

      EM.add_timer(2) do
        EM.next_tick do
          connection.should_not be_connected
          done
        end
      end
    end
  end

  it 'should instantiate a Database' do
    EM::Spec::Mongo.connection do |connection|
      db1 = connection.db
      db1.should be_kind_of(EM::Mongo::Database)

      db2 = connection.db('db2')
      db2.should be_kind_of(EM::Mongo::Database)
      db2.should_not == db1

      EM::Spec::Mongo.close
    end
  end


end
