require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

describe EMMongo::Database do
  include EM::Spec

  it 'should add a user' do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.add_user('test', 'test') do |res| 
      res.should_not == nil
      res.should be_a_kind_of(BSON::ObjectId)
      done
    end
  end

  # This test requires the above test.
  it 'should authenticate a user' do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.authenticate('test', 'test') do |res|
      res.should == true
      done
    end
  end 


end
