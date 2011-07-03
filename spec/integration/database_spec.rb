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
  
  it 'should cache collections correctly' do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    a = @db.collection('first_collection')
    b = @db.collection('second_collection')
    a.should_not == b                      
    @db.collection('first_collection').should == a
    @db.collection('second_collection').should == b            
    done
  end

  describe "Command" do
    it "should fail when the database returns an error" do
      @conn = EM::Mongo::Connection.new
      @db = @conn.db
      @db.command({:non_command => 1}, :check_response => true).errback do
        done
      end
    end
    it "should not fail when checkresponse is false" do
      @conn = EM::Mongo::Connection.new
      @db = @conn.db
      @db.command({:non_command => 1}, :check_response => false).callback do
        done
      end
    end
    it "should succesfully execute a valid command" do
      @conn, @coll = connection_and_collection
      @db = @conn.db
      @coll.insert( {:col => {:easy => "andy" } } )
      @db.command({:collstats => @coll.name}).callback do |doc|
        doc.should_not be_nil
        doc["count"].should == 1
        done
      end
    end
  end

end
