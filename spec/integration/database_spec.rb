require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

describe EMMongo::Database do
  include EM::Spec

  it 'should add a user' do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.collection(EM::Mongo::Database::SYSTEM_USER_COLLECTION).remove({})
    @db.add_user('test', 'test').callback do |res|
      res.should_not == nil
      res.should be_a_kind_of(BSON::ObjectId)
      done
    end
  end

  it 'should authenticate a user' do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.add_user('test', 'test')
    @db.authenticate('test', 'test').callback do |res|
      res.should == true
      done
    end
  end

  it "should create a collection" do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.create_collection("a").callback do |col|
      col.should be_kind_of EM::Mongo::Collection
      col.name.should == "a"
      done
    end
  end

  it "should drop a collection" do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.create_collection("a").callback do |col|
      @db.drop_collection("a").callback do
        @db.collection_names.callback do |names|
          names.should_not include "a"
          done
        end
      end
    end
  end

  it "should provide a list of collection names in the database" do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.create_collection "a"
    @db.create_collection("b").callback do
      @db.collection_names.callback do |names|
        names.should include "a"
        names.should include "b"
        done
      end
    end
  end

  it "should provide a list of collections in the database" do
    @conn = EM::Mongo::Connection.new
    @db = @conn.db
    @db.create_collection "a"
    @db.create_collection("b").callback do
      @db.collection_names.callback do |names|
        @db.collections do |collections|
          collections.length.should == names.length
          collections.each do |col|
            col.should be_kind_of EM::Mongo::Collection
          end
        end
        done
      end
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

  describe "Errors" do
    describe "when there are no errors" do
      it "should return a nil 'err' from get_last_error" do
        @conn = EM::Mongo::Connection.new
        @db = @conn.db
        @db.reset_error_history.callback do
          @db.get_last_error.callback do |doc|
            doc['err'].should be_nil
            done
          end
        end
      end
      it "should have a false error?" do
        @conn = EM::Mongo::Connection.new
        @db = @conn.db
        @db.reset_error_history.callback do
          @db.error?.callback do |result|
            result.should == false
            done
          end
        end
      end
    end
    describe "when there are errors" do
      it "should return a value for 'err' from get_last_error" do
        @conn = EM::Mongo::Connection.new
        @db = @conn.db
        @db.command({:forceerror=>1}, :check_response => false).callback do
          @db.get_last_error.callback do |doc|
            doc['err'].should_not be_nil
            done
          end
        end
      end
      it "should have a true error?" do
        @conn = EM::Mongo::Connection.new
        @db = @conn.db
        @db.command({:forceerror=>1}, :check_response => false).callback do
          @db.error?.callback do |result|
            result.should == true
            done
          end
        end
      end
    end
    it "should be able to reset the error history" do
      @conn = EM::Mongo::Connection.new
      @db = @conn.db
      @db.command({:forceerror=>1}, :check_response => false).callback do
        @db.reset_error_history.callback do
          @db.error?.callback do |result|
            result.should == false
            done
          end
        end
      end
    end
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

  describe "Indexes" do
    #Index functions are integration tested via the collection specs. Maybe the wrong order,
    #but the collection index functions all call down to the database index functions, and the
    #tests would simply duplicate eachother
  end

end
