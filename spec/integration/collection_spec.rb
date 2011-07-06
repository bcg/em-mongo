require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

describe EMMongo::Collection do
  include EM::Spec

  it 'should return a sub collection via the indexer method' do
    @conn, @coll = connection_and_collection
    @coll["child"].name.should == "#{@coll.name}.child"
    done
  end

  it "should drop the collection" do
    @conn, @coll = connection_and_collection
    @coll.insert({:x => "x"})
    @coll.drop.callback do
      @coll.db.collection_names.callback do |names|
        names.should_not include @ns
        done
      end
    end
  end

  describe "find" do
    it 'should return a cursor' do
      @conn, @coll = connection_and_collection
      cursor = @coll.find(:hi=>"there")
      cursor.should be_a_kind_of(EM::Mongo::Cursor)
      done
    end

    it 'should find an object by attribute' do
      @conn, @coll = connection_and_collection

      @coll.insert("hello" => 'world')
      @coll.find({"hello" => "world"},{}).to_a.callback do |res|
        res.size.should >= 1
        res[0]["hello"].should == "world"
        done
      end
    end

    it 'should take strings or symbols for hashes' do
      @conn, @coll = connection_and_collection

      obj = @coll.insert({:_id => 1234, 'foo' => 'bar', :hello => 'world'})
      @coll.first({:_id => 1234},{}).callback do |res|
        res['hello'].should == 'world'
        res['foo'].should == 'bar'
        done
      end
    end

    it 'should find an object by symbol' do
      @conn, @coll = connection_and_collection

      @coll.insert('hello' => 'world')
      @coll.find({:hello => "world"},{}).to_a.callback do |res|
        res.size.should >= 1
        res[0]["hello"].should == "world"
        done
      end
    end

    it 'should find an object by id' do
      @conn, @coll = connection_and_collection

      id = @coll.insert('hello' => 'world')
      @coll.find({:_id => id},{}).to_a.callback do |res|
        res.size.should >= 1
        res[0]['hello'].should == "world"
        done
      end
    end

    it 'should find all objects' do
      @conn, @coll = connection_and_collection

      @coll.insert('one' => 'one')
      @coll.insert('two' => 'two')
      @coll.find.to_a.callback do |res|
        res.size.should >= 2
        done
      end
    end

    it 'should find objects and sort by the order field' do
      @conn, @coll = connection_and_collection

      @coll.insert(:name => 'one', :position => 0)
      @coll.insert(:name => 'three', :position => 2)
      @coll.insert(:name => 'two', :position => 1)

      @coll.find({}, {:order => 'position'}).to_a.callback do |res|
        res[0]["name"].should == 'one'
        res[1]["name"].should == 'two'
        res[2]["name"].should == 'three'
        done
      end

      @coll.find({}, {:order => [:position, :desc]}).to_a.callback do |res|
        res[0]["name"].should == 'three'
        res[1]["name"].should == 'two'
        res[2]["name"].should == 'one'
        done
      end
    end

    it "should find a single document with find_one/first" do
      @conn, @coll = connection_and_collection

      @coll.insert(:name => 'one', :position => 0)
      @coll.insert(:name => 'three', :position => 2)
      @coll.insert(:name => 'two', :position => 1)

      @coll.find_one({},:sort => [:position,-1]).callback do |first|
        first["position"].should == 2
        done
      end
    end

    it 'should find an object using nested properties' do
      @conn, @coll = connection_and_collection

      @coll.insert({
        'name' => 'Google',
        'address' => {
          'cxity' => 'Mountain View',
          'state' => 'California'}
      })

      @coll.first('address.cxity' => 'Mountain View').callback do |res|
        res['name'].should == 'Google'
        done
      end
    end

    it 'should find objects wxith specific values' do
      @conn, @coll = connection_and_collection

      number_hash.each do |num, word|
        @coll.insert({'num' => num, 'word' => word})
      end

      @coll.find({'num' => {'$in' => [1,3,5]}}).to_a.callback do |res|
        res.size.should == 3
        res.map{|r| r['num'] }.sort.should == [1,3,5]
        done
      end
    end

    it 'should find objects greater than something' do
      @conn, @coll = connection_and_collection

      number_hash.each do |num, word|
        @coll.insert('num' => num, 'word' => word)
      end

      @coll.find({'num' => {'$gt' => 3}}).to_a.callback do |res|
        res.size.should == 6
        res.map{|r| r['num'] }.sort.should == [4,5,6,7,8,9]
        done
      end
    end
  end

  describe "insert" do

    it 'should insert an object' do
      @conn, @coll = connection_and_collection

      doc = {'hello' => 'world'}
      id = @coll.insert(doc)
      id.should be_a_kind_of(BSON::ObjectId)
      doc[:_id].should be_a_kind_of(BSON::ObjectId)
      done
    end

    it "should insert multiple documents" do
      @conn, @coll = connection_and_collection

      docs = [{'hello' => 'world'}, {'goodbye' => 'womb'}]
      ids = @coll.insert(docs)
      ids.should be_a_kind_of(Array)
      ids[0].should == docs[0][:_id]
      ids[1].should == docs[1][:_id]
      done
    end

    it 'should insert an object with a custom _id' do
      @conn, @coll = connection_and_collection

      id = @coll.insert(:_id => 1234, 'hello' => 'world')
      id.should == 1234
      @coll.first({'hello' => 'world'}).callback do |res|
        res['_id'].should == 1234
        done
      end
    end

    it 'should insert a Time' do
      @conn, @coll = connection_and_collection

      t = Time.now.utc.freeze
      @coll.insert('date' => t)
      @coll.find.to_a.callback do |res|
        res[0]['date'].to_s.should == t.to_s
        done
      end
    end

    it 'should insert a complex object' do
      @conn, @coll = connection_and_collection

      obj = {
        'array' => [1,2,3],
        'float' => 123.456,
        'hash' => {'boolean' => true},
        'nil' => nil,
        'symbol' => :name,
        'string' => 'hello world',
        'time' => Time.now.to_f,
        'regex' => /abc$/ix
      }
      retobj = @coll.insert(obj)
      @coll.find({:_id => obj[:_id]}).to_a.callback do |ret|
        ret.size.should == 1
        ret[0].each_key do |key|
          next if key == '_id'
          ret[0][key].should == obj[key]
        end
        done
      end
    end

  end

  describe "update" do

    it 'should update an object' do
      @conn, @coll = connection_and_collection

      id = @coll.insert('hello' => 'world')
      @coll.update({'hello' => 'world'}, {'hello' => 'newworld'})
      @coll.find({:_id => id},{}).to_a.callback do |res|
        res[0]['hello'].should == 'newworld'
        done
      end
    end

    it 'should update an object wxith $inc' do
      @conn, @coll = connection_and_collection

      id = @coll.insert('hello' => 'world')
      @coll.update({'hello' => 'world'}, {'$inc' => {'count' => 1}})
      @coll.find({:_id => id},{}).to_a.callback do |res|
        res.first['hello'].should == 'world'
        res.first['count'].should == 1
        done
      end
    end

  end

  describe "remove" do

    it 'should remove an object' do
      @conn, @coll = connection_and_collection

      id = @coll.insert('hello' => 'world')
      @coll.remove(:_id => id)
      @coll.find({'hello' => "world"}).to_a.callback do |res|
        res.size.should == 0
        done
      end
    end

    it 'should remove all objects' do
      @conn, @coll = connection_and_collection

      @coll.insert('one' => 'one')
      @coll.insert('two' => 'two')
      @coll.remove
      @coll.find.to_a.callback do |res|
        res.size.should == 0
        done
      end
    end

  end

  describe "find_and_modify" do
    
    it "should find and modify a document" do
      @conn, @coll = connection_and_collection
      @coll << { :a => 1, :processed => false }
      @coll << { :a => 2, :processed => false }
      @coll << { :a => 3, :processed => false }

      resp = @coll.find_and_modify(:query => {}, :sort => [['a', -1]], :update => {"$set" => {:processed => true}})
      resp.callback do |doc|
        doc['processed'].should_not be_true
        @coll.find_one({:a=>3}).callback do |updated|
          updated['processed'].should be_true
          done
        end
      end
    end

    it "should fail with invalid options" do
      @conn, @coll = connection_and_collection
      @coll << { :a => 1, :processed => false }
      @coll << { :a => 2, :processed => false }
      @coll << { :a => 3, :processed => false }

      resp = @coll.find_and_modify(:blimey => {})
      resp.errback do |err|
        err[0].should == EM::Mongo::OperationFailure
        done
      end
    end

  end

  describe "mapreduce" do
    it "should map, and then reduce" do
      @conn, @coll = connection_and_collection
      @coll << { "user_id" => 1 }
      @coll << { "user_id" => 2 }

      m = "function() { emit(this.user_id, 1); }"
      r = "function(k,vals) { return 1; }"

      res = @coll.map_reduce(m, r, :out => 'foo')
      res.callback do |collection|
        collection.find_one({"_id" => 1}).callback do |doc|
          doc.should_not be_nil
          collection.find_one({"_id" => 2}).callback do |doc2|
            doc2.should_not be_nil
            done
          end
        end
      end
    end

    it "should work with code objects" do
      @conn, @coll = connection_and_collection
      @coll << { "user_id" => 1 }
      @coll << { "user_id" => 2 }

      m = BSON::Code.new "function() { emit(this.user_id, 1); }"
      r = BSON::Code.new "function(k,vals) { return 1; }"

      res = @coll.map_reduce(m, r, :out => 'foo')
      res.callback do |collection|
        collection.find_one({"_id" => 1}).callback do |doc|
          doc.should_not be_nil
          collection.find_one({"_id" => 2}).callback do |doc2|
            doc2.should_not be_nil
            done
          end
        end
      end
    end

    it "should respect a query" do
      @conn, @coll = connection_and_collection
      @coll << { "user_id" => 1 }
      @coll << { "user_id" => 2 }
      @coll <<  { "user_id" => 3 }

      m = BSON::Code.new "function() { emit(this.user_id, 1); }"
      r = BSON::Code.new "function(k,vals) { return 1; }"

      res = @coll.map_reduce(m, r, :query => {"user_id" => {"$gt" => 1}}, :out => 'foo')
      res.callback do |collection|
        collection.count .callback do |c|
          c.should == 2  
          collection.find_one({"_id" => 2}).callback do |doc|
            doc.should_not be_nil
            collection.find_one({"_id" => 3}).callback do |doc2|
              doc2.should_not be_nil
              done
            end
          end
        end
      end
    end

    it "should return a raw response if requested" do
      @conn, @coll = connection_and_collection
      m = BSON::Code.new("function() { emit(this.user_id, 1); }")
      r = BSON::Code.new("function(k,vals) { return 1; }")
      res = @coll.map_reduce(m, r, :raw => true, :out => 'foo')
      res.callback do |res|
        res["result"].should_not be_nil
        res["counts"].should_not be_nil
        res["timeMillis"].should_not be_nil
        done
      end
    end

    it "should use an output collection if specified" do
      @conn, @coll = connection_and_collection
      output_collection = "test-map-coll"
      m = BSON::Code.new("function() { emit(this.user_id, 1); }")
      r = BSON::Code.new("function(k,vals) { return 1; }")
      res = @coll.map_reduce(m, r, :raw => true, :out => output_collection)
      res.callback do |res|
        res["result"].should == output_collection
        res["counts"].should_not be_nil
        res["timeMillis"].should_not be_nil
        done
      end
    end

  end

 

  it 'should handle multiple pending queries' do
    @conn, @coll = connection_and_collection

    id = @coll.insert("foo" => "bar")
    received = 0

    10.times do |n|
      @coll.first("_id" => id).callback do |res|
        received += 1
        done
      end
    end
  end

end
