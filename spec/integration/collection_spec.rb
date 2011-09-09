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

    context "safe_insert" do
      it "should succesfully save a document with no errors" do
        @conn, @coll = connection_and_collection('safe.test')
        @coll.safe_insert({"hello" => "world"}).callback do |ok|
          ok.should be_a_kind_of BSON::ObjectId
          done
        end
      end

      it "should respond with an error when an invalid document is saved" do
        @conn, @coll = connection_and_collection('safe.test')
        @coll.create_index("hello", :unique => true)
        a = {"hello" => "world"}
        @coll.insert(a)
        resp = @coll.safe_insert(a).errback do |err|
          err[0].should == EM::Mongo::OperationFailure
          done
        end
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

    context "safe_update" do
      it "should respond with an error when an invalid document is updated" do
        @conn, @coll = connection_and_collection('safe.update.test')
        @coll.create_index("x", :unique => true)
        @coll.insert({"x" => 5})
        @coll.insert({"x" => 10})

        @coll.safe_update({},{"x" => 10}).errback do |err|
          err[0].should == EM::Mongo::OperationFailure
          done
        end
      end
    end

  end

  describe "save" do

    it "should insert a record when no id is present" do
      @conn, @coll = connection_and_collection
      id = @coll.save("x" => 1)
      @coll.find("x" => 1).to_a.callback do |result|
        result[0]["_id"].should == id
        done
      end
    end

    it "should update a record when id is present" do
      @conn, @coll = connection_and_collection
      doc = {"x" => 1}
      id = @coll.save(doc)
      doc["x"] = 2
      @coll.save(doc).should be_true
      @coll.find().to_a.callback do |result|
        result.count.should == 1
        result[0]["x"].should == 2
        done
      end
    end

    context "safe_save" do
       it "should respond with an error when an invalid document is updated" do
        @conn, @coll = connection_and_collection('safe.save.test')
        @coll.create_index("x", :unique => true)
        @coll.save({"x" => 5})
        @coll.save({"x" => 5})

        @coll.safe_save({"x" => 5}).errback do |err|
          err[0].should == EM::Mongo::OperationFailure
          done
        end
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

  describe "distinct" do
    it "shoud perform a distinct query" do
      @conn, @coll = connection_and_collection
      @coll.insert([{:a => 0, :b => {:c => "a"}},
                     {:a => 1, :b => {:c => "b"}},
                     {:a => 1, :b => {:c => "c"}},
                     {:a => 2, :b => {:c => "a"}},
                     {:a => 3},
                     {:a => 3}])

      @coll.distinct(:a).callback do |vals|
        vals.sort.should == [0,1,2,3]
        @coll.distinct("b.c").callback do |vals2|
          vals2.sort.should == ["a","b","c"]
          done
        end
      end
    end
    it "should respect a query" do
      @conn, @coll = connection_and_collection
      @coll.insert([{:a => 0, :b => {:c => "a"}},
                     {:a => 1, :b => {:c => "b"}},
                     {:a => 1, :b => {:c => "c"}},
                     {:a => 2, :b => {:c => "a"}},
                     {:a => 3},
                     {:a => 3}])

      @coll.distinct(:a, {:a => {"$gt" => 1}}).callback do |vals|
        vals.sort.should == [2,3]
        done
      end
    end
    it "should respect a query and nested objects" do
      @conn, @coll = connection_and_collection
      @coll.insert([{:a => 0, :b => {:c => "a"}},
                     {:a => 1, :b => {:c => "b"}},
                     {:a => 1, :b => {:c => "c"}},
                     {:a => 2, :b => {:c => "a"}},
                     {:a => 3},
                     {:a => 3}])

      @coll.distinct("b.c", {"b.c" => {"$ne" => "c"}}).callback do |vals|
        vals.sort.should == ["a","b"]
        done
      end
    end
  end

  describe "group" do
    it "should fail if missing required options" do
      @conn, @coll = connection_and_collection
      lambda { @coll.group(:initial => {}) }.should raise_error EM::Mongo::MongoArgumentError
      lambda { @coll.group(:reduce => "foo") }.should raise_error EM::Mongo::MongoArgumentError
      done
    end
    it "should group results using eval form" do
      @conn, @coll = connection_and_collection
      @coll.save("a" => 1)
      @coll.save("b" => 1)
      @initial = {"count" => 0}
      @reduce_function = "function (obj, prev) { prev.count += inc_value; }"
      @coll.group(:initial => @initial, :reduce => BSON::Code.new(@reduce_function, {"inc_value" => 0.5})).callback do |result|
        result[0]["count"].should == 1
        done
      end
      @coll.group(:initial => @initial, :reduce => BSON::Code.new(@reduce_function, {"inc_value" => 1})).callback do |result|
        result[0]["count"].should == 2
        done
      end
      @coll.group(:initial => @initial, :reduce => BSON::Code.new(@reduce_function, {"inc_value" => 2})).callback do |result|
        result[0]["count"].should == 4
        done
      end
    end
    it "should finalize grouped results" do
      @conn, @coll = connection_and_collection
      @coll.save("a" => 1)
      @coll.save("b" => 1)
      @initial = {"count" => 0}
      @reduce_function = "function (obj, prev) { prev.count += inc_value; }"
      @finalize = "function(doc) {doc.f = doc.count + 200; }"
      @coll.group(:initial => @initial, :reduce => BSON::Code.new(@reduce_function, {"inc_value" => 1}), :finalize => BSON::Code.new(@finalize)).callback do |results|
        results[0]["f"].should == 202
        done
      end
    end
  end

  describe "grouping with a key" do
    it "should group" do
      @conn, @coll = connection_and_collection
      @coll.save("a" => 1, "pop" => 100)
      @coll.save("a" => 1, "pop" => 100)
      @coll.save("a" => 2, "pop" => 100)
      @coll.save("a" => 2, "pop" => 100)
      @initial = {"count" => 0, "foo" => 1}
      @reduce_function = "function (obj, prev) { prev.count += obj.pop; }"
      @coll.group(:key => :a, :initial => @initial, :reduce => @reduce_function).callback do |result|
        result.all? {|r| r['count'] = 200 }.should be_true
        done
      end
    end
  end

  describe "grouping with a function" do
    it "should group results" do
      @conn, @coll = connection_and_collection
      @coll.save("a" => 1)
      @coll.save("a" => 2)
      @coll.save("a" => 3)
      @coll.save("a" => 4)
      @coll.save("a" => 5)
      @initial = {"count" => 0}
      @keyf    = "function (doc) { if(doc.a % 2 == 0) { return {even: true}; } else {return {odd: true}} };"
      @reduce  = "function (obj, prev) { prev.count += 1; }"
      @coll.group(:keyf => @keyf, :initial => @initial, :reduce => @reduce).callback do |results|
        res = results.sort {|a,b| a['count'] <=> b['count']}
        (res[0]['even'] && res[0]['count']).should == 2.0
        (res[1]['odd'] && res[1]['count']) == 3.0
        done
      end
    end

    it "should group filtered results" do
      @conn, @coll = connection_and_collection
      @coll.save("a" => 1)
      @coll.save("a" => 2)
      @coll.save("a" => 3)
      @coll.save("a" => 4)
      @coll.save("a" => 5)
      @initial = {"count" => 0}
      @keyf    = "function (doc) { if(doc.a % 2 == 0) { return {even: true}; } else {return {odd: true}} };"
      @reduce  = "function (obj, prev) { prev.count += 1; }"
      @coll.group(:keyf => @keyf, :cond => {:a => {'$ne' => 2}},
        :initial => @initial, :reduce => @reduce).callback do |results|
        res = results.sort {|a, b| a['count'] <=> b['count']}
        (res[0]['even'] && res[0]['count']).should == 1.0
        (res[1]['odd'] && res[1]['count']) == 3.0
        done
      end
    end
  end

  context "indexes" do
    it "should create an index using symbols" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index :foo, :name => :bar
      @collection.index_information.callback do |info|
        info['bar'].should_not be_nil
        done
      end
    end

    it "should create a geospatial index" do
      @conn, @geo = connection_and_collection('geo')
      @geo.save({'loc' => [-100, 100]})
      @geo.create_index([['loc', EM::Mongo::GEO2D]])
      @geo.index_information.callback do |info|
        info['loc_2d'].should_not be_nil
        done
      end
    end

    it "should create a unique index" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['a', EM::Mongo::ASCENDING]], :unique => true)
      @collection.index_information.callback do |info|
        info['a_1']['unique'].should == true
        done
      end
    end

    it "should create an index in the background" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['b', EM::Mongo::ASCENDING]], :background => true)
      @collection.index_information.callback do |info|
        info['b_1']['background'].should == true
        done
      end
    end

    it "should require an array of arrays" do
      @conn, @collection = connection_and_collection('test-collection')
      proc { @collection.create_index(['c', EM::Mongo::ASCENDING]) }.should raise_error
      done
    end

    it "should enforce proper index types" do
      @conn, @collection = connection_and_collection('test-collection')
      proc { @collection.create_index([['c', 'blah']]) }.should raise_error
      done
    end

    it "should allow an alernate name to be specified" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index :bar, :name => 'foo_index'
      @collection.index_information.callback do |info|
        info['foo_index'].should_not be_nil
        done
      end
    end

    it "should generate indexes in the proper order" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.should_receive(:insert_documents) do |sel, coll|
        sel[0][:name].should == 'b_1_a_1'
      end
      @collection.create_index([['b',1],['a',1]])
      done
    end

    it "should allow multiple calls to create_index" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['a',1]]).should be_true
      @collection.create_index([['a',1]]).should be_true
      done
    end

    it "should allow the creation of multiple indexes" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['a',1]]).should be_true
      @collection.create_index([['b',1]]).should be_true
      done
    end

    it "should return a properly ordered index info" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['b',1],['a',1]])
      @collection.index_information.callback do |info|
        info['b_1_a_1'].should_not be_nil
        done
      end
    end

    it "should drop an index" do
      @conn, @collection = connection_and_collection('test-collection')
      @collection.create_index([['a',EM::Mongo::ASCENDING]])
      @collection.index_information.callback do |info|
        info['a_1'].should_not be_nil
        @collection.drop_index([['a',EM::Mongo::ASCENDING]]).callback do
          @collection.index_information.callback do |info|
            info['a_1'].should be_nil
            done
          end
        end
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
