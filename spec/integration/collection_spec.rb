require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

describe EMMongo::Collection do
  include EM::Spec

  it 'should insert an object' do
    @conn, @coll = connection_and_collection

    doc = {'hello' => 'world'}
    id = @coll.insert(doc)
    id.should be_a_kind_of(BSON::ObjectId)
    doc[:_id].should be_a_kind_of(BSON::ObjectId)
    done
  end

  it 'should insert an object with a custom _id' do
    @conn, @coll = connection_and_collection

    id = @coll.insert(:_id => 1234, 'hello' => 'world')
    id.should == 1234
    @coll.first({'hello' => 'world'}) do |res|
      res['_id'].should == 1234
      done
    end
  end

  it 'should find an object by attribute' do
    @conn, @coll = connection_and_collection
    
    @coll.insert("hello" => 'world')
    @coll.find({"hello" => "world"},{}) do |res|
      res.size.should >= 1
      res[0]["hello"].should == "world"
      done
    end
  end

  it 'should take strings or symbols for hashes' do
    @conn, @coll = connection_and_collection

    obj = @coll.insert({:_id => 1234, 'foo' => 'bar', :hello => 'world'})
    @coll.first({:_id => 1234},{}) do |res|
      res['hello'].should == 'world' 
      res['foo'].should == 'bar'
      done
    end
  end

  it 'should find an object by symbol' do
    @conn, @coll = connection_and_collection
    
    @coll.insert('hello' => 'world')
    @coll.find({:hello => "world"},{}) do |res|
      res.size.should >= 1
      res[0]["hello"].should == "world"
      done
    end
  end

  it 'should find an object by id' do
    @conn, @coll = connection_and_collection
    
    id = @coll.insert('hello' => 'world')
    @coll.find({:_id => id},{}) do |res|
      res.size.should >= 1
      res[0]['hello'].should == "world"
      done
    end
  end

  it 'should find all objects' do
    @conn, @coll = connection_and_collection

    @coll.insert('one' => 'one')
    @coll.insert('two' => 'two')
    @coll.find do |res|
      res.size.should >= 2
      done
    end
  end

  it 'should find large sets of objects' do
    @conn, @coll = connection_and_collection
    
    (0..1500).each { |n| @coll.insert({n.to_s => n.to_s}) }
    @coll.find do |res|
      res.size.should == EM::Mongo::DEFAULT_QUERY_DOCS
      @coll.find({}, {:limit => 1500}) do |res|
        res.size.should == 1500
        done
      end
    end
  end

  it 'should update an object' do
    @conn, @coll = connection_and_collection

    id = @coll.insert('hello' => 'world')
    @coll.update({'hello' => 'world'}, {'hello' => 'newworld'})
    @coll.find({:_id => id},{}) do |res|
      res[0]['hello'].should == 'newworld'
      done
    end
  end

  it 'should update an object wxith $inc' do
    @conn, @coll = connection_and_collection

    id = @coll.insert('hello' => 'world')
    @coll.update({'hello' => 'world'}, {'$inc' => {'count' => 1}})
    @coll.find({:_id => id},{}) do |res|
      res.first['hello'].should == 'world'
      res.first['count'].should == 1
      done
    end
  end

  it 'should remove an object' do
    @conn, @coll = connection_and_collection

    id = @coll.insert('hello' => 'world')
    @coll.remove(:_id => id)
    @coll.find({'hello' => "world"}) do |res|
      res.size.should == 0
      done
    end
  end

  it 'should remove all objects' do
    @conn, @coll = connection_and_collection

    @coll.insert('one' => 'one')
    @coll.insert('two' => 'two')
    @coll.remove
    @coll.find do |res|
      res.size.should == 0
      done
    end
  end

  it 'should insert a Time' do
    @conn, @coll = connection_and_collection

    t = Time.now.utc.freeze
    @coll.insert('date' => t)
    @coll.find do |res|
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
    @coll.find({:_id => obj[:_id]}) do |ret|
      ret.size.should == 1
      ret[0].each_key do |key|
        next if key == '_id'
        ret[0][key].should == obj[key]
      end
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

    @coll.first('address.cxity' => 'Mountain View') do |res|
      res['name'].should == 'Google'
      done
    end
  end

  it 'should find objects wxith specific values' do
    @conn, @coll = connection_and_collection

    number_hash.each do |num, word|
      @coll.insert({'num' => num, 'word' => word})
    end

    @coll.find({'num' => {'$in' => [1,3,5]}}) do |res|
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

    @coll.find({'num' => {'$gt' => 3}}) do |res|
      res.size.should == 6
      res.map{|r| r['num'] }.sort.should == [4,5,6,7,8,9]
      done
    end
  end

  it 'should handle multiple pending queries' do
    @conn, @coll = connection_and_collection
    
    id = @coll.insert("foo" => "bar")
    received = 0

    10.times do |n|
      @coll.first("_id" => id) do |res|
        received += 1
        done
      end
    end
  end

end
