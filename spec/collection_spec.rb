require File.expand_path('spec_helper', File.dirname(__FILE__))

describe EMMongo::Collection do
  include EM::SpecHelper

  before(:all) do
    @numbers = {
      1 => 'one',
      2 => 'two',
      3 => 'three',
      4 => 'four',
      5 => 'five',
      6 => 'six',
      7 => 'seven',
      8 => 'eight',
      9 => 'nine'
    }
  end

  after(:all) do
  end

  it 'should insert an object' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert('hello' => 'world')
      obj.keys.should include '_id'
      obj['_id'].should be_a_kind_of(BSON::ObjectId)
      EM::Spec::Mongo.close
    end
  end

  it 'should find an object by attribute' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert("hello" => 'world')
      r = collection.find({"hello" => "world"},{}) do |res|
        res.size.should >= 1
        res[0]["hello"].should == "world"
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should find an object by id' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert('hello' => 'world')
      collection.find({'_id' => obj['_id']},{}) do |res|
        res.size.should >= 1
        res[0]['hello'].should == "world"
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should find all objects' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert('one' => 'one')
      collection.insert('two' => 'two')
      collection.find do |res|
        res.size.should >= 2
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should find large sets of objects' do
    EM::Spec::Mongo.collection do |collection|
      (0..1500).each { |n| collection.insert({n.to_s => n.to_s}) }
      collection.find do |res|
        res.size.should == EM::Mongo::DEFAULT_QUERY_DOCS
        collection.find({}, {:limit => 1500}) do |res|
          res.size.should == 1500
          EM::Spec::Mongo.close
        end
      end
    end
  end

  it 'should update an object' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert('hello' => 'world')
      collection.update({'hello' => 'world'}, {'hello' => 'newworld'})
      collection.find({'_id' => obj['_id']},{}) do |res|
        res[0]['hello'].should == 'newworld'
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should update an object with $inc' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert('hello' => 'world')
      collection.update({'hello' => 'world'}, {'$inc' => {'count' => 1}})
      collection.find({'_id' => obj['_id']},{}) do |res|
        res.first['hello'].should == 'world'
        res.first['count'].should == 1
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should remove an object' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert('hello' => 'world')
      collection.remove('_id' => obj['_id'])
      collection.find({'hello' => "world"}) do |res|
        res.size.should == 0
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should remove all objects' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert('one' => 'one')
      collection.insert('two' => 'two')
      collection.remove
      collection.find do |res|
        res.size.should == 0
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should insert a Time' do
    EM::Spec::Mongo.collection do |collection|
      t = Time.now.utc.freeze
      collection.insert('date' => t)
      collection.find do |res|
        res[0]['date'].to_s.should == t.to_s
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should insert a complex object' do
    EM::Spec::Mongo.collection do |collection|
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
      retobj = collection.insert(obj)
      collection.find({'_id' => obj['_id']}) do |ret|
        ret.size.should == 1
        ret[0].each_key do |key|
          ret[0][key].should == obj[key]
        end
        EM::Spec::Mongo.close
      end

    end
  end

  it 'should find an object using nested properties' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert({
        'name' => 'Google',
        'address' => {
          'city' => 'Mountain View',
          'state' => 'California'}
      })

      collection.first('address.city' => 'Mountain View') do |res|
        res['name'].should == 'Google'
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should find objects with specific values' do
    EM::Spec::Mongo.collection do |collection|
      @numbers.each do |num, word|
        collection.insert({'num' => num, 'word' => word})
      end

      collection.find({'num' => {'$in' => [1,3,5]}}) do |res|
        res.size.should == 3
        res.map{|r| r['num'] }.sort.should == [1,3,5]
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should find objects greater than something' do
    EM::Spec::Mongo.collection do |collection|
      @numbers.each do |num, word|
        collection.insert('num' => num, 'word' => word)
      end

      collection.find({'num' => {'$gt' => 3}}) do |res|
        res.size.should == 6
        res.map{|r| r['num'] }.sort.should == [4,5,6,7,8,9]
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should handle multiple pending queries' do
    EM::Spec::Mongo.collection do |collection|
      id = collection.insert("foo" => "bar")['_id']
      received = 0

      10.times do |n|
        collection.first("_id" => id) do |res|
          received += 1
          EM::Spec::Mongo.close if received == 10
        end
      end

    end
  end

end
