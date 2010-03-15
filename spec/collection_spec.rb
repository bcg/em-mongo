require File.dirname(__FILE__) + '/spec_helper.rb'

describe EMMongo::Collection do
  include EM::SpecHelper

  before(:each) do
  end

  after(:all) do
  end

  it 'should insert an object' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert(:hello => 'world')
      obj.keys.should include :_id
      obj[:_id].should be_a_kind_of String
      obj[:_id].length.should == 24
      EM::Spec::Mongo.close
    end
  end

  it 'should find an object' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert(:hello => 'world') do
        r = collection.find({:hello => "world"},{}) do |res|
          res.size.should >= 1
          res[0][:hello].should == "world"
          EM::Spec::Mongo.close
        end
      end
    end
  end

  it 'should find all objects' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert(:one => 'one')
      collection.insert(:two => 'two')
      collection.find do |res|
        res.size.should >= 2
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should remove an object' do
    EM::Spec::Mongo.collection do |collection|
      obj = collection.insert(:hello => 'world')
      collection.remove(obj)
      collection.find({:hello => "world"}) do |res|
        res.size.should == 0
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should remove all objects' do
    EM::Spec::Mongo.collection do |collection|
      collection.insert(:one => 'one')
      collection.insert(:two => 'two')
      collection.remove
      collection.find do |res|
        res.size.should == 0
        EM::Spec::Mongo.close
      end
    end
  end

  it 'should insert a complex object' do
    EM::Spec::Mongo.collection do |collection|
      obj = {
        :array => [1,2,3],
        :float => 123.456,
        :hash => {:boolean => true},
        :nil => nil,
        :symbol => :name,
        :string => 'hello world',
        :time => Time.at(Time.now.to_i),
        :regex => /abc$/ix
      }
      obj = collection.insert(obj)
      collection.find(:_id => obj[:_id]) do |ret|
        ret.should == [ obj ]
        EM::Spec::Mongo.close
      end
    end
  end

  xit 'should find an object using nested properties' do
    EM::Spec::Mongo.connect do |m|
      coll = m.collection
      
      coll.insert({
        :name => 'Google',
        :address => {
          :city => 'Mountain View',
          :state => 'California'}
      })

      coll.first('address.city' => 'Mountain View') do |res|
        STDERR.puts res.inspect
        res[:name].should == 'Google'
      end
    end
  end

end
