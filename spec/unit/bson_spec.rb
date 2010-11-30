require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')

# This is to prove how BSON works so we can code around it where appropriate
# BSON documents and Ruby Hashes are not the same thing afterall.
#
# http://www.mongodb.org/display/DOCS/BSON

describe BSON do

  it 'should do what it does' do
    doc = {:_id => 12345, :foo => 'notbar', "foo" => "bar", :hello => :world  }
    doc = BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc, false, true).to_s)
    # 1. An ID passed as Symbol is really a String
    doc['_id'].should == 12345
    # 2. More to the point, all keys are Strings.
    doc['hello'].should == :world
    # 3. The last String/Symbol wins
    doc['foo'].should == 'bar'
  end

end
