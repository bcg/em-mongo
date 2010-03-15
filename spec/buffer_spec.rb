require File.dirname(__FILE__) + '/spec_helper.rb'

include EMMongo

describe Buffer do
  before do
    @buf = Buffer.new
  end

  it 'have contents' do
    @buf.contents.should == ''
  end

  it 'initialize with data' do
    @buf = Buffer.new('abc')
    @buf.contents.should == 'abc'
  end

  it 'append raw data' do
    @buf << 'abc'
    @buf << 'def'
    @buf.contents.should == 'abcdef'
  end

  it 'append other buffers' do
    @buf << Buffer.new('abc')
    @buf.data.should == 'abc'
  end

  it 'have a position' do
    @buf.pos.should == 0
  end

  it 'have a length' do
    @buf.length.should == 0
    @buf << 'abc'
    @buf.length.should == 3
  end

  it 'know the end' do
    @buf.empty?.should == true
  end

  it 'read and write data' do
    @buf._write('abc')
    @buf.rewind
    @buf._read(2).should == 'ab'
    @buf._read(1).should == 'c'
  end

  it 'raise on overflow' do
    lambda{ @buf._read(1) }.should { raise Buffer::Overflow }
  end

  it 'raise on invalid types' do
    lambda{ @buf.read(:junk) }.should { raise Buffer::InvalidType }
    lambda{ @buf.write(:junk, 1) }.should { raise Buffer::InvalidType }
  end

  { :byte => 0b10101010,
    :short => 100,
    :int => 65536,
    :double => 123.456,
    :long => 100_000_000,
    :longlong => 666_555_444_333_222_111,
    :cstring => 'hello',
  }.each do |type, value|

    it "read and write a #{type}" do
      @buf.write(type, value)
      @buf.rewind
      @buf.read(type).should == value
      #@buf.should.be.empty
    end

  end
  
  it "read and write multiple times" do
    arr = [ :byte, 0b10101010, :short, 100 ]
    @buf.write(*arr)
    @buf.rewind
    @buf.read(arr.shift).should == arr.shift
    @buf.read(arr.shift).should == arr.shift
  end
  
  [
    { :num => 1                                      },
    { :symbol => :abc                                },
    { :object => {}                                  },
    { :array => [1, 2, 3]                            },
    { :string => 'abcdefg'                           },
    { :oid => { :_id => '51d9ca7053a2012be4ecd660' } },
    { :ref => { :_ns => 'namespace',
                :_id => '51d9ca7053a2012be4ecd660' } },
    { :boolean => true                               },
    { :time => Time.at(Time.now.to_i)                },
    { :null => nil                                   },
    { :regex => /^.*?def/im                          }
  ]. each do |bson|

    it "read and write bson with #{bson.keys.first}s" do
      @buf.write(:bson, bson)
      @buf.rewind
      @buf.read(:bson).should == bson
      #@buf.should.be.empty
    end

  end

  it 'do transactional reads with #extract' do
    @buf.write :byte, 8
    orig = @buf.to_s

    @buf.rewind
    @buf.extract do |b|
      b.read :byte
      b.read :short
    end

    @buf.pos.should == 0
    @buf.data.should == orig
  end
end
