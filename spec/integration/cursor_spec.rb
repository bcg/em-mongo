require File.expand_path('spec_helper', File.dirname(__FILE__) + '/../')



describe EMMongo::Cursor do
  include EM::Spec

  it 'should describe itself via inspect' do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new( @coll, :selector => {'a' => 1} )
    cursor.inspect.should == "<EM::Mongo::Cursor:0x#{cursor.object_id.to_s} namespace='#{@coll.db.name}.#{@coll.name}' " +
        "@selector=#{cursor.selector.inspect}>"
    done
  end

  it 'should explain itself' do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new(@coll, :selector => {'a' => 1} )
    cursor.explain.callback do |explanation|
      explanation['cursor'].should_not be_nil
      explanation['n'].should be_kind_of Numeric
      explanation['millis'].should be_kind_of Numeric
      explanation['nscanned'].should be_kind_of Numeric
      done
    end
  end

  it "should allow limit and skip to be chained" do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new(@coll)
    all = []
    10.times do |i|
      all << {"x" => i}
      @coll.save(all[-1])
    end

    cursor.limit(5).skip(3).sort("x",1).to_a.callback do |results|
      all.slice(3...8).each_with_index do |item,idx|
        results[idx]["x"].should == item["x"]
      end
      done
    end
  end

  it "should allow a limit larger than the batch size" do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new(@coll, :selector => {})
    all = []
    1501.times do |i|
      @coll.insert(i.to_s => i.to_s)
    end
    cursor.limit(1500).to_a.callback do |docs|
      docs.length.should == 1500
      done
    end
  end



  it "should say if it has next" do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new(@coll)
    1.times do |i|
      @coll.save("x" => 1)
    end
    cursor.has_next?.callback do |result|
      result.should be_true
      cursor.next_document.callback do |doc|
        cursor.has_next?.callback do |result|
          result.should be_false
          done
        end
      end
    end
  end

  it "should rewind" do
    @conn, @coll = connection_and_collection
    cursor = EM::Mongo::Cursor.new(@coll)
    100.times do |i|
      @coll.save("x" => 1)
    end
    cursor.to_a.callback do |r1|
      r1.length.should == 100
      cursor.to_a.callback do |r2|
        r2.length.should == 0
        cursor.rewind!
        cursor.to_a.callback do |r3|
          r3.length.should == 100
          done
        end
      end

    end
  end

  describe "Get More" do
    it "should refill via get more" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll)
      1000.times do |i|
        @coll.save("x" => 1)
      end
      cursor.to_a.callback do |results|
        results.length.should == 1000
        done
      end
    end
  end

  describe "Count" do

    it 'should count 0 records in a empty collection' do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll)
      cursor.count.callback do |c|
        c.should == 0
        done
      end
    end

    it "should count records in a collection" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll)
      10.times do |i|
        @coll.save("x" => 1)
      end

      cursor.count.callback do |c|
        c.should == 10
        done
      end
    end

    it "should ignore skip and limit by default" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll).skip(5).limit(5)
      10.times do |i|
        @coll.save("x" => i)
      end

      cursor.count.callback do |c|
        c.should == 10
        done
      end
    end

    it "should account for skip when requested" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll).limit(5)
      10.times do |i|
        @coll.save("x" => i)
      end

      cursor.count(true).callback do |c|
        c.should == 5
        done
      end
    end

    it "should account for skip when requested" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll).skip(5)
      10.times do |i|
        @coll.save("x" => i)
      end

      cursor.count(true).callback do |c|
        c.should == 5
        done
      end
    end

    it "should count based on a simple selector" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll, :selector => {"x"=>1})
      10.times do |i|
        @coll.save("x" => i)
      end

      cursor.count(true).callback do |c|
        c.should == 1
        done
      end
    end

    it "should count based on a selector with an operator" do
      @conn, @coll = connection_and_collection
      cursor = EM::Mongo::Cursor.new(@coll, :selector => {"x"=>{"$lt"=>5}})
      10.times do |i|
        @coll.save("x" => i)
      end

      cursor.count(true).callback do |c|
        c.should == 5
        done
      end
    end

    it "should count a non-existing collection as 0 without vomiting blood" do
      @conn, @coll = connection_and_collection
      @coll = @conn.db.collection('imnotreallyheredontlookatme')

      cursor = EM::Mongo::Cursor.new(@coll)

      cursor.count(true).callback do |c|
        c.should == 0
        done
      end
    end
  end

  describe "Sort" do
    it "should sort ascending" do
      @conn, @coll = connection_and_collection
      5.times do |i|
        @coll.save("x" => i)
      end
      cursor = EM::Mongo::Cursor.new(@coll).sort(:x, 1)
      cursor.next_document.callback do |first|
        first["x"].should == 0
        done
      end
    end

    it "should sort descending" do
      @conn, @coll = connection_and_collection
      5.times do |i|
        @coll.save("x" => i)
      end
      cursor = EM::Mongo::Cursor.new(@coll).sort(:x, -1)
      cursor.next_document.callback do |first|
        first["x"].should == 4
        done
      end
    end

    it "should sort descending using a symbol sort dir" do
      @conn, @coll = connection_and_collection
      5.times do |i|
        @coll.save("x" => i)
      end
      cursor = EM::Mongo::Cursor.new(@coll).sort(["x", :desc])
      cursor.next_document.callback do |first|
        first["x"].should == 4
        done
      end
    end

    it "should not allow sort to be called on an executed cursor" do
      @conn, @coll = connection_and_collection
      5.times do |i|
        @coll.save("x" => i)
      end
      cursor = EM::Mongo::Cursor.new(@coll).sort(["x", :desc])
      cursor.next_document.callback do |first|
        lambda { cursor.sort("x",1) }.should raise_error EM::Mongo::InvalidOperation
        done
      end
    end

    it "should sort by dates" do
      @conn, @coll = connection_and_collection
      5.times do |i|
        @coll.insert("x" => Time.utc(2000 + i))
      end
      cursor = EM::Mongo::Cursor.new(@coll).sort(["x", :desc])
      cursor.next_document.callback do |first|
        first["x"].year.should == 2004
        done
      end
    end

    describe "Each" do
      it "should iterate through each doc, returning null when done" do
        @conn, @coll = connection_and_collection
        5.times do |i|
          @coll.insert("x" => i)
        end
        cursor = EM::Mongo::Cursor.new(@coll)
        counter = 0
        cursor.each do |doc|
          if doc
            counter+=1
          else
            counter.should == 5
            done
          end
        end
      end
    end

    describe "to_a" do
      it "should return an array of all documents in a query" do
        @conn, @coll = connection_and_collection
        5.times do |i|
          @coll.insert("x" => i)
        end
        cursor = EM::Mongo::Cursor.new(@coll).sort("x",1)
        cursor.to_a.callback do |docs|
          docs.length.should == 5
          5.times do |i|
            docs[i]["x"].should == i
          end
          done
        end
      end
    end

    describe "Transformer (a robot in disguise)" do
      it "should set the transformer when passed in the constructor" do
        @conn, @coll = connection_and_collection
        transformer = Proc.new {|doc|doc}
        cursor = EM::Mongo::Cursor.new(@coll, :transformer => transformer)
        cursor.transformer.should == transformer
        done
      end
      it "should transform docs with next" do
        @conn, @coll = connection_and_collection
        @coll.insert({:a=>1})
        klass = Struct.new(:id,:a)
        transformer = Proc.new {|doc|klass.new(doc['_id'],doc['a'])}
        cursor = EM::Mongo::Cursor.new(@coll, :transformer => transformer)
        cursor.next.callback do |doc|
          doc.should be_kind_of klass
          doc.id.should be_kind_of BSON::ObjectId
          doc.a.should == 1
          done
        end
      end
      it "should transform docs with each" do
        @conn, @coll = connection_and_collection
        @coll.insert({:a=>1})
        klass       = Struct.new(:id, :a)
        transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
        cursor      = EM::Mongo::Cursor.new(@coll, :transformer => transformer)

        cursor.each do |doc|
          if doc
            doc.should be_kind_of klass
            doc.id.should be_kind_of BSON::ObjectId
            doc.a.should == 1
          end
          done
        end
      end
    end
  end

end
