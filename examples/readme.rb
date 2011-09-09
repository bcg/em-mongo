#bundle exec ruby examples/readme.rb
require 'em-mongo'
require 'eventmachine'

EM.run do
  db = EM::Mongo::Connection.new('localhost').db('my_database')
  collection = db.collection('my_collection')
  EM.next_tick do
    (1..10).each do |i|
      collection.insert( { :revolution => i } )
    end

    #find returns an EM::Mongo::Cursor
    cursor = collection.find

    #most cursor methods return an EM::Mongo::RequestResponse,
    #which is an EventMachine::Deferrable
    resp = cursor.to_a

    #when em-mongo IO methods succeed, they
    #will always call back with the return
    #value you would have expected from the
    #synchronous version of the method from
    #the mongo-ruby-driver
    resp.callback do |documents|
      puts "I just got #{documents.length} documents! I'm really cool!"
    end

    #when em-mongo IO methods fail, they
    #errback with an array in the form
    #[ErrorClass, "error message"]
    resp.errback do |err|
      raise *err
    end

    #iterate though each result in a query
    collection.find( :revolution => { "$gt" => 5 } ).limit(1).skip(1).each do |doc|
      #unlike the mongo-ruby-driver, each returns null at the end of the cursor
      if doc
        puts "Revolution ##{doc['revolution']}"
      end
    end

    #add an index
    collection.create_index [[:revolution, -1]]

    #insert a document and ensure it gets written
    save_resp = collection.safe_save( { :hi => "there" }, :last_error_params => {:fsync=>true} )
    save_resp.callback { puts "Hi is there, let us give thanks" }
    save_resp.errback { |err| puts "AAAAAAAAAAAAAAAARGH! Oh why! WHY!?!?!" }

    collection.drop

    EM.add_periodic_timer(1) { EM.stop }

  end

end