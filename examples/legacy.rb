#bundle exec ruby examples/legacy.rb
require 'em-mongo'
require 'em-mongo/prev.rb'

require 'eventmachine'

EM.run do
  conn = EM::Mongo::Connection.new('localhost')
  db = conn.db('my_database')
  collection = db.collection('my_collection')
  EM.next_tick do

    (1..10).each do |i|
      conn.insert('my_database.my_collection', { :revolution => i } )
    end

    conn.update('my_database.my_collection', {:revolution => 9}, {:revolution => 8.5})

    conn.delete('my_database.my_collection', {:revolution => 1})

    collection.find do |documents|
      puts "I just got #{documents.length} documents! I'm really cool!"
    end

    #iterate though each result in a query
    collection.find( {:revolution => { "$gt" => 5 }}, :limit =>1, :skip => 1, :order => [:revolution, -1]) do |docs|
      docs.each do |doc|
        puts "Revolution ##{doc['revolution']}"
      end
    end

    collection.drop

    EM.add_periodic_timer(1) { EM.stop }

  end

end