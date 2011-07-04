module EM::Mongo
  class Collection
    attr_accessor :connection
    attr_reader :pk_factory, :hint

    def initialize(db, ns, connection = nil)
      @db = db || "db"
      @ns = ns || "ns"
      @name = [@db,@ns].join('.')
      @connection = connection || EM::Mongo::Connection.new
    end

    def db
      connection.db(@db)
    end

    def name
      @ns
    end

    def find(selector={}, opts={}, &blk)
      raise "find requires a block" if not block_given?

      skip  = opts.delete(:skip) || 0
      limit = opts.delete(:limit) || 0
      order = opts.delete(:order)

      @connection.find(@name, skip, limit, order, selector, nil, &blk)
    end

    def first(selector={}, opts={}, &blk)
      opts[:limit] = 1
      find(selector, opts) do |res|
        yield res.first
      end
    end

    def insert(doc)
      sanitize_id!(doc)
      @connection.insert(@name, doc)
      doc[:_id] # mongo-ruby-driver returns ID
    end

    def update(selector, updater, opts={})
      @connection.update(@name, selector, updater, opts)
      true
    end

    # XXX Missing tests
    def save(doc, opts={})
      id = has_id?(doc)
      sanitize_id!(doc)
      if id
        update({:_id => id}, doc, :upsert => true)
        id
      else
        insert(doc)
      end
    end

    def remove(obj = {})
      @connection.delete(@name, obj)
      true
    end

    private

    def has_id?(doc)
      # mongo-ruby-driver seems to take :_id over '_id' for some reason
      id = doc[:_id] || doc['_id']
      return id if id
      nil
    end

    def sanitize_id!(doc)
      doc[:_id] = has_id?(doc) || BSON::ObjectId.new
      doc.delete('_id')
      doc
    end

  end
end
