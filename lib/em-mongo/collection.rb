module EMMongo
  class Collection
    attr_accessor :connection

    def initialize(db, ns, connection = nil)
      @db = db || "db"
      @ns = ns || "ns"
      @name = [@db,@ns].join('.')
      @connection = connection || EMMongo::Connection.new
    end

    def find(selector={}, opts={}, &blk)
      raise "find requires a block" if not block_given?

      skip  = opts.delete(:skip) || 0
      limit = opts.delete(:limit) || 0

      @connection.find(@name, skip, limit, selector, &blk)
    end

    def first(selector={}, opts={}, &blk)
      skip  = opts.delete(:skip) || 0
      limit = 1
      @connection.find(@name, skip, limit, selector, &blk)
    end

    def insert(obj, &blk)
      obj[:_id] ||= EMMongo::Util.unique_id 
      @connection.insert(@name, obj)
      if block_given?
        EM.next_tick do
          yield obj
        end
      end
      obj
    end

    def remove(obj = {}, &blk)
      @connection.delete(@name, obj)
      if block_given?
        EM.next_tick do
          yield true
        end
      end
      true
    end

#    def index obj
#      obj = { obj => true } if obj.is_a? Symbol
#
#      indexes.insert({ :name => obj.keys.map{|o| o.to_s }.sort.join('_'),
#                       :ns => @ns,
#                       :key => obj }, false)
#    end

#    def indexes obj = {}, &blk
#      @indexes ||= self.class.new("#{@ns.split('.').first}.system.indexes")
#      blk ? @indexes.find(obj.merge(:ns => @ns), &blk) : @indexes
#    end

    def method_missing meth
      puts meth
      raise ArgumentError, 'collection cannot take block' if block_given?
      (@subns ||= {})[meth] ||= self.class.new("#{@ns}.#{meth}", @client)
    end

  end
end
