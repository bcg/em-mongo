module EMMongo
  DEFAULT_IP   = "127.0.0.1"
  DEFAULT_PORT = 27017
  DEFAULT_DB   = "db"
  DEFAULT_NS   = "ns"

  module EMConnection
    class Error < Exception; 
      class ConnectionNotBound
      end
    end
    
    include EM::Deferrable

    RESERVED    = 0
    OP_REPLY    = 1
    OP_MSG      = 1000
    OP_UPDATE   = 2001
    OP_INSERT   = 2002
    OP_QUERY    = 2004
    OP_DELETE   = 2006

    attr_reader :connection

    def responses_pending?
      @responses.size >= 1
    end

    def connected?
      @is_connected
    end

    # RMongo interface
    def collection(db = DEFAULT_DB, ns = DEFAULT_NS)
      raise "Not connected" if not connected?
      EMMongo::Collection.new(db, ns, self)
    end

    # MongoDB Commands

    def send_command(id, *args, &blk)
      request_id = @request_id += 1

      callback {
        buf  = Buffer.new
        buf.write :int, request_id,
                  :int, response = 0,
                  :int, operation = id

        buf.write *args
        send_data [ buf.size + 4 ].pack('i') # header length first
        send_data buf.data
      }

      @responses[request_id] = blk if blk
      request_id
    end

    def insert(collection_name, documents)
      # XXX multiple documents?
      send_command(OP_INSERT, :int,      RESERVED,
                              :cstring,  collection_name,
                              :bson,     documents)
    end

    def delete(collection_name, selector)
      send_command(OP_DELETE, :int,      RESERVED,
                              :cstring,  collection_name,
                              :int,      RESERVED,
                              :bson,     selector)
    end

    def find(collection_name, skip, limit, query, &blk)
      send_command(OP_QUERY,  :int,       RESERVED,
                              :cstring,   collection_name,
                              :int,       skip,
                              :int,       limit,
                              :bson,      query,
                              &blk)
    end


    # EM hooks
    def initialize(options={})
      @request_id = 0
      @responses = {}
      @is_connected = false
      @host = options[:host] || DEFAULT_IP
      @port = options[:port] || DEFAULT_PORT

      @on_close = proc{
        raise Error, "could not connect to server #{@host}:#{@port}"
      }
      timeout options[:timeout] if options[:timeout]
      errback{ @on_close.call }
    end

    def self.connect(host = DEFAULT_IP, port = DEFAUL_PORT, timeout = nil)
      opt = {:host => host, :port => port, :timeout => timeout}
      EM.connect(host, port, self, opt)
    end

    def connection_completed
      log 'connected'
      @buf = Buffer.new
      @is_connected = true
      @on_close = proc{
      }
      succeed
    end

    def receive_data data
      log 'receive_data', data
      @buf << data

      until @buf.empty?
        # packet size
        size = @buf.read(:int)

        # XXX put size back into the buffer!!
        break unless @buf.size >= size-4

        # header
        id, response, operation = @buf.read(:int, :int, :int)

        # body
        reserved, cursor, start, num = @buf.read(:int, :longlong, :int, :int)

        # bson results
        results = (1..num).map do
          @buf.read(:bson)
        end

        if cb = @responses.delete(response)
          cb.call(results)
        end

        # close if no more responses pending
        close_connection if @close_pending and @responses.size == 0
      end
    end

    def send_data data
      log 'send_data', data
      super data
    end

    def unbind
      log "unbind"
      @is_connected = false
      @on_close.call unless $!
    end

    def close
      log "close"
      @on_close = proc{ yield if block_given? }
      if @responses.empty?
        close_connection
      else
        @close_pending = true
      end
    end

    private

    def log *args
      return
      pp args
      puts
    end

  end
end

# Make EMMongo look like mongo-ruby-driver
module EMMongo
  class Database
    def initialize(name = DEFAULT_DB, connection = nil)
      @db_name = name
      @em_connection = connection || EMMongo::Connection.new
      @collection = nil
    end

    def collection(name = DEFAULT_NS)
      @collection = EMMongo::Collection.new(@db_name, name, @em_connection)
    end

    def close
      @em_connection.close
    end
  end
  class Connection
    def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil)
      @em_connection = EMConnection.connect(host, port, timeout) 
      @db = {}
      self
    end
    
    def db(name = DEFAULT_DB)
      @db[name] = EMMongo::Database.new(name, @em_connection)
    end

    def collection(db = DEFAULT_DB, ns = DEFAULT_NS)
      @em_connection.collection(db, ns)
    end

    def close
      @em_connection.close
    end

    def connected?
      @em_connection.connected?
    end

  end
end
