module EM::Mongo
  DEFAULT_IP         = "127.0.0.1"
  DEFAULT_PORT       = 27017
  DEFAULT_DB         = "db"
  DEFAULT_NS         = "ns"
  DEFAULT_QUERY_DOCS = 101

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

    STANDARD_HEADER_SIZE = 16
    RESPONSE_HEADER_SIZE = 20

    attr_reader :connection

    def responses_pending?
      @responses.size >= 1
    end

    def connected?
      @is_connected
    end

    def new_request_id
      @request_id += 1
    end

    # MongoDB Commands

    def message_headers(operation, request_id, message)
      headers = BSON::ByteBuffer.new
      headers.put_int(16 + message.size)
      headers.put_int(request_id)
      headers.put_int(0)
      headers.put_int(operation)
      headers
    end

    def send_command(buffer, request_id, &blk)
      callback do
        send_data buffer
      end

      @responses[request_id] = blk if blk
      request_id
    end

    def insert(collection_name, documents)
      message = BSON::ByteBuffer.new([0, 0, 0, 0])
      BSON::BSON_RUBY.serialize_cstr(message, collection_name)

      documents = [documents] if not documents.is_a?(Array)
      documents.each { |doc| message.put_array(BSON::BSON_CODER.serialize(doc, true, true).to_a) }

      req_id = new_request_id
      message.prepend!(message_headers(OP_INSERT, req_id, message))
      send_command(message.to_s, req_id)
    end

    def update(collection_name, selector, document, options)
      message = BSON::ByteBuffer.new([0, 0, 0, 0])
      BSON::BSON_RUBY.serialize_cstr(message, collection_name)

      flags  = 0
      flags += 1 if options[:upsert]
      flags += 2 if options[:multi]
      message.put_int(flags)

      message.put_array(BSON::BSON_CODER.serialize(selector, true, true).to_a)
      message.put_array(BSON::BSON_CODER.serialize(document, false, true).to_a)

      req_id = new_request_id
      message.prepend!(message_headers(OP_UPDATE, req_id, message))
      send_command(message.to_s, req_id)
    end

    def delete(collection_name, selector)
      message = BSON::ByteBuffer.new([0, 0, 0, 0])
      BSON::BSON_RUBY.serialize_cstr(message, collection_name)
      message.put_int(0)
      message.put_array(BSON::BSON_CODER.serialize(selector, false, true).to_a)
      req_id = new_request_id
      message.prepend!(message_headers(OP_DELETE, req_id, message))
      send_command(message.to_s, req_id)
    end

    def find(collection_name, skip, limit, query, fields, &blk)
      message = BSON::ByteBuffer.new
      message.put_int(RESERVED) # query options
      BSON::BSON_RUBY.serialize_cstr(message, collection_name)
      message.put_int(skip)
      message.put_int(limit)
      message.put_array(BSON::BSON_CODER.serialize(query, false).to_a)
      message.put_array(BSON::BSON_CODER.serialize(fields, false).to_a) if fields
      req_id = new_request_id
      message.prepend!(message_headers(OP_QUERY, req_id, message))
      send_command(message.to_s, req_id, &blk)
    end

    # EM hooks
    def initialize(options={})
      @request_id    = 0
      @responses     = {}
      @is_connected  = false
      @host          = options[:host] || DEFAULT_IP
      @port          = options[:port] || DEFAULT_PORT
      @on_unbind     = options[:unbind_cb] || proc {}

      @on_close = proc {
        raise Error, "failure with mongodb server #{@host}:#{@port}"
      }
      timeout options[:timeout] if options[:timeout]
      errback { @on_close.call }
    end

    def self.connect(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil)
      opt = {:host => host, :port => port, :timeout => timeout}
      EM.connect(host, port, self, opt)
    end

    def connection_completed
      @buffer = BSON::ByteBuffer.new
      @is_connected = true
      succeed
    end

    def message_received?(buffer)
      x= remaining_bytes(@buffer)
      x > STANDARD_HEADER_SIZE && x >= peek_size(@buffer)
    end

    def remaining_bytes(buffer)
      buffer.size-buffer.position
    end

    def peek_size(buffer)
      position= buffer.position
      size= buffer.get_int
      buffer.position= position
      size
    end

    def receive_data(data)

      @buffer.append!(BSON::ByteBuffer.new(data.unpack('C*')))

      @buffer.rewind
      while message_received?(@buffer)
        response_to, docs= next_response
        callback = @responses.delete(response_to)
        callback.call(docs) if callback
      end

      if @buffer.more?
        remaining_bytes= @buffer.size-@buffer.position
        @buffer = BSON::ByteBuffer.new(@buffer.get(remaining_bytes))
        @buffer.rewind
      else
        @buffer.clear
      end

      close_connection if @close_pending && @responses.empty?

    end

    def next_response()

      # Header
      size        = @buffer.get_int
      request_id  = @buffer.get_int
      response_to = @buffer.get_int
      op          = @buffer.get_int
      #puts "message header #{size} #{request_id} #{response_to} #{op}"

      # Response Header
      result_flags     = @buffer.get_int
      cursor_id        = @buffer.get_long
      starting_from    = @buffer.get_int
      number_returned  = @buffer.get_int
      #puts "response header #{result_flags} #{cursor_id} #{starting_from} #{number_returned}"

      # Documents
      docs = (1..number_returned).map do
        size= peek_size(@buffer)
        buf = BSON::ByteBuffer.new(@buffer.get(size))
        BSON::BSON_CODER.deserialize(buf)
      end
      [response_to,docs]
    end

    def unbind
      @is_connected = false
      @on_unbind.call
    end

    def close
      @on_close = proc { yield if block_given? }
      if @responses.empty?
        close_connection
      else
        @close_pending = true
      end
    end

  end

end

# Make EM::Mongo look like mongo-ruby-driver
module EM::Mongo
  class Database
    def initialize(name = DEFAULT_DB, connection = nil)
      @db_name = name
      @em_connection = connection || EM::Mongo::Connection.new
      @collection = nil
    end

    def collection(name = DEFAULT_NS)
      @collection = EM::Mongo::Collection.new(@db_name, name, @em_connection)
    end

    def close
      @em_connection.close
    end
  end
  class Connection
    def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil)
      @em_connection = EMConnection.connect(host, port, timeout)
      @db = {}
    end

    def db(name = DEFAULT_DB)
      @db[name] ||= EM::Mongo::Database.new(name, @em_connection)
    end

    def close
      @em_connection.close
    end

    def connected?
      @em_connection.connected?
    end

  end
end
