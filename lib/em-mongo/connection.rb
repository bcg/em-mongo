module EM::Mongo
  DEFAULT_IP         = "127.0.0.1"
  DEFAULT_PORT       = 27017
  DEFAULT_DB         = "db"
  DEFAULT_NS         = "ns"
  DEFAULT_QUERY_DOCS = 101

  OP_REPLY        = 1
  OP_MSG          = 1000
  OP_UPDATE       = 2001
  OP_INSERT       = 2002
  OP_QUERY        = 2004
  OP_GET_MORE     = 2005
  OP_DELETE       = 2006
  OP_KILL_CURSORS = 2007

  OP_QUERY_TAILABLE          = 2 ** 1
  OP_QUERY_SLAVE_OK          = 2 ** 2
  OP_QUERY_OPLOG_REPLAY      = 2 ** 3
  OP_QUERY_NO_CURSOR_TIMEOUT = 2 ** 4
  OP_QUERY_AWAIT_DATA        = 2 ** 5
  OP_QUERY_EXHAUST           = 2 ** 6

  class EMConnection < EM::Connection
    MAX_RETRIES = 5

    class Error < Exception;
      class ConnectionNotBound
      end
    end

    include EM::Deferrable

    RESERVED    = 0

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

    def slave_ok?
      @slave_ok
    end

    # MongoDB Commands

    def prepare_message(op, message)
      req_id = new_request_id
      message.prepend!(message_headers(op, req_id, message))
      [req_id, message.to_s]
    end
    
    def message_headers(operation, request_id, message)
      headers = BSON::ByteBuffer.new
      headers.put_int(16 + message.size)
      headers.put_int(request_id)
      headers.put_int(0)
      headers.put_int(operation)
      headers
    end

    def send_command(op, message)
      request_id, buffer = prepare_message(op, message)

      callback do
        send_data buffer
      end

      @responses[request_id] = EM::DefaultDeferrable.new
    end

    # EM hooks
    def initialize(options={})
      @request_id    = 0
      @retries       = 0
      @responses     = {}
      @is_connected  = false
      @host          = options[:host]        || DEFAULT_IP
      @port          = options[:port]        || DEFAULT_PORT
      @on_unbind     = options[:unbind_cb]   || proc {}
      @reconnect_in  = options[:reconnect_in]|| false
      @slave_ok      = options[:slave_ok]    || false

      @on_close = proc {
        raise Error, "failure with mongodb server #{@host}:#{@port}"
      }
      timeout options[:timeout] if options[:timeout]
      errback { @on_close.call }
    end

    def self.connect(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil, opts = nil)
      opt = {:host => host, :port => port, :timeout => timeout, :reconnect_in => false}.merge(opts)
      EM.connect(host, port, self, opt)
    end

    def connection_completed
      @buffer = BSON::ByteBuffer.new
      @is_connected = true
      @retries = 0
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
        response = next_response
        callback = @responses.delete(response.response_to)
        callback.succeed(response)
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
      ServerResponse.new(@buffer, self)
    end

    def unbind
      if @is_connected
        @responses.values.each { |resp| resp.fail(:disconnected) }

        @request_id = 0
        @responses = {}
      end

      @is_connected = false

      set_deferred_status(nil)

      if @reconnect_in && @retries < MAX_RETRIES
        EM.add_timer(@reconnect_in) { reconnect(@host, @port) }
      elsif @on_unbind
        @on_unbind.call
      else
        raise "Connection to Mongo Lost"
      end

      @retries += 1
    end

    def close
      @on_close = proc { yield if block_given? }
      if @responses.empty?
        close_connection_after_writing
      else
        @close_pending = true
      end
    end

  end

  class Connection

    def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil, opts = {})
      @em_connection = EMConnection.connect(host, port, timeout, opts)
      @db = {}
    end

    def db(name = DEFAULT_DB)
      @db[name] ||= EM::Mongo::Database.new(name, self)
    end

    def close
      @em_connection.close
    end

    def connected?
      @em_connection.connected?
    end
   
    def send_command(*args);@em_connection.send_command(*args);end
    def slave_ok?;@em_connection.slave_ok?;end

  end
end
