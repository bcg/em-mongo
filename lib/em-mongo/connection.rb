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

  ASCENDING  =  1
  DESCENDING = -1
  GEO2D      = '2d'

  DEFAULT_MAX_BSON_SIZE = 4 * 1024 * 1024

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

    def prepare_message(op, message, options={})
      req_id = new_request_id
      message.prepend!(message_headers(op, req_id, message))
      req_id = prepare_safe_message(message,options) if options[:safe]
      [req_id, message.to_s]
    end

    def prepare_safe_message(message,options)
        db_name = options[:db_name]
        unless db_name
          raise( ArgumentError, "You must include the :db_name option when :safe => true" )
        end

        last_error_params = options[:last_error_params] || false
        last_error_message = BSON::ByteBuffer.new

        build_last_error_message(last_error_message, db_name, last_error_params)
        last_error_id = new_request_id
        last_error_message.prepend!(message_headers(EM::Mongo::OP_QUERY, last_error_id, last_error_message))
        message.append!(last_error_message)
        last_error_id
    end

    def message_headers(operation, request_id, message)
      headers = BSON::ByteBuffer.new
      headers.put_int(16 + message.size)
      headers.put_int(request_id)
      headers.put_int(0)
      headers.put_int(operation)
      headers
    end

    def send_command(op, message, options={}, &cb)
      request_id, buffer = prepare_message(op, message, options)

      callback do
        send_data buffer
      end

      @responses[request_id] = cb if cb
      request_id
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
        callback.call(response) if callback
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
        @responses.values.each { |resp| resp.call(:disconnected) }

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

     # Constructs a getlasterror message. This method is used exclusively by
    # Connection#send_message_with_safe_check.
    #
    # Because it modifies message by reference, we don't need to return it.
    def build_last_error_message(message, db_name, opts)
      message.put_int(0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{db_name}.$cmd")
      message.put_int(0)
      message.put_int(-1)
      cmd = BSON::OrderedHash.new
      cmd[:getlasterror] = 1
      if opts.is_a?(Hash)
        opts.assert_valid_keys(:w, :wtimeout, :fsync)
        cmd.merge!(opts)
      end
      message.put_binary(BSON::BSON_CODER.serialize(cmd, false).to_s)
      nil
    end

  end

  # An em-mongo Connection
  class Connection

    # Initialize and connect to a MongoDB instance
    # @param [String] host the host name or IP of the mongodb server to connect to
    # @param [Integer] port the port the mongodb server is listening on
    # @param [Integer] timeout the connection timeout
    # @opts [Hash] opts connection options
    def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil, opts = {})
      @em_connection = EMConnection.connect(host, port, timeout, opts)
      @db = {}
    end

    # Return a database with the given name.
    #
    # @param [String] db_name a valid database name.
    #
    # @return [EM::Mongo::Database]
    def db(name = DEFAULT_DB)
      @db[name] ||= EM::Mongo::Database.new(name, self)
    end

    # Close the connection to the database.
    def close
      @em_connection.close
    end

    #@return [true, false]
    #  whether or not the connection is currently connected
    def connected?
      @em_connection.connected?
    end

    def send_command(*args, &block);@em_connection.send_command(*args, &block);end

    # Is it okay to connect to a slave?
    #
    # @return [Boolean]
    def slave_ok?;@em_connection.slave_ok?;end

  end
end
