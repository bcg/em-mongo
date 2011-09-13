module EM::Mongo
  class ServerResponse

    attr_reader :size, :request_id, :response_to, :op,
      :result_flags, :cursor_id, :starting_from,
      :number_returned, :docs, :connection


    def initialize(buffer, connection)
      @connection = connection
      # Header
      @size        = buffer.get_int
      @request_id  = buffer.get_int
      @response_to = buffer.get_int
      @op          = buffer.get_int

      # Response Header
      @result_flags     = buffer.get_int
      @cursor_id        = buffer.get_long
      @starting_from    = buffer.get_int
      @number_returned  = buffer.get_int

      # Documents
      pos = buffer.position
      @docs = (1..@number_returned).map do
        size= @connection.peek_size(buffer)
        doc = BSON::BSON_CODER.deserialize(buffer.to_s[pos,size])
        pos += size
        buffer.position = pos
        doc
      end
    end

  end
end
