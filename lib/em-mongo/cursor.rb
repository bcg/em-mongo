# encoding: UTF-8

# Copyright (C) 2008-2011 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module EM::Mongo

  # A cursor over query results. Returned objects are hashes.
  class Cursor
    include EM::Mongo::Conversions
    #include Enumerable

    attr_reader :collection, :selector, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name, :transformer

    # Create a new cursor.
    #
    # Note: cursors are created when executing queries using [Collection#find] and other
    # similar methods. Application developers shouldn't have to create cursors manually.
    #
    # @return [Cursor]
    #
    # @core cursors constructor_details
    def initialize(collection, opts={})
      @cursor_id  = nil

      @db         = collection.db
      @collection = collection
      @connection = @db.connection
      #@logger     = @connection.logger

      # Query selector
      @selector   = opts[:selector] || {}

      # Special operators that form part of $query
      @order      = opts[:order]
      @explain    = opts[:explain]
      @hint       = opts[:hint]
      @snapshot   = opts[:snapshot]
      @max_scan   = opts.fetch(:max_scan, nil)
      @return_key = opts.fetch(:return_key, nil)
      @show_disk_loc = opts.fetch(:show_disk_loc, nil)

      # Wire-protocol settings
      @fields     = convert_fields_for_query(opts[:fields])
      @skip       = opts[:skip]     || 0
      @limit      = opts[:limit]    || 0
      @tailable   = opts[:tailable] || false
      @timeout    = opts.fetch(:timeout, true)

      # Use this socket for the query
      #@socket     = opts[:socket]

      @closed       = false
      @query_run    = false

      @transformer = opts[:transformer]
      batch_size(opts[:batch_size] || 0)

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @cache        = []
      @returned     = 0

      if @collection.name =~ /^\$cmd/ || @collection.name =~ /^system/
        @command = true
      else
        @command = false
      end
    end

    # Get the next document specified the cursor options.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with the next document or Nil if no documents remain.
    def next_document
      response = RequestResponse.new
      if @cache.length == 0
        refresh.callback do
          check_and_transform_document(@cache.shift, response)
        end
      else
        check_and_transform_document(@cache.shift, response)
      end
      response
    end
    alias :next :next_document

    def check_and_transform_document(doc, response)
      return response.succeed(nil) if doc.nil?

      if doc['$err']

        err = doc['$err']

        # If the server has stopped being the master (e.g., it's one of a
        # pair but it has died or something like that) then we close that
        # connection. The next request will re-open on master server.
        if err == "not master"
          @connection.close
          response.fail([ConnectionFailure, err])
        else
          response.fail([OperationFailure, err])
        end

      else
        response.succeed(
          @transformer ? @transformer.call(doc) : doc
        )
      end
    end
    private :check_and_transform_document

    # Reset this cursor on the server. Cursor options, such as the
    # query string and the values for skip and limit, are preserved.
    def rewind!
      close
      @cache.clear
      @cursor_id  = nil
      @closed     = false
      @query_run  = false
      @n_received = nil
    end

    # Determine whether this cursor has any remaining results.
    #
    # @return [EM::Mongo::RequestResponse]
    def has_next?
      response = RequestResponse.new
      num_resp = num_remaining
      num_resp.callback { |num| response.succeed( num > 0 ) }
      num_resp.errback { |err| response.fail err }
      response
    end

    # Get the size of the result set for this query.
    #
    # @param [Boolean] whether of not to take notice of skip and limit
    #
    # @return [EM::Mongo::RequestResponse] Calls back with the number of objects in the result set for this query.
    #
    # @raise [OperationFailure] on a database error.
    def count(skip_and_limit = false)
      response = RequestResponse.new
      command = BSON::OrderedHash["count",  @collection.name, "query",  @selector]

      if skip_and_limit
        command.merge!(BSON::OrderedHash["limit", @limit]) if @limit != 0
        command.merge!(BSON::OrderedHash["skip", @skip]) if @skip != 0
      end

      command.merge!(BSON::OrderedHash["fields", @fields])

      cmd_resp = @db.command(command)

      cmd_resp.callback { |doc| response.succeed( doc['n'].to_i ) }
      cmd_resp.errback do |err|
        if err[1] =~ /ns missing/
          response.succeed(0)
        else
          response.fail([OperationFailure, "Count failed: #{err[1]}"])
        end
      end

      response
    end

    # Sort this cursor's results.
    #
    # This method overrides any sort order specified in the Collection#find
    # method, and only the last sort applied has an effect.
    #
    # @param [Symbol, Array] key_or_list either 1) a key to sort by or 2)
    #   an array of [key, direction] pairs to sort by. Direction should
    #   be specified as EM::Mongo::ASCENDING (or :ascending / :asc) or EM::Mongo::DESCENDING (or :descending / :desc)
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    #
    # @raise [InvalidSortValueError] if the specified order is invalid.
    def sort(key_or_list, direction=nil)
      check_modifiable

      if !direction.nil?
        order = [[key_or_list, direction]]
      else
        order = key_or_list
      end

      @order = order
      self
    end

    # Limit the number of results to be returned by this cursor.
    #
    # This method overrides any limit specified in the Collection#find method,
    # and only the last limit applied has an effect.
    #
    # @return [Integer] the current number_to_return if no parameter is given.
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    #
    # @core limit limit-instance_method
    def limit(number_to_return=nil)
      return @limit unless number_to_return
      check_modifiable

      @limit = number_to_return
      self
    end

    # Skips the first +number_to_skip+ results of this cursor.
    # Returns the current number_to_skip if no parameter is given.
    #
    # This method overrides any skip specified in the Collection#find method,
    # and only the last skip applied has an effect.
    #
    # @return [Integer]
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    def skip(number_to_skip=nil)
      return @skip unless number_to_skip
      check_modifiable

      @skip = number_to_skip
      self
    end

    # Set the batch size for server responses.
    #
    # Note that the batch size will take effect only on queries
    # where the number to be returned is greater than 100.
    #
    # @param [Integer] size either 0 or some integer greater than 1. If 0,
    #   the server will determine the batch size.
    #
    # @return [Cursor]
    def batch_size(size=0)
      check_modifiable
      if size < 0 || size == 1
        raise ArgumentError, "Invalid value for batch_size #{size}; must be 0 or > 1."
      else
        @batch_size = size > @limit ? @limit : size
      end

      self
    end

    # Iterate over each document in this cursor, yielding it to the given
    # block.
    #
    # Iterating over an entire cursor will close it.
    #
    # @yield passes each document to a block for processing. When the cursor is empty,
    #   each will yield a nil value
    #
    # @example if 'comments' represents a collection of comments:
    #   comments.find.each do |doc|
    #     if doc
    #       puts doc['user']
    #     end
    #   end
    def each(&blk)
      raise "A callback block is required for #each" unless blk
      EM.next_tick do
        next_doc_resp = next_document
        next_doc_resp.callback do |doc|
          blk.call(doc)
          doc.nil? ? close : self.each(&blk)
        end
        next_doc_resp.errback do |err|
          if blk.arity > 1
            blk.call(:error, err)
          else
            blk.call(:error)
          end
        end
      end
    end

    # Receive all the documents from this cursor as an array of hashes.
    #
    # Notes:
    #
    # If you've already started iterating over the cursor, the array returned
    # by this method contains only the remaining documents. See Cursor#rewind! if you
    # need to reset the cursor.
    #
    # Use of this method is discouraged - in most cases, it's much more
    # efficient to retrieve documents as you need them by iterating over the cursor.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with an array of documents.
    def to_a
      response = RequestResponse.new
      items = []
      self.each do |doc,err|
        if doc == :error
          response.fail(err)
        elsif doc
          items << doc
        else
          response.succeed(items)
        end
      end
      response
    end

    # Get the explain plan for this cursor.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with a document containing the explain plan for this cursor.
    #
    # @core explain explain-instance_method
    def explain
      response = RequestResponse.new
      c = Cursor.new(@collection, query_options_hash.merge(:limit => -@limit.abs, :explain => true))

      exp_response = c.next_document
      exp_response.callback do |explanation|
        c.close
        response.succeed(explanation)
      end
      exp_response.errback do |err|
        c.close
        response.fail(err)
      end
      response
    end

    # Close the cursor.
    #
    # Note: if a cursor is read until exhausted (read until EM::Mongo::Constants::OP_QUERY or
    # EM::Mongo::Constants::OP_GETMORE returns zero for the cursor id), there is no need to
    # close it manually.
    #
    # Note also: Collection#find takes an optional block argument which can be used to
    # ensure that your cursors get closed.
    #
    # @return [True]
    def close
      if @cursor_id && @cursor_id != 0
        @cursor_id = 0
        @closed    = true
        message = BSON::ByteBuffer.new([0, 0, 0, 0])
        message.put_int(1)
        message.put_long(@cursor_id)
        @connection.send_command(EM::Mongo::OP_KILL_CURSORS, message)
      end
      true
    end

    # Is this cursor closed?
    #
    # @return [Boolean]
    def closed?; @closed; end

    # Returns an integer indicating which query options have been selected.
    #
    # @return [Integer]
    #
    # @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-EM::Mongo::Constants::OPQUERY
    # The MongoDB wire protocol.
    def query_opts
      opts     = 0
      opts    |= EM::Mongo::OP_QUERY_NO_CURSOR_TIMEOUT unless @timeout
      opts    |= EM::Mongo::OP_QUERY_SLAVE_OK if @connection.slave_ok?
      opts    |= EM::Mongo::OP_QUERY_TAILABLE if @tailable
      opts
    end

    # Get the query options for this Cursor.
    #
    # @return [Hash]
    def query_options_hash
      { :selector => @selector,
        :fields   => @fields,
        :skip     => @skip,
        :limit    => @limit,
        :order    => @order,
        :hint     => @hint,
        :snapshot => @snapshot,
        :timeout  => @timeout,
        :max_scan => @max_scan,
        :return_key => @return_key,
        :show_disk_loc => @show_disk_loc }
    end

    # Clean output for inspect.
    def inspect
      "<EM::Mongo::Cursor:0x#{object_id.to_s} namespace='#{@db.name}.#{@collection.name}' " +
        "@selector=#{@selector.inspect}>"
    end

    private

    # Convert the +:fields+ parameter from a single field name or an array
    # of fields names to a hash, with the field names for keys and '1' for each
    # value.
    def convert_fields_for_query(fields)
      case fields
        when String, Symbol
          {fields => 1}
        when Array
          return nil if fields.length.zero?
          fields.each_with_object({}) { |field, hash| hash[field] = 1 }
        when Hash
          return fields
      end
    end

    # Return the number of documents remaining for this cursor.
    # @return [EM::Mongo::RequestResponse]
    def num_remaining
      response = RequestResponse.new
      if @cache.length == 0
        ref_resp = refresh
        ref_resp.callback { response.succeed(@cache.length) }
        ref_resp.errback { |err| response.fail err }
      else
        response.succeed(@cache.length)
      end
      response
    end

    def refresh
      return RequestResponse.new.tap{|d|d.succeed} if @cursor_id && @cursor_id.zero?
      return send_initial_query unless @query_run

      message = BSON::ByteBuffer.new([0, 0, 0, 0])

      # DB name.
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@collection.name}")

      # Number of results to return.
      if @limit > 0
        limit = @limit - @returned
        if @batch_size > 0
          limit = limit < @batch_size ? limit : @batch_size
        end
        message.put_int(limit)
      else
        message.put_int(@batch_size)
      end

      # Cursor id.
      message.put_long(@cursor_id)

      response = RequestResponse.new
      @connection.send_command(EM::Mongo::OP_GET_MORE, message) do |resp|
        if resp == :disconnected
          response.fail(:disconnected)
        else
          @cache += resp.docs
          @n_received = resp.number_returned
          @returned += @n_received
          close_cursor_if_query_complete
          response.succeed
        end
      end
      response
    end

    # Run query the first time we request an object from the wire
    def send_initial_query
      response = RequestResponse.new
      message = construct_query_message
      @connection.send_command(EM::Mongo::OP_QUERY, message) do |resp|
        if resp == :disconnected
          response.fail(:disconnected)
        else
          @cache += resp.docs
          @n_received = resp.number_returned
          @cursor_id = resp.cursor_id
          @returned += @n_received
          @query_run = true
          close_cursor_if_query_complete
          response.succeed
        end
      end
      response
    end

    def construct_query_message
      message = BSON::ByteBuffer.new
      message.put_int(query_opts)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@collection.name}")
      message.put_int(@skip)
      message.put_int(@limit)
      spec = query_contains_special_fields? ? construct_query_spec : @selector
      message.put_binary(BSON::BSON_CODER.serialize(spec, false).to_s)
      message.put_binary(BSON::BSON_CODER.serialize(@fields, false).to_s) if @fields
      message
    end


    def construct_query_spec
      return @selector if @selector.has_key?('$query')
      spec = BSON::OrderedHash.new
      spec['$query']    = @selector
      spec['$orderby']  = EM::Mongo::Support.format_order_clause(@order) if @order
      spec['$hint']     = @hint if @hint && @hint.length > 0
      spec['$explain']  = true if @explain
      spec['$snapshot'] = true if @snapshot
      spec['$maxscan']  = @max_scan if @max_scan
      spec['$returnKey']   = true if @return_key
      spec['$showDiskLoc'] = true if @show_disk_loc
      spec
    end

    # Returns true if the query contains order, explain, hint, or snapshot.
    def query_contains_special_fields?
      @order || @explain || @hint || @snapshot
    end

    def to_s
      "DBResponse(flags=#@result_flags, cursor_id=#@cursor_id, start=#@starting_from)"
    end

    def close_cursor_if_query_complete
      close if @limit > 0 && @returned >= @limit
    end

    def check_modifiable
      if @query_run || @closed
        raise InvalidOperation, "Cannot modify the query once it has been run or closed."
      end
    end
  end
end