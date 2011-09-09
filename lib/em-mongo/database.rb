module EM::Mongo
  class Database

    SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
    SYSTEM_INDEX_COLLECTION = "system.indexes"
    SYSTEM_PROFILE_COLLECTION = "system.profile"
    SYSTEM_USER_COLLECTION = "system.users"
    SYSTEM_JS_COLLECTION = "system.js"
    SYSTEM_COMMAND_COLLECTION = "$cmd"

    # The length of time that Collection.ensure_index should cache index calls
    attr_accessor :cache_time

    # @param [String] name the database name.
    # @param [EM::Mongo::Connection] connection a connection object pointing to MongoDB. Note
    #   that databases are usually instantiated via the Connection class. See the examples below.
    #
    # @core databases constructor_details
    def initialize(name = DEFAULT_DB, connection = nil)
      @db_name = name
      @em_connection = connection || EM::Mongo::Connection.new
      @collection = nil
      @collections = {}
      @cache_time = 300 #5 minutes.
    end

    # Get a collection by name.
    #
    # @param [String, Symbol] name the collection name.
    #
    # @return [EM::Mongo::Collection]
    def collection(name = EM::Mongo::DEFAULT_NS)
      @collections[name] ||= EM::Mongo::Collection.new(@db_name, name, @em_connection)
    end

    # Get the connection associated with this database
    #
    # @return [EM::Mongo::Connection]
    def connection
      @em_connection
    end

    #Get the name of this database
    #
    # @return [String]
    def name
      @db_name
    end

    # Get an array of collection names in this database.
    #
    # @return [EM::Mongo::RequestResponse]
    def collection_names
      response = RequestResponse.new
      name_resp = collections_info.to_a
      name_resp.callback do |docs|
        names = docs.collect{ |doc| doc['name'] || '' }
        names = names.delete_if {|name| name.index(self.name).nil? || name.index('$')}
        names = names.map{ |name| name.sub(self.name + '.','')}
        response.succeed(names)
      end
      name_resp.errback { |err| response.fail err }
      response
    end

    # Get an array of Collection instances, one for each collection in this database.
    #
    # @return [EM::Mongo::RequestResponse]
    def collections
      response = RequestResponse.new
      name_resp = collection_names
      name_resp.callback do |names|
        collections = names.map do |name|
          EM::Mongo::Collection.new(@db_name, name, @em_connection)
        end
        response.succeed collections
      end
      name_resp.errback { |err| response.fail err }
      response
    end

    # Get info on system namespaces (collections). This method returns
    # a cursor which can be iterated over. For each collection, a hash
    # will be yielded containing a 'name' string and, optionally, an 'options' hash.
    #
    # @param [String] coll_name return info for the specifed collection only.
    #
    # @return [EM::Mongo::Cursor]
    def collections_info(coll_name=nil)
      selector = {}
      selector[:name] = full_collection_name(coll_name) if coll_name
      Cursor.new(EM::Mongo::Collection.new(@db_name, SYSTEM_NAMESPACE_COLLECTION, @em_connection), :selector => selector)
    end

    # Create a collection.
    #
    # new collection. If +strict+ is true, will raise an error if
    # collection +name+ already exists.
    #
    # @param [String, Symbol] name the name of the new collection.
    #
    # @option opts [Boolean] :capped (False) created a capped collection.
    #
    # @option opts [Integer] :size (Nil) If +capped+ is +true+,
    #   specifies the maximum number of bytes for the capped collection.
    #   If +false+, specifies the number of bytes allocated
    #   for the initial extent of the collection.
    #
    # @option opts [Integer] :max (Nil) If +capped+ is +true+, indicates
    #   the maximum number of records in a capped collection.
    #
    # @raise [MongoDBError] raised under two conditions:
    #   either we're in +strict+ mode and the collection
    #   already exists or collection creation fails on the server.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with the new collection
    def create_collection(name)
      response = RequestResponse.new
      names_resp = collection_names
      names_resp.callback do |names|
        if names.include?(name.to_s)
          response.succeed EM::Mongo::Collection.new(@db_name, name, @em_connection)
        end

        # Create a new collection.
        oh = BSON::OrderedHash.new
        oh[:create] = name
        cmd_resp = command(oh)
        cmd_resp.callback do |doc|
          if EM::Mongo::Support.ok?(doc)
            response.succeed EM::Mongo::Collection.new(@db_name, name, @em_connection)
          else
            response.fail [MongoDBError, "Error creating collection: #{doc.inspect}"]
          end
        end
        cmd_resp.errback { |err| response.fail err }
      end
      names_resp.errback { |err| response.fail err }
      response
    end

    # Drop a collection by +name+.
    #
    # @param [String, Symbol] name
    #
    # @return [EM::Mongo::RequestResponse] Calls back with +true+ on success or +false+ if the collection name doesn't exist.
    def drop_collection(name)
      response = RequestResponse.new
      names_resp = collection_names
      names_resp.callback do |names|
        if names.include?(name.to_s)
          cmd_resp = command(:drop=>name)
          cmd_resp.callback do |doc|
            response.succeed EM::Mongo::Support.ok?(doc)
          end
          cmd_resp.errback { |err| response.fail err }
        else
          response.succeed false
        end
      end
      names_resp.errback { |err| response.fail err }
      response
    end

    # Drop an index from a given collection. Normally called from
    # Collection#drop_index or Collection#drop_indexes.
    #
    # @param [String] collection_name
    # @param [String] index_name
    #
    # @return [EM::Mongo::RequestResponse] returns +true+ on success.
    #
    # @raise MongoDBError if there's an error renaming the collection.
    def drop_index(collection_name, index_name)
      response = RequestResponse.new
      oh = BSON::OrderedHash.new
      oh[:deleteIndexes] = collection_name
      oh[:index] = index_name.to_s
      cmd_resp = command(oh, :check_response => false)
      cmd_resp.callback do |doc|
        if EM::Mongo::Support.ok?(doc)
          response.succeed(true)
        else
          response.fail [MongoDBError, "Error with drop_index command: #{doc.inspect}"]
        end
      end
      cmd_resp.errback do |err|
        response.fail err
      end
      response
    end

    # Get information on the indexes for the given collection.
    # Normally called by Collection#index_information.
    #
    # @param [String] collection_name
    #
    # @return [EM::Mongo::RequestResponse] Calls back with a hash where keys are index names and the values are lists of [key, direction] pairs
    #   defining the index.
    def index_information(collection_name)
      response = RequestResponse.new
      sel  = {:ns => full_collection_name(collection_name)}
      idx_resp = Cursor.new(self.collection(SYSTEM_INDEX_COLLECTION), :selector => sel).to_a
      idx_resp.callback do |indexes|
        info = indexes.inject({}) do |info, index|
          info[index['name']] = index
          info
        end
        response.succeed info
      end
      idx_resp.errback do |err|
        fail err
      end
      response
    end

    # Run the getlasterror command with the specified replication options.
    #
    # @option opts [Boolean] :fsync (false)
    # @option opts [Integer] :w (nil)
    # @option opts [Integer] :wtimeout (nil)
    #
    # @return [EM::Mongo::RequestResponse] the entire response to getlasterror.
    #
    # @raise [MongoDBError] if the operation fails.
    def get_last_error(opts={})
      response = RequestResponse.new
      cmd = BSON::OrderedHash.new
      cmd[:getlasterror] = 1
      cmd.merge!(opts)
      cmd_resp = command(cmd, :check_response => false)
      cmd_resp.callback do |doc|
        if EM::Mongo::Support.ok?(doc)
          response.succeed doc
        else
          response.fail [MongoDBError, "error retrieving last error: #{doc.inspect}"]
        end
      end
      cmd_resp.errback { |err| response.fail err }
      response
    end

    # Return +true+ if an error was caused by the most recently executed
    # database operation.
    #
    # @return [EM::Mongo::RequestResponse]
    def error?
      response = RequestResponse.new
      err_resp = get_last_error
      err_resp.callback do |doc|
        response.succeed doc['err'] != nil
      end
      err_resp.errback do |err|
        response.fail err
      end
      response
    end

    # Reset the error history of this database
    #
    # Calls to DB#previous_error will only return errors that have occurred
    # since the most recent call to this method.
    #
    # @return [EM::Mongo::RequestResponse]
    def reset_error_history
      command(:reseterror => 1)
    end


    # A shortcut returning db plus dot plus collection name.
    #
    # @param [String] collection_name
    #
    # @return [String]
    def full_collection_name(collection_name)
      "#{name}.#{collection_name}"
    end



    # Send a command to the database.
    #
    # Note: DB commands must start with the "command" key. For this reason,
    # any selector containing more than one key must be an OrderedHash.
    #
    # Note also that a command in MongoDB is just a kind of query
    # that occurs on the system command collection ($cmd). Examine this method's implementation
    # to see how it works.
    #
    # @param [OrderedHash, Hash] selector an OrderedHash, or a standard Hash with just one
    # key, specifying the command to be performed. In Ruby 1.9, OrderedHash isn't necessary since
    # hashes are ordered by default.
    #
    # @option opts [Boolean] :check_response (true) If +true+, raises an exception if the
    # command fails.
    # @option opts [Socket] :socket a socket to use for sending the command. This is mainly for internal use.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with a hash representing the result of the command
    #
    # @core commands command_instance-method
    def command(selector, opts={})
      check_response = opts.fetch(:check_response, true)
      raise MongoArgumentError, "command must be given a selector" unless selector.is_a?(Hash) && !selector.empty?

      if selector.keys.length > 1 && RUBY_VERSION < '1.9' && selector.class != BSON::OrderedHash
        raise MongoArgumentError, "DB#command requires an OrderedHash when hash contains multiple keys"
      end

      response = RequestResponse.new
      cmd_resp = Cursor.new(self.collection(SYSTEM_COMMAND_COLLECTION), :limit => -1, :selector => selector).next_document

      cmd_resp.callback do |doc|
        if doc.nil?
          response.fail([OperationFailure, "Database command '#{selector.keys.first}' failed: returned null."])
        elsif (check_response && !EM::Mongo::Support.ok?(doc))
          response.fail([OperationFailure, "Database command '#{selector.keys.first}' failed: #{doc.inspect}"])
        else
          response.succeed(doc)
        end
      end

      cmd_resp.errback do |err|
        response.fail([OperationFailure, "Database command '#{selector.keys.first}' failed: #{err[1]}"])
      end

      response
    end

    # Authenticate with the given username and password. Note that mongod
    # must be started with the --auth option for authentication to be enabled.
    #
    # @param [String] username
    # @param [String] password
    #
    # @return [EM::Mongo::RequestResponse] Calls back with +true+ or +false+, indicating success or failure
    #
    # @raise [AuthenticationError]
    #
    # @core authenticate authenticate-instance_method
    def authenticate(username, password)
      response = RequestResponse.new
      auth_resp = self.collection(SYSTEM_COMMAND_COLLECTION).first({'getnonce' => 1})
      auth_resp.callback do |res|
        if not res or not res['nonce']
          response.succeed false
        else
          auth                 = BSON::OrderedHash.new
          auth['authenticate'] = 1
          auth['user']         = username
          auth['nonce']        = res['nonce']
          auth['key']          = EM::Mongo::Support.auth_key(username, password, res['nonce'])

          auth_resp2 = self.collection(SYSTEM_COMMAND_COLLECTION).first(auth)
          auth_resp2.callback do |res|
            if EM::Mongo::Support.ok?(res)
              response.succeed true
            else
              response.fail res
            end
          end
          auth_resp2.errback { |err| response.fail err }
        end
      end
      auth_resp.errback { |err| response.fail err }
      response
    end

    # Adds a user to this database for use with authentication. If the user already
    # exists in the system, the password will be updated.
    #
    # @param [String] username
    # @param [String] password
    #
    # @return [EM::Mongo::RequestResponse] Calls back with an object representing the user.
    def add_user(username, password)
      response = RequestResponse.new
      user_resp = self.collection(SYSTEM_USER_COLLECTION).first({:user => username})
      user_resp.callback do |res|
        user = res || {:user => username}
        user['pwd'] = EM::Mongo::Support.hash_password(username, password)
        response.succeed self.collection(SYSTEM_USER_COLLECTION).save(user)
      end
      user_resp.errback { |err| response.fail err }
      response
    end

  end
end
