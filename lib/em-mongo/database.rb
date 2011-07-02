module EM::Mongo
  class Database

    SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
    SYSTEM_INDEX_COLLECTION = "system.indexes"
    SYSTEM_PROFILE_COLLECTION = "system.profile"
    SYSTEM_USER_COLLECTION = "system.users"
    SYSTEM_JS_COLLECTION = "system.js"
    SYSTEM_COMMAND_COLLECTION = "$cmd"

    def initialize(name = DEFAULT_DB, connection = nil)
      @db_name = name
      @em_connection = connection || EM::Mongo::Connection.new
      @collection = nil
      @collections = {}
    end

    def collection(name = EM::Mongo::DEFAULT_NS)
      @collections[name] ||= EM::Mongo::Collection.new(@db_name, name, @em_connection)
    end

    def connection
      @em_connection
    end

    def close
      @em_connection.close
    end

    def name
      @db_name
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
    # @return [EM::Deferrable]
    #
    # @core commands command_instance-method
    def command(selector, opts={})
      check_response = opts.fetch(:check_response, true)
      raise MongoArgumentError, "command must be given a selector" unless selector.is_a?(Hash) && !selector.empty?
      
      if selector.keys.length > 1 && RUBY_VERSION < '1.9' && selector.class != BSON::OrderedHash
        raise MongoArgumentError, "DB#command requires an OrderedHash when hash contains multiple keys"
      end

      response = EM::DefaultDeferrable.new
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

    def authenticate(username, password)
      self.collection(SYSTEM_COMMAND_COLLECTION).first({'getnonce' => 1}) do |res|
        yield false if not res or not res['nonce']

        auth                 = BSON::OrderedHash.new
        auth['authenticate'] = 1
        auth['user']         = username
        auth['nonce']        = res['nonce']   
        auth['key']          = EM::Mongo::Support.auth_key(username, password, res['nonce'])

        self.collection(SYSTEM_COMMAND_COLLECTION).first(auth) do |res|
          if EM::Mongo::Support.ok?(res)
            yield true
          else
            yield res
          end
        end
      end
    end

    def add_user(username, password, &blk)
      self.collection(SYSTEM_USER_COLLECTION).first({:user => username}) do |res|
        user = res || {:user => username}
        user['pwd'] = EM::Mongo::Support.hash_password(username, password)
        yield self.collection(SYSTEM_USER_COLLECTION).save(user)
      end
    end

  end
end
