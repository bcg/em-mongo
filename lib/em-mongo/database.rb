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

    def authenticate(username, password)
      self.collection(SYSTEM_COMMAND_COLLECTION).first({'getnonce' => 1}) do |res|
        yield false if not res or not res['nonce']

        auth                 = BSON::OrderedHash.new
        auth['authenticate'] = 1
        auth['user']         = username
        auth['nonce']        = res['nonce']   
        auth['key']          = Mongo::Support.auth_key(username, password, res['nonce'])

        self.collection(SYSTEM_COMMAND_COLLECTION).first(auth) do |res|
          if Mongo::Support.ok?(res)
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
        user['pwd'] = Mongo::Support.hash_password(username, password)
        yield self.collection(SYSTEM_USER_COLLECTION).save(user)
      end
    end

  end
end
