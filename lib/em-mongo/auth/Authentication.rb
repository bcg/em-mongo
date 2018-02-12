require 'eventmachine'

# interface for all possible authentications
module EM::Mongo
  class Authentication
    include EM::Deferrable

    SYSTEM_COMMAND_COLLECTION = '$cmd'

    # supported AuthMethods (TODO make instantiation (in database.authenticate) dynamic)
      module AuthMethod
        SCRAM_SHA1 = :scram_sha1
        MONGODB_CR = :mongodb_cr
      end

    def initialize(database)
      @db = database
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
      r=DefaultDeferrable.new #stub implementation
      r.fail "not implemented, use a subclass instead"
      return r
    end
  end
end
