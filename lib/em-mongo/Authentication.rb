require 'eventmachine'

# interface for all possible authentications
class Authentication
  include EM::Deferrable

  # supported AuthMethods (TODO make instantiation (in database.authenticate) dynamic)
    module AuthMethod
      SCRAM_SHA1 = :scram_sha1
      MONGODB_CR = :mongocr
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
  def authenticate(user, password)
    return DefaultDeferrable.new #stub implemetation
