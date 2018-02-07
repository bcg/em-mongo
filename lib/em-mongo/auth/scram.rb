require 'openssl'
require 'bson'

# an RFC5802 compilant SCRAM(-SHA-1) implementation
# for MongoDB-Authentication
#
# so everything is encapsulated, but the main part (PAYLOAD of messages) is RFC5802 compilant
class SCRAM << Authentication

    MECHANISM = 'SCRAM-SHA-1'.freeze

    DIGEST = OpenSSL::Digest::SHA1.new.freeze

    CLIENT_FIRST_MESSAGE = { saslStart: 1, autoAuthorize: 1 }.freeze
    CLIENT_FINAL_MESSAGE = CLIENT_CONTINUE_MESSAGE = { saslContinue: 1 }.freeze


    CLIENT_KEY = 'Client Key'.freeze
    SERVER_KEY = 'Server Key'.freeze

    RNONCE = /r=([^,]*)/.freeze
    SALT = /s=([^,]*)/.freeze
    ITERATIONS = /i=(\d+)/.freeze
    VERIFIER = /v=([^,]*)/.freeze
    PAYLOAD = 'payload'.freeze

    def initialize(database)
      @db = database
    end
    
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
      
      #TODO look for fail-fast-ness (strange word!?)
      @username = username
      @plain_password = password

      gs2_header = 'n,,'
      client_first_bare = "n=#{@username},r=#{client_nonce}"

      first = BSON::Binary.new(gs2_header+client_first_bare) # client_first msg
      first_msg = CLIENT_FIRST_MESSAGE.merge({PAYLOAD:first, mechanism:MECHANISM})

      client_first_resp = @db.collection(EM::Mongo::Database::SYSTEM_COMMAND_COLLECTION).first(first_msg) #TODO extract and make easier to understand (e.g. command(first_msg) or sthg like that)

      #server_first_resp #for flattening

      client_first_resp.callback do |res|
        if not res or not res[PAYLOAD]
          response.fail res if res #TODO fix to something meaningful
          response.succeed false
        else
          # if payload is not in result fail
          #
          # else take the salt & iterations and do the pw-derivation
          server_first = res[PAYLOAD].to_s

          convId = res['conversationId']

          combined_nonce = server_first.match(RNONCE)[1] #r= ...
          salt       =     server_first.match( SALT )[1] #s=... (from server_first)
          iterations = server_first.match(ITERATIONS)[1].to_i #i=...  ..

          if(!combined_nonce.start_with?(client_nonce)) # combined_nonce should be client_nonce+server_nonce
            response.fail res
          else
            client_final_wo_proof= "c=#{Base64.strict_encode64(gs2_header)},r=#{combined_nonce}" #c='biws'
            auth_message = client_first_bare + ',' + server_first + ',' + client_final_wo_proof

            # proof = clientKey XOR clientSig  ## needs to be sent back
            #
            # ClientSign  = HMAC(StoredKey, AuthMessage)
            # StoredKey = H(ClientKey) ## lt. RFC5802 (needs to be verified against ruby-mongo driver impl)
            # AuthMessage = client_first_bare + ','+server_first+','+client_final_wo_proof

            @salt = salt
            @iterations = iterations
            #client_key = client_key()

            @auth_message = auth_message
            #client_signature = client_signature()

            proof = Base64.strict_encode64(xor(client_key, client_signature))
            client_final = BSON::Binary.new ( client_final_wo_proof + ",p=#{proof}")
            client_final_msg = CLIENT_CONTINUE_MESSAGE.merge({payload: client_final, conversationId: convId})

            server_final_resp = self.collection(SYSTEM_COMMAND_COLLECTION).first(client_final_msg)
            server_final_resp.callback do |res| # TODO Flatten Hierarchies
              ## TODO verify the verifier (v=...)
              #  verifier == server_signature
              # server_signature = B64(hmac(server_key, auth_message))
              # server_key = hmac(salted_password,"Server Key")
              # salted_password = hi(hashed_password)  --> see clientKey impl in support.rb
              if not res or not res['payload']
                response.fail res # TODO  put a more meaningful output than res here (and probably above too)
              else
                verifier = res['payload'].match(VERIFIER)[1] #r= ...
                if verifier
                  #do some veriification HERE

                  #WHILE res['done'] != 1
                  # client_final_msg = CLIENT_FINAL_MESSAGE.merge({payload: BSON::Binary.new(''), conversationId: convId})
                  # server_resp = self.collection(System_command_Collection).first(client_final_mesg)
                  #  \-> repeat here until sucess
                  response.succeed true
                else response.fail res

              end
            end
          end
          client_final_resp.errback { |err| response.fail err }
        end
      end
      client_first_resp.errback {
          |err| response.fail err }
      return response
   end


 ### Building blocks
   # @see http://tools.ietf.org/html/rfc5802#section-2.2

  def hi(password, salt, iterations)
    OpenSSL::PKCS5.pbkdf2_hmac_sha1(
      password,
      Base64.strict_decode64(salt),
      iterations,
      digest.size
     )
  end

  def hmac(data,key)
    OpenSSL::HMAC.digest(DIGEST, data, key)
  end

  # xor for strings
  def xor(first, second)
    first.bytes
      .zip(second.bytes)
      .map{|(x,y)| (x ^ y).chr}
      .join('')
  end


  def client_nonce
      @client_nonce ||= SecureRandom.base64
  end

  # needs @username, @plain_password defined
  def hashed_password
    @hashed_password ||= OpenSSL::Digest::MD5.hexdigest("#{@username}:mongo:#{@plain_password}").encode("UTF-8")
  end

  #needs @username, @plain_password, @salt, @iterations defined
  def salted_password
    @salted_password ||= hi(hashed_password, @salt, @iterations)
  end
  
  # @see http://tools.ietf.org/html/rfc5802#section-3
  def client_key 
    @client_key ||= hmac(salted_password,CLIENT_KEY)
    return @client_key
  end

  #needs @username, @plain_password, @salt, @iterations, @auth_message defined
  def client_signature
    @client_signature || = hmac(DIGEST.digest(client_key), @auth_message)
  end


  class FirstMessage
    include EM::Deferrable

    #def initialize(
  end
end

