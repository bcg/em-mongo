require 'openssl'
require 'bson'
require 'eventmachine'

require_relative '../support.rb'

module EM::Mongo

  # an RFC 5802 compilant SCRAM(-SHA-1) implementation
  # for MongoDB-Authentication
  #
  # so everything is encapsulated, but the main part (PAYLOAD of messages) is RFC5802 compilant
  class SCRAM < Authentication

      MECHANISM = 'SCRAM-SHA-1'.freeze

      DIGEST = OpenSSL::Digest::SHA1.new.freeze

      CLIENT_FIRST_MESSAGE = { saslStart: 1, autoAuthorize: 1 }.freeze
      CLIENT_FINAL_MESSAGE = CLIENT_EMPTY_MESSAGE = { saslContinue: 1 }.freeze


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
        #TODO Flatten Hierarchies
        @username = username
        @plain_password = password

        gs2_header = 'n,,'
        client_first_bare = "n=#{@username},r=#{client_nonce}"

        client_first = BSON::Binary.new(gs2_header+client_first_bare) # client_first msg
        client_first_msg = CLIENT_FIRST_MESSAGE.merge({PAYLOAD=>client_first, mechanism:MECHANISM})

        client_first_resp = @db.collection(EM::Mongo::Database::SYSTEM_COMMAND_COLLECTION).first(client_first_msg) #TODO extract and make easier to understand (e.g. command(first_msg) or sthg like that)

        #server_first_resp #for flattening

        client_first_resp.callback do |res_first|
          if not is_server_response_valid? res_first
            response.fail "first server response not valid: " + res_first.to_s
          else
            # take the salt & iterations and do the pw-derivation
            server_first = res_first[PAYLOAD].to_s

            conv_id = res_first['conversationId']

            combined_nonce = server_first.match(RNONCE)[1] #r= ...
            salt       =     server_first.match( SALT )[1] #s=... (from server_first)
            iterations = server_first.match(ITERATIONS)[1].to_i #i=...  ..

            if not combined_nonce.start_with?(client_nonce) # combined_nonce should be client_nonce+server_nonce
              response.fail "nonce doesn't start with client_nonce: " + res_first.to_s
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
              client_final_msg = CLIENT_FINAL_MESSAGE.merge({PAYLOAD => client_final, conversationId: conv_id})

              client_final_resp = @db.collection('$cmd').first(client_final_msg)
              client_final_resp.callback do |res_final|
                if not is_server_response_valid? res_final
                  response.fail "Final Server Response not valid " + res_final.to_s
                else
                  server_final = res_final[PAYLOAD].to_s # in RFC this equals server_final
                  verifier = server_final.match(VERIFIER)[1] #r= ...
                  if verifier and verifier_valid? verifier
                    handle_server_end(response,conv_id) # will set the response
                  else
                    response.fail "verifier #{verifier.nil? ? 'not present':'invalid'} #{res_final}"
                  end
                end
              end
            client_final_resp.errback { |err| response.fail err }
            end
          end
        end
        client_first_resp.errback {
            |err| response.fail err }
        return response
      end


      # MongoDB handles the end of authentication different than in RFC 5802
      # it needs at least an additional empty response (this needs to be iterated until res[done]=true 
      #  (at least it is done so in the official mongo-ruby-drive (at least it is done so in the official mongo-ruby-driver))
      #   -> recursion (is technically more loop than recursion but here it's one)
      # 
      # @param response [EM::Mongo::ResponseRequest] to fail or succeed after completion
      # @param conv_id   ConversationId to send to the server on each iteration
    def handle_server_end(response,conv_id) # will set the response
      client_end = BSON::Binary.new('')
      client_end_msg = CLIENT_EMPTY_MESSAGE.merge(PAYLOAD=>client_end, conversationId:conv_id)
      server_end_resp = @db.collection('$cmd').first(client_end_msg)
      
      server_end_resp.errback{|err| response.fail err}
      
      server_end_resp.callback do |res|
        if not is_server_response_valid? res
          response.fail "got invalid response on handling server_end: #{res.nil? ? 'nil' : res}"
        else
         if res['done'] == true || res['done'] == 'true'
           response.succeed true
         else
           handle_server_end(response,conv_id) # try it again
         end
        end
      end
    end
      
      # to be valid the response has to
      #  * be not nil
      #  * contain at least ['done'], ['ok'], ['payload'], ['conversationId']
      #  * ['ok'].to_i has to be 1
      #  * ['conversationId'] has to match the first sent one
      # @param [BSON::OrderedHash] response the response got from server
    def is_server_response_valid?(response)
      if response.nil? then return false; end
      if response['done'].nil?    or
         response['ok'].nil?      or
         response['payload'].nil? or
         response['conversationId'].nil? then
        return false;
      end
      
      if not Support.ok? response then return false; end
      if not @conversationId.nil? and response['conversationId'] != @conversationId
        return false;
      end
      
      true
    end
      
      ## verify the verifier (v=...)
    def verifier_valid?(verifier)
      verifier == server_signature
    end


   ### Building blocks
     # @see http://tools.ietf.org/html/rfc5802#section-2.2

    def hi(password, salt, iterations)
      OpenSSL::PKCS5.pbkdf2_hmac_sha1(
        password,
        Base64.strict_decode64(salt),
        iterations,
        DIGEST.size
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
      @hashed_password ||= Support.hash_password(@username, @plain_password).encode("UTF-8")
    end

    #needs @username, @plain_password, @salt, @iterations defined
    def salted_password
      @salted_password ||= hi(hashed_password, @salt, @iterations)
    end
    
    # @see http://tools.ietf.org/html/rfc5802#section-3
    def client_key 
      @client_key ||= hmac(salted_password,CLIENT_KEY)
    end
      # server_key = hmac(salted_password,"Server Key")
    def server_key
      @server_key ||= hmac(salted_password,SERVER_KEY)
    end

    #needs @username, @plain_password, @salt, @iterations, @auth_message defined
    def client_signature
      @client_signature ||= hmac(DIGEST.digest(client_key), @auth_message)
    end

    # server_signature = B64(hmac(server_key, auth_message)
   def server_signature
     @server_signature ||= Base64.strict_encode64(hmac(server_key, @auth_message))
   end

    class FirstMessage
      include EM::Deferrable

    end
  end
end
