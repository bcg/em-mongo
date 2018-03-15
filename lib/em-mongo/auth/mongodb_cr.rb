require 'openssl'
require_relative '../support.rb'

module EM::Mongo
  class MONGODB_CR < Authentication

    MECHANISM = 'MONGODB-CR'.freeze

    def authenticate(username, password)
      response = RequestResponse.new

      auth_resp = @db.collection(SYSTEM_COMMAND_COLLECTION).first({'getnonce' => 1})
      auth_resp.callback do |res|
        if not res or not res['nonce']
          if res.nil? then response.fail "connection failure"
          else response.fail "invalid first server response: " + res.to_s
          end
        else
          auth                 = BSON::OrderedHash.new
          auth['authenticate'] = 1
          auth['user']         = username
          auth['nonce']        = res['nonce']
          auth['key']          = auth_key(username, password, res['nonce'])

          auth_resp2 = @db.collection(SYSTEM_COMMAND_COLLECTION).first(auth)
          auth_resp2.callback do |res|
            if Support.ok?(res)
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

    # Generate an MD5 for authentication.
    #
    # @param [String] username
    # @param [String] password
    # @param [String] nonce
    #
    # @return [String] a key for db authentication.
    def auth_key(username, password, nonce)
      OpenSSL::Digest::MD5.hexdigest("#{nonce}#{username}#{Support.hash_password(username, password)}")
    end

  end
end
