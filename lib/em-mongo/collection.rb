module EM::Mongo
  class Collection
    attr_accessor :connection

    # Initialize a collection object.
    #
    # @param [String, Symbol] db the name of the database to which this collection belongs.
    # @param [String, Symbol] ns the name of the collection
    # @param [Connection] connection the EM::Mongo::Connection that will service this collection
    #
    # @return [Collection]
    def initialize(db, ns, connection = nil)
      @db = db || "db"
      @ns = ns || "ns"
      @name = [@db,@ns].join('.')
      @connection = connection || EM::Mongo::Connection.new
    end

    # The database that this collection belongs to
    # @return [EM::Mongo::Database]
    def db
      connection.db(@db)
    end

    #The name of this collection
    # @return [String]
    def name
      @ns
    end

    # Return a sub-collection of this collection by name. If 'users' is a collection, then
    # 'users.comments' is a sub-collection of users.
    #
    # @param [String, Symbol] name
    #   the collection to return
    #
    # @return [Collection]
    #   the specified sub-collection
    def [](name)
      name = "#{self.name}.#{name}"
      db.collection(name)
    end

    # Query the database.
    #
    # The +selector+ argument is a prototype document that all results must
    # match. For example:
    #
    #   collection.find({"hello" => "world"})
    #
    # only matches documents that have a key "hello" with value "world".
    # Matches can have other keys *in addition* to "hello".
    #
    # @return [EM::Mongo::Cursor]
    #   a cursor over the results of the query
    #
    # @param [Hash] selector
    #   a document specifying elements which must be present for a
    #   document to be included in the result set. Note that in rare cases,
    #   (e.g., with $near queries), the order of keys will matter. To preserve
    #   key order on a selector, use an instance of BSON::OrderedHash (only applies
    #   to Ruby 1.8).
    #
    # @option opts [Array, Hash] :fields field names that should be returned in the result
    #   set ("_id" will be included unless explicity excluded). By limiting results to a certain subset of fields,
    #   you can cut down on network traffic and decoding time. If using a Hash, keys should be field
    #   names and values should be either 1 or 0, depending on whether you want to include or exclude
    #   the given field.
    # @option opts [Integer] :skip number of documents to skip from the beginning of the result set
    # @option opts [Integer] :limit maximum number of documents to return
    # @option opts [Array]   :sort an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [String, Array, OrderedHash] :hint hint for query optimizer, usually not necessary if
    #   using MongoDB > 1.1
    # @option opts [Boolean] :snapshot (false) if true, snapshot mode will be used for this query.
    #   Snapshot mode assures no duplicates are returned, or objects missed, which were preset at both the start and
    #   end of the query's execution.
    #   For details see http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # @option opts [Boolean] :batch_size (100) the number of documents to returned by the database per
    #   GETMORE operation. A value of 0 will let the database server decide how many results to returns.
    #   This option can be ignored for most use cases.
    # @option opts [Boolean] :timeout (true) when +true+, the returned cursor will be subject to
    #   the normal cursor timeout behavior of the mongod process. Disabling the timeout is not supported by em-mongo
    # @option opts [Integer] :max_scan (nil) Limit the number of items to scan on both collection scans and indexed queries..
    # @option opts [Boolean] :show_disk_loc (false) Return the disk location of each query result (for debugging).
    # @option opts [Boolean] :return_key (false) Return the index key used to obtain the result (for debugging).
    # @option opts [Block] :transformer (nil) a block for tranforming returned documents.
    #   This is normally used by object mappers to convert each returned document to an instance of a class.
    #
    # @raise [ArgumentError]
    #   if timeout is set to false
    #
    # @raise [RuntimeError]
    #   if given unknown options
    #
    # @core find find-instance_method
    def find(selector={}, opts={})
      opts   = opts.dup
      fields = opts.delete(:fields)
      fields = ["_id"] if fields && fields.empty?
      skip   = opts.delete(:skip) || skip || 0
      limit  = opts.delete(:limit) || 0
      sort   = opts.delete(:sort) || opts.delete(:order)
      hint   = opts.delete(:hint)
      snapshot   = opts.delete(:snapshot)
      batch_size = opts.delete(:batch_size)
      timeout    = (opts.delete(:timeout) == false) ? false : true
      max_scan   = opts.delete(:max_scan)
      return_key = opts.delete(:return_key)
      transformer = opts.delete(:transformer)
      show_disk_loc = opts.delete(:max_scan)

      if timeout == false
        raise ArgumentError, "EM::Mongo::Collection#find does not support disabling the timeout"
      end

      if hint
        hint = normalize_hint_fields(hint)
      end

      raise RuntimeError, "Unknown options [#{opts.inspect}]" unless opts.empty?

      EM::Mongo::Cursor.new(self, {
        :selector    => selector,
        :fields      => fields,
        :skip        => skip,
        :limit       => limit,
        :order       => sort,
        :hint        => hint,
        :snapshot    => snapshot,
        :timeout     => timeout,
        :batch_size  => batch_size,
        :transformer => transformer,
        :max_scan    => max_scan,
        :show_disk_loc => show_disk_loc,
        :return_key    => return_key
      })
    end

    # Return a single object from the database.
    #
    # @return [EM::Mongo::RequestResponse]
    #   calls back with a single document or nil if no result is found.
    #
    # @param [Hash, ObjectId, Nil] spec_or_object_id a hash specifying elements
    #   which must be present for a document to be included in the result set or an
    #   instance of ObjectId to be used as the value for an _id query.
    #   If nil, an empty selector, {}, will be used.
    #
    # @option opts [Hash]
    #   any valid options that can be send to Collection#find
    #
    # @raise [TypeError]
    #   if the argument is of an improper type.
    def find_one(spec_or_object_id=nil, opts={})
      spec = case spec_or_object_id
             when nil
               {}
             when BSON::ObjectId
               {:_id => spec_or_object_id}
             when Hash
               spec_or_object_id
             else
               raise TypeError, "spec_or_object_id must be an instance of ObjectId or Hash, or nil"
             end
      find(spec, opts.merge(:limit => -1)).next_document
    end
    alias :first :find_one

    # Insert one or more documents into the collection.
    #
    # @param [Hash, Array] doc_or_docs
    #   a document (as a hash) or array of documents to be inserted.
    #
    # @return [ObjectId, Array]
    #   The _id of the inserted document or a list of _ids of all inserted documents.
    #
    # @see DB#remove for options that can be passed to :safe.
    #
    # @core insert insert-instance_method
    def insert(doc_or_docs)
      safe_insert(doc_or_docs, :safe => false).data
    end
    alias_method :<<, :insert

    # Insert one or more documents into the collection, with a failure if the operation doesn't succeed
    # Unlike insert, this method returns a deferrable
    #
    # @param [Hash, Array] doc_or_docs
    #   a document (as a hash) or array of documents to be inserted.
    #
    # @return [EM::Mongo::RequestResponse]
    #   Calls backw ith the _id of the inserted document or a list of _ids of all inserted documents.
    #
    # @option opts [Boolean, Hash] :safe (+true+)
    #   run the operation in safe mode, which run a getlasterror command on the
    #   database to report any assertion. In addition, a hash can be provided to
    #   run an fsync and/or wait for replication of the insert (>= 1.5.1). Safe
    #   options provided here will override any safe options set on this collection,
    #   its database object, or the current connection. See the options on
    #   for DB#get_last_error.
    #
    # @see DB#remove for options that can be passed to :safe.
    #
    # @core insert insert-instance_method
    def safe_insert(doc_or_docs, safe_opts = {})
      response = RequestResponse.new
      safe_opts[:safe] = true unless safe_opts[:safe] == false
      doc_or_docs = [doc_or_docs] unless doc_or_docs.is_a?(Array)
      doc_or_docs.map! { |doc| sanitize_id!(doc) }
      insert_resp = insert_documents(doc_or_docs, @ns, true, safe_opts)
      insert_resp.callback do |ids|
        ids.length > 1 ? response.succeed(ids) : response.succeed(ids[0])
      end
      insert_resp.errback do |err|
        response.fail err
      end
      response
    end

    # Update one or more documents in this collection.
    #
    # @param [Hash] selector
    #   a hash specifying elements which must be present for a document to be updated. Note:
    #   the update command currently updates only the first document matching the
    #   given selector. If you want all matching documents to be updated, be sure
    #   to specify :multi => true.
    # @param [Hash] document
    #   a hash specifying the fields to be changed in the selected document,
    #   or (in the case of an upsert) the document to be inserted
    #
    # @option opts [Boolean] :upsert (+false+) if true, performs an upsert (update or insert)
    # @option opts [Boolean] :multi (+false+) update all documents matching the selector, as opposed to
    #   just the first matching document. Note: only works in MongoDB 1.1.3 or later.
    #
    # @return [Hash, true] Returns a Hash containing the last error object if running in safe mode.
    #   Otherwise, returns true.
    #
    # @core update update-instance_method
    def update(selector, document, opts={})
      # Initial byte is 0.
      safe_update(selector, document, opts.merge(:safe => false)).data
    end

     # Update one or more documents in this collection.
    #
    # @param [Hash] selector
    #   a hash specifying elements which must be present for a document to be updated. Note:
    #   the update command currently updates only the first document matching the
    #   given selector. If you want all matching documents to be updated, be sure
    #   to specify :multi => true.
    # @param [EM::Mongo::RequestResponse] document
    #   calls back with a hash specifying the fields to be changed in the selected document,
    #   or (in the case of an upsert) the document to be inserted
    #
    # @option opts [Boolean] :upsert (+false+) if true, performs an upsert (update or insert)
    # @option opts [Boolean] :multi (+false+) update all documents matching the selector, as opposed to
    #   just the first matching document. Note: only works in MongoDB 1.1.3 or later.
    # @option opts [Boolean] :safe (+true+)
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database. Safe options provided here will override any safe
    #   options set on this collection, its database object, or the current collection.
    #   See the options for DB#get_last_error for details.
    #
    # @return [Hash, true] Returns a Hash containing the last error object if running in safe mode.
    #   Otherwise, returns true.
    #
    # @core update update-instance_method
    def safe_update(selector, document, opts={})
      response = RequestResponse.new
      opts = opts.dup
      opts[:safe] = true unless opts[:safe] == false
      # Initial byte is 0.
      message = BSON::ByteBuffer.new("\0\0\0\0")
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db}.#{@ns}")
      update_options  = 0
      update_options += 1 if opts.delete(:upsert)
      update_options += 2 if opts.delete(:multi)
      message.put_int(update_options)
      message.put_binary(BSON::BSON_CODER.serialize(selector, false, true).to_s)
      message.put_binary(BSON::BSON_CODER.serialize(document, false, true).to_s)

      if opts[:safe]
        send_resp = safe_send(EM::Mongo::OP_UPDATE, message, true, opts)
        send_resp.callback { response.succeed(true) }
        send_resp.errback { |err| response.fail(err) }
      else
        @connection.send_command(EM::Mongo::OP_UPDATE, message)
        response.succeed(true)
      end
      response
    end

    # Save a document to this collection.
    #
    # @param [Hash] doc
    #   the document to be saved. If the document already has an '_id' key,
    #   then an update (upsert) operation will be performed, and any existing
    #   document with that _id is overwritten. Otherwise an insert operation is performed.
    #
    # @return [ObjectId] the _id of the saved document.
    #
    def save(doc, opts={})
      safe_save(doc, opts.merge(:safe => false)).data
    end

     # Save a document to this collection.
    #
    # @param [Hash] doc
    #   the document to be saved. If the document already has an '_id' key,
    #   then an update (upsert) operation will be performed, and any existing
    #   document with that _id is overwritten. Otherwise an insert operation is performed.
    #
    # @return [EM::Mongo::RequestResponse] Calls backw with the _id of the saved document.
    #
    # @option opts [Boolean, Hash] :safe (+true+)
    #   run the operation in safe mode, which run a getlasterror command on the
    #   database to report any assertion. In addition, a hash can be provided to
    #   run an fsync and/or wait for replication of the save (>= 1.5.1). See the options
    #   for DB#error.
    #
    def safe_save(doc, opts={})
      opts[:safe] = true unless opts[:safe] == false
      id = has_id?(doc)
      sanitize_id!(doc)
      if id
        safe_update({:_id => id}, doc, opts.merge(:upsert => true))
      else
        safe_insert(doc, opts)
      end
    end

    # Remove all documents from this collection.
    #
    # @param [Hash] selector
    #   If specified, only matching documents will be removed.
    #
    # @option opts [Boolean, Hash] :safe (+false+)
    #   run the operation in safe mode, which will run a getlasterror command on the
    #   database to report any assertion. In addition, a hash can be provided to
    #   run an fsync and/or wait for replication of the remove (>= 1.5.1). Safe
    #   options provided here will override any safe options set on this collection,
    #   its database, or the current connection. See the options for DB#get_last_error for more details.
    #
    # @example remove all documents from the 'users' collection:
    #   users.remove
    #   users.remove({})
    #
    # @example remove only documents that have expired:
    #   users.remove({:expire => {"$lte" => Time.now}})
    #
    # @return [true] Returns true.
    #
    # @see DB#remove for options that can be passed to :safe.
    #
    # @core remove remove-instance_method
    def remove(selector={}, opts={})
      # Initial byte is 0.
      message = BSON::ByteBuffer.new("\0\0\0\0")
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db}.#{@ns}")
      message.put_int(0)
      message.put_binary(BSON::BSON_CODER.serialize(selector, false, true).to_s)
      @connection.send_command(EM::Mongo::OP_DELETE, message)
      true
    end

    # Drop the entire collection. USE WITH CAUTION.
    def drop
      db.drop_collection(@ns)
    end

    # Atomically update and return a document using MongoDB's findAndModify command. (MongoDB > 1.3.0)
    #
    # @option opts [Hash] :query ({}) a query selector document for matching the desired document.
    # @option opts [Hash] :update (nil) the update operation to perform on the matched document.
    # @option opts [Array, String, OrderedHash] :sort ({}) specify a sort option for the query using any
    #   of the sort options available for Cursor#sort. Sort order is important if the query will be matching
    #   multiple documents since only the first matching document will be updated and returned.
    # @option opts [Boolean] :remove (false) If true, removes the the returned document from the collection.
    # @option opts [Boolean] :new (false) If true, returns the updated document; otherwise, returns the document
    #   prior to update.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with the matched document.
    #
    # @core findandmodify find_and_modify-instance_method
    def find_and_modify(opts={})
      response = RequestResponse.new
      cmd = BSON::OrderedHash.new
      cmd[:findandmodify] = @ns
      cmd.merge!(opts)
      cmd[:sort] = EM::Mongo::Support.format_order_clause(opts[:sort]) if opts[:sort]

      cmd_resp = db.command(cmd)
      cmd_resp.callback do |doc|
        response.succeed doc['value']
      end
      cmd_resp.errback do |err|
        response.fail err
      end
      response
    end

    # Perform a map-reduce operation on the current collection.
    #
    # @param [String, BSON::Code] map a map function, written in JavaScript.
    # @param [String, BSON::Code] reduce a reduce function, written in JavaScript.
    #
    # @option opts [Hash] :query ({}) a query selector document, like what's passed to #find, to limit
    #   the operation to a subset of the collection.
    # @option opts [Array] :sort ([]) an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [Integer] :limit (nil) if passing a query, number of objects to return from the collection.
    # @option opts [String, BSON::Code] :finalize (nil) a javascript function to apply to the result set after the
    #   map/reduce operation has finished.
    # @option opts [String] :out (nil) a valid output type. In versions of MongoDB prior to v1.7.6,
    #   this option takes the name of a collection for the output results. In versions 1.7.6 and later,
    #   this option specifies the output type. See the core docs for available output types.
    # @option opts [Boolean] :keeptemp (false) if true, the generated collection will be persisted. The defualt
    #   is false. Note that this option has no effect is versions of MongoDB > v1.7.6.
    # @option opts [Boolean ] :verbose (false) if true, provides statistics on job execution time.
    # @option opts [Boolean] :raw (false) if true, return the raw result object from the map_reduce command, and not
    #   the instantiated collection that's returned by default. Note if a collection name isn't returned in the
    #   map-reduce output (as, for example, when using :out => {:inline => 1}), then you must specify this option
    #   or an ArgumentError will be raised.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with a EM::Mongo::Collection object or a Hash with the map-reduce command's results.
    #
    # @raise ArgumentError if you specify {:out => {:inline => true}} but don't specify :raw => true.
    #
    # @see http://www.mongodb.org/display/DOCS/MapReduce Offical MongoDB map/reduce documentation.
    #
    # @core mapreduce map_reduce-instance_method
    def map_reduce(map, reduce, opts={})
      response = RequestResponse.new
      map    = BSON::Code.new(map) unless map.is_a?(BSON::Code)
      reduce = BSON::Code.new(reduce) unless reduce.is_a?(BSON::Code)
      raw    = opts.delete(:raw)

      hash = BSON::OrderedHash.new
      hash['mapreduce'] = @ns
      hash['map'] = map
      hash['reduce'] = reduce
      hash.merge! opts

      cmd_resp = db.command(hash)
      cmd_resp.callback do |result|
        if EM::Mongo::Support.ok?(result) == false
          response.fail [Mongo::OperationFailure, "map-reduce failed: #{result['errmsg']}"]
        elsif raw
          response.succeed result
        elsif result["result"]
          response.succeed db.collection(result["result"])
        else
          response.fail [ArgumentError, "Could not instantiate collection from result. If you specified " +
            "{:out => {:inline => true}}, then you must also specify :raw => true to get the results."]
        end
      end
      cmd_resp.errback do |err|
        response.fail(err)
      end
      response
    end
    alias :mapreduce :map_reduce

    # Return a list of distinct values for +key+ across all
    # documents in the collection. The key may use dot notation
    # to reach into an embedded object.
    #
    # @param [String, Symbol, OrderedHash] key or hash to group by.
    # @param [Hash] query a selector for limiting the result set over which to group.
    #
    # @example Saving zip codes and ages and returning distinct results.
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 94108, :name => {:age => 24}})
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 99701, :name => {:age => 24}})
    #   @collection.save({:zip => 94108, :name => {:age => 27}})
    #
    #   @collection.distinct(:zip)
    #     [10010, 94108, 99701]
    #   @collection.distinct("name.age")
    #     [27, 24]
    #
    #   # You may also pass a document selector as the second parameter
    #   # to limit the documents over which distinct is run:
    #   @collection.distinct("name.age", {"name.age" => {"$gt" => 24}})
    #     [27]
    #
    # @return [EM::Mongo::RequestResponse] Calls back with an array of distinct values.
    def distinct(key, query=nil)
      raise MongoArgumentError unless [String, Symbol].include?(key.class)
      response = RequestResponse.new
      command = BSON::OrderedHash.new
      command[:distinct] = @ns
      command[:key]      = key.to_s
      command[:query]    = query

      cmd_resp = db.command(command)
      cmd_resp.callback do |resp|
        response.succeed resp["values"]
      end
      cmd_resp.errback do |err|
        response.fail err
      end
      response
    end

    # Perform a group aggregation.
    #
    # @param [Hash] opts the options for this group operation. The minimum required are :initial
    #   and :reduce.
    #
    # @option opts [Array, String, Symbol] :key (nil) Either the name of a field or a list of fields to group by (optional).
    # @option opts [String, BSON::Code] :keyf (nil) A JavaScript function to be used to generate the grouping keys (optional).
    # @option opts [String, BSON::Code] :cond ({}) A document specifying a query for filtering the documents over
    #   which the aggregation is run (optional).
    # @option opts [Hash] :initial the initial value of the aggregation counter object (required).
    # @option opts [String, BSON::Code] :reduce (nil) a JavaScript aggregation function (required).
    # @option opts [String, BSON::Code] :finalize (nil) a JavaScript function that receives and modifies
    #   each of the resultant grouped objects. Available only when group is run with command
    #   set to true.
    #
    # @return [EM::Mongo::RequestResponse] calls back with the command response consisting of grouped items.
    def group(opts={})
      response = RequestResponse.new
      reduce   =  opts[:reduce]
      finalize =  opts[:finalize]
      cond     =  opts.fetch(:cond, {})
      initial  =  opts[:initial]

      if !(reduce && initial)
        raise MongoArgumentError, "Group requires at minimum values for initial and reduce."
      end

      cmd = {
        "group" => {
          "ns"      => @ns,
          "$reduce" => reduce.to_bson_code,
          "cond"    => cond,
          "initial" => initial
        }
      }

      if finalize
        cmd['group']['finalize'] = finalize.to_bson_code
      end

      if key = opts[:key]
        if key.is_a?(String) || key.is_a?(Symbol)
          key = [key]
        end
        key_value = {}
        key.each { |k| key_value[k] = 1 }
        cmd["group"]["key"] = key_value
      elsif keyf = opts[:keyf]
        cmd["group"]["$keyf"] = keyf.to_bson_code
      end

      cmd_resp = db.command(cmd)
      cmd_resp.callback do |result|
        response.succeed result["retval"]
      end
      cmd_resp.errback do |err|
        response.fail err
      end
      response
    end


    # Get the number of documents in this collection.
    #
    # @return [EM::Mongo::RequestResponse]
    def count
      find().count
    end
    alias :size :count

    # Return stats on the collection. Uses MongoDB's collstats command.
    #
    # @return [EM::Mongo::RequestResponse]
    def stats
      @db.command({:collstats => @name})
    end

    # Get information on the indexes for this collection.
    #
    # @return [EM::Mongo::RequestResponse] Calls back with a hash where the keys are index names.
    #
    # @core indexes
    def index_information
      db.index_information(@ns)
    end

     # Create a new index.
    #
    # @param [String, Array] spec
    #   should be either a single field name or an array of
    #   [field name, direction] pairs. Directions should be specified
    #   as Mongo::ASCENDING, Mongo::DESCENDING, or Mongo::GEO2D.
    #
    #   Note that geospatial indexing only works with versions of MongoDB >= 1.3.3+. Keep in mind, too,
    #   that in order to geo-index a given field, that field must reference either an array or a sub-object
    #   where the first two values represent x- and y-coordinates. Examples can be seen below.
    #
    #   Also note that it is permissible to create compound indexes that include a geospatial index as
    #   long as the geospatial index comes first.
    #
    #   If your code calls create_index frequently, you can use Collection#ensure_index to cache these calls
    #   and thereby prevent excessive round trips to the database.
    #
    # @option opts [Boolean] :unique (false) if true, this index will enforce a uniqueness constraint.
    # @option opts [Boolean] :background (false) indicate that the index should be built in the background. This
    #   feature is only available in MongoDB >= 1.3.2.
    # @option opts [Boolean] :drop_dups (nil) If creating a unique index on a collection with pre-existing records,
    #   this option will keep the first document the database indexes and drop all subsequent with duplicate values.
    # @option opts [Integer] :min (nil) specify the minimum longitude and latitude for a geo index.
    # @option opts [Integer] :max (nil) specify the maximum longitude and latitude for a geo index.
    #
    # @example Creating a compound index:
    #   @posts.create_index([['subject', Mongo::ASCENDING], ['created_at', Mongo::DESCENDING]])
    #
    # @example Creating a geospatial index:
    #   @restaurants.create_index([['location', Mongo::GEO2D]])
    #
    #   # Note that this will work only if 'location' represents x,y coordinates:
    #   {'location': [0, 50]}
    #   {'location': {'x' => 0, 'y' => 50}}
    #   {'location': {'latitude' => 0, 'longitude' => 50}}
    #
    # @example A geospatial index with alternate longitude and latitude:
    #   @restaurants.create_index([['location', Mongo::GEO2D]], :min => 500, :max => 500)
    #
    # @return [String] the name of the index created.
    #
    # @core indexes create_index-instance_method
    def create_index(spec, opts={})
      field_spec = parse_index_spec(spec)
      opts = opts.dup
      name = opts.delete(:name) || generate_index_name(field_spec)
      name = name.to_s if name

      generate_indexes(field_spec, name, opts)
      name
    end

    # Drop a specified index.
    #
    # @param [EM::Mongo::RequestResponse] name
    #
    # @core indexes
    def drop_index(name)
      if name.is_a?(Array)
        response = RequestResponse.new
        name_resp = index_name(name)
        name_resp.callback do |name|
          drop_resp = db.drop_index(@ns, name)
          drop_resp.callback { response.succeed }
          drop_resp.errback { |err| response.fail(err) }
        end
        name_resp.errback { |err| response.fail(err) }
        response
      else
        db.drop_index(@ns, name)
      end
    end

    # Drop all indexes.
    #
    # @core indexes
    def drop_indexes
      # Note: calling drop_indexes with no args will drop them all.
      db.drop_index(@ns, '*')
    end

    protected

    def normalize_hint_fields(hint)
      case hint
      when String
        {hint => 1}
      when Hash
        hint
      when nil
        nil
      else
        h = BSON::OrderedHash.new
        hint.to_a.each { |k| h[k] = 1 }
        h
      end
    end

    private

    def has_id?(doc)
      # mongo-ruby-driver seems to take :_id over '_id' for some reason
      id = doc[:_id] || doc['_id']
      return id if id
      nil
    end

    def sanitize_id!(doc)
      doc[:_id] = has_id?(doc) || BSON::ObjectId.new
      doc.delete('_id')
      doc
    end

    # Sends a Mongo::Constants::OP_INSERT message to the database.
    # Takes an array of +documents+, an optional +collection_name+, and a
    # +check_keys+ setting.
    def insert_documents(documents, collection_name=@name, check_keys = true, safe_options={})
      response = RequestResponse.new
      # Initial byte is 0.
      message = BSON::ByteBuffer.new("\0\0\0\0")
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db}.#{collection_name}")
      documents.each do |doc|
        message.put_binary(BSON::BSON_CODER.serialize(doc, check_keys, true).to_s)
      end
      raise InvalidOperation, "Exceded maximum insert size of 16,000,000 bytes" if message.size > 16_000_000

      ids = documents.collect { |o| o[:_id] || o['_id'] }

      if safe_options[:safe]
        send_resp = safe_send(EM::Mongo::OP_INSERT, message, ids, safe_options)
        send_resp.callback { response.succeed(ids) }
        send_resp.errback { |err| response.fail(err) }
      else
        @connection.send_command(EM::Mongo::OP_INSERT, message)
        response.succeed(ids)
      end
      response
    end

    def safe_send(op, message, return_val, options={})
      response = RequestResponse.new
      options[:safe] = true
      options[:db_name] = @db
      @connection.send_command(op, message, options)  do |server_resp|
        docs = server_resp.docs
        if server_resp.number_returned == 1 && (error = docs[0]['err'] || docs[0]['errmsg'])
          @connection.close if error == "not master"
          error = "wtimeout" if error == "timeout"
          response.fail [EM::Mongo::OperationFailure, "#{docs[0]['code']}: #{error}"]
        else
          response.succeed(return_val)
        end
      end
      response
    end

    def index_name(spec)
      response = RequestResponse.new
      field_spec = parse_index_spec(spec)
      info_resp = index_information
      info_resp.callback do |indexes|
        found = indexes.values.find do |index|
          index['key'] == field_spec
        end
        response.succeed( found ? found['name'] : nil )
      end
      info_resp.errback do |err|
        response.fail err
      end
      response
    end

    def parse_index_spec(spec)
      field_spec = BSON::OrderedHash.new
      if spec.is_a?(String) || spec.is_a?(Symbol)
        field_spec[spec.to_s] = 1
      elsif spec.is_a?(Array) && spec.all? {|field| field.is_a?(Array) }
        spec.each do |f|
          if [EM::Mongo::ASCENDING, EM::Mongo::DESCENDING, EM::Mongo::GEO2D].include?(f[1])
            field_spec[f[0].to_s] = f[1]
          else
            raise MongoArgumentError, "Invalid index field #{f[1].inspect}; " +
              "should be one of Mongo::ASCENDING (1), Mongo::DESCENDING (-1) or Mongo::GEO2D ('2d')."
          end
        end
      else
        raise MongoArgumentError, "Invalid index specification #{spec.inspect}; " +
          "should be either a string, symbol, or an array of arrays."
      end
      field_spec
    end

    def generate_indexes(field_spec, name, opts)
      selector = {
        :name   => name,
        :ns     => "#{@db}.#{@ns}",
        :key    => field_spec
      }
      selector.merge!(opts)
      insert_documents([selector], EM::Mongo::Database::SYSTEM_INDEX_COLLECTION, false)
    end

    def generate_index_name(spec)
      indexes = []
      spec.each_pair do |field, direction|
        indexes.push("#{field}_#{direction}")
      end
      indexes.join("_")
    end

  end
end
