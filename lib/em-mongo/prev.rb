module EM
  module Mongo
    class Collection

      alias :new_find :find
      def find(selector={}, opts={}, &blk)
        raise "find requires a block" if not block_given?

        new_find(selector, opts).to_a.callback do |docs|
          blk.call(docs)
        end
      end

      def first(selector={}, opts={}, &blk)
        opts[:limit] = 1
        find(selector, opts) do |res|
          yield res.first
        end
      end
    end

    class Connection

      def insert(collection_name, documents)
        db_name, col_name = db_and_col_name(collection_name)
        db(db_name).collection(col_name).insert(documents)
      end

      def update(collection_name, selector, document, options={})
        db_name, col_name = db_and_col_name(collection_name)
        db(db_name).collection(col_name).update(selector, document, options)
      end

      def delete(collection_name, selector)
        db_name, col_name = db_and_col_name(collection_name)
        db(db_name).collection(col_name).remove(selector)
      end

      def find(collection_name, skip, limit, order, query, fields, &blk)
        db_name, col_name = db_and_col_name(collection_name)
        db(db_name).collection(col_name).find(query, :skip=>skip,:limit=>limit,:order=>order,:fields=>fields).to_a.callback do |docs|
          yield docs if block_given?
        end
      end

      def db_and_col_name(full_name)
        parts = full_name.split(".")
        [ parts.shift, parts.join(".") ]
      end

    end
  end
end