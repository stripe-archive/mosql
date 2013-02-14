module MoSQL
  class SQLAdapter
    include MoSQL::Logging

    attr_reader :db

    def initialize(schema, uri, pgschema=nil)
      @schema = schema
      connect_db(uri, pgschema)
    end

    def connect_db(uri, pgschema)
      @db = Sequel.connect(uri, :after_connect => proc do |conn|
                             if pgschema
                               begin
                                 conn.execute("CREATE SCHEMA \"#{pgschema}\"")
                               rescue PG::Error
                               end
                               conn.execute("SET search_path TO \"#{pgschema}\"")
                             end
                           end)
    end

    def table_for_ns(ns)
      @db[@schema.table_for_ns(ns).intern]
    end

    def transform_one_ns(ns, obj)
      h = {}
      cols = @schema.all_columns(@schema.find_ns(ns))
      row  = @schema.transform(ns, obj)
      cols.zip(row).each { |k,v| h[k] = v }
      h
    end

    def upsert_ns(ns, obj)
      h = transform_one_ns(ns, obj)
      upsert(table_for_ns(ns), h)
    end

    # obj must contain an _id field. All other fields will be ignored.
    def delete_ns(ns, obj)
      h = transform_one_ns(ns, obj)
      raise "No _id found in transform of #{obj.inspect}" if h['_id'].nil?
      table_for_ns(ns).where(:_id => h['_id']).delete
    end

    def upsert(table, item)
      begin
        upsert!(table, item)
      rescue Sequel::DatabaseError => e
        wrapped = e.wrapped_exception
        if wrapped.result
          log.warn("Ignoring row (_id=#{item['_id']}): #{e}")
        else
          raise e
        end
      end
    end

    def upsert!(table, item)
      begin
        table.insert(item)
      rescue Sequel::DatabaseError => e
        raise e unless e.message =~ /duplicate key value violates unique constraint/
        table.where(:_id => item['_id']).update(item)
      end
    end
  end
end

