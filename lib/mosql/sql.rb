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
      upsert(table_for_ns(ns), @schema.primary_sql_key_for_ns(ns), h)
    end

    # obj must contain an _id field. All other fields will be ignored.
    def delete_ns(ns, obj)
      primary_sql_key = @schema.primary_sql_key_for_ns(ns)
      h = transform_one_ns(ns, obj)
      raise "No #{primary_sql_key} found in transform of #{obj.inspect}" if h[primary_sql_key].nil?
      table_for_ns(ns).where(primary_sql_key.to_sym => h[primary_sql_key]).delete
    end

    def upsert(table, table_primary_key, item)
      begin
        upsert!(table, table_primary_key, item)
      rescue Sequel::DatabaseError => e
        wrapped = e.wrapped_exception
        if wrapped.result
          log.warn("Ignoring row (#{table_primary_key}=#{item[table_primary_key]}): #{e}")
        else
          raise e
        end
      end
    end

    def upsert!(table, table_primary_key, item)
      rows = table.where(table_primary_key.to_sym => item[table_primary_key]).update(item)
      if rows == 0
        begin
          table.insert(item)
        rescue Sequel::DatabaseError => e
          raise e unless self.class.duplicate_key_error?(e)
          log.info("RACE during upsert: Upserting #{item} into #{table}: #{e}")
        end
      elsif rows > 1
        log.warn("Huh? Updated #{rows} > 1 rows: upsert(#{table}, #{item})")
      end
    end

    def self.duplicate_key_error?(e)
      # c.f. http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
      # for the list of error codes.
      #
      # No thanks to Sequel and pg for making it easy to figure out
      # how to get at this error code....
      e.wrapped_exception.result.error_field(PG::Result::PG_DIAG_SQLSTATE) == "23505"
    end
  end
end

