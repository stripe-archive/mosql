module MoSQL
  class SQLAdapter
    include MoSQL::Logging

    attr_reader :db

    def initialize(schema, uri, pgschema=nil)
      @schema = schema
      connect_db(uri, pgschema)
      @db.extension :pg_array
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

    def table_for_ident(ident)
      @db[ident]
    end

    def upsert_ns(ns, obj)
      @schema.all_transforms_for_ns(ns, [obj]) do |table, pks, row|
        upsert!(table_for_ident(table), pks, row)
      end
    end

    def delete_ns(ns, obj)
      @schema.all_transforms_for_ns(ns, [obj]) do |table, pks, _|
        table_for_ident(table).where(pks).delete
      end
    end

    def upsert!(table, table_primary_keys, item)
      rows = table.where(table_primary_keys).update(item)
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

    def self.duplicate_column_error?(e)
      e.wrapped_exception.result.error_field(PG::Result::PG_DIAG_SQLSTATE) == "42701"
    end
  end
end

