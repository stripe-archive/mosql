module MoSQL
  class Tailer < Mongoriver::AbstractPersistentTailer
    def self.create_table(db, tablename)
      if !db.table_exists?(tablename)
        db.create_table?(tablename) do
          column :service,     'TEXT'
          column :timestamp,   'INTEGER'
          column :placeholder, 'BYTEA'
          primary_key [:service]
        end
      else
        # Try to do seamless upgrades from before-tokumx times
        # It will raise an exception in this in most cases,
        # but there isn't a nice way I found to check if column
        # exists.
        begin
          db.add_column(tablename, :placeholder, 'BYTEA')
        rescue Sequel::DatabaseError => e
          raise unless MoSQL::SQLAdapter.duplicate_column_error?(e)
        end
      end

      db[tablename.to_sym]
    end

    def initialize(backends, type, table, opts)
      super(backends, type, opts)
      @table   = table
      @service = opts[:service] || "mosql"
    end

    def read_state
      row = @table.where(:service => @service).first
      return nil unless row
      # Again, try to do seamless upgrades - 
      # If latest operation before or at timestamp if no placeholder 
      # exists, use timestamp in database to guess what it could be.
      result = {}
      result['time'] = Time.at(row.fetch(:timestamp))
      if row[:placeholder]
        result['placeholder'] = from_blob(row[:placeholder])
      else
        log.warn("Trying to seamlessly update from old version!")
        result['placeholder'] = most_recent_operation(result['time'])
        save_state(result)
      end
      result
    end

    def write_state(state)
      data = {
        :service => @service,
        :timestamp => state['time'].to_i,
        :placeholder => to_blob(state['placeholder'])
      }

      unless @did_insert
        begin
          @table.insert(data)
        rescue Sequel::DatabaseError => e
          raise unless MoSQL::SQLAdapter.duplicate_key_error?(e)
        end
        @did_insert = true
      end

      @table.where(:service => @service).update(data)
    end

    private
    def to_blob(placeholder)
      case database_type
      when :mongo
        return Sequel::SQL::Blob.new(placeholder.seconds.to_s)
      when :toku
        return Sequel::SQL::Blob.new(placeholder.to_s)
      end
    end

    def from_blob(blob)
      case database_type
      when :mongo
        return BSON::Timestamp.new(blob.to_i, 0)
      when :toku
        return BSON::Binary.new(blob)
      end
    end
  end
end
