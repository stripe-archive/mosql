module MoSQL
  class Tailer < Mongoriver::AbstractPersistentTailer
    def self.create_table(db, tablename)
      if !db.table_exists?(tablename)
        db.create_table?(tablename) do
          column :service,     'TEXT'
          column :timestamp,   'INTEGER'
          column :placeholder, 'BLOB'
          primary_key [:service]
        end
      else
        # Try to do seamless upgrades
        db.add_column(tablename, :placeholder, 'BLOB')
      end

      db[tablename.to_sym]
    end

    def initialize(backends, type, table, opts)
      super(backends, type, opts)
      @table   = table
      @service = opts[:service] || "mosql"
    end

    def read_state
      row = @table.where(:service => @service).select([:timestamp, :placeholder]).first
      return nil unless row
      # try to do seamless upgrades - use timestamp as placeholder if no
      # placeholder exists
      if row[:placeholder].nil?
        row[:placeholder] = to_blob(BSON::Timestamp.new(row[:timestamp], 0))
      end
      {
        :time => Time.at(row[:timestamp]),
        :placeholder => from_blob(row[:placeholder])
      }
    end

    def write_timestamp(state)
      time = state[:time].to_i
      placeholder = to_blob(state[:placeholder])

      unless @did_insert
        begin
          @table.insert({
            :service => @service,
            :timestamp => time,
            :placeholder => placeholder
          })
        rescue Sequel::DatabaseError => e
          raise unless MoSQL::SQLAdapter.duplicate_key_error?(e)
        end
        @did_insert = true
      end

      @table.where(:service => @service).update({
        :timestamp => time,
        :placeholder => placeholder
      })
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
