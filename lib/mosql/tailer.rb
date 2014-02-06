module MoSQL
  class Tailer < Mongoriver::AbstractPersistentTailer
    def self.create_table(db, tablename)
      # MySQL needs primary keys to have defined lengths, so we can't use a TEXT column type
      pk_column_type = ([:mysql, :mysql2].include? db.adapter_scheme) ? 'VARCHAR(255)' : 'TEXT'

      # Create the table
      db.create_table?(tablename) do
        column :service,   pk_column_type
        column :timestamp, 'INTEGER'
        primary_key [:service]
      end
      db[tablename.to_sym]
    end

    def initialize(backends, type, table, opts, adapter_scheme)
      super(backends, type, opts)
      @table   = table
      @service = opts[:service] || "mosql"
      @adapter_scheme = adapter_scheme
    end

    def read_timestamp
      row = @table.where(:service => @service).select([:timestamp]).first
      if row
        BSON::Timestamp.new(row[:timestamp], 0)
      else
        BSON::Timestamp.new(0, 0)
      end
    end

    def write_timestamp(ts)
      unless @did_insert
        begin
          @table.insert({:service => @service, :timestamp => ts.seconds})
        rescue Sequel::DatabaseError => e
          raise unless MoSQL::SQLAdapter.duplicate_key_error?(e, @adapter_scheme)
        end
        @did_insert = true
      end
      @table.where(:service => @service).update(:timestamp => ts.seconds)
    end
  end
end
