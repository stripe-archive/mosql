require 'mosql'
require 'optparse'
require 'yaml'
require 'logger'

module MoSQL
  class CLI
    include MoSQL::Logging

    BATCH       = 1000

    attr_reader :args, :options, :tailer

    def self.run(args)
      cli = CLI.new(args)
      cli.run
    end

    def initialize(args)
      @args    = args
      @options = []
      @done    = false
      setup_signal_handlers
    end

    def setup_signal_handlers
      %w[TERM INT USR2].each do |sig|
        Signal.trap(sig) do
          log.info("Got SIG#{sig}. Preparing to exit...")
          @done = true
        end
      end
    end

    def parse_args
      @options = {
        :collections => 'collections.yml',
        :sql    => 'postgres:///',
        :mongo  => 'mongodb://localhost',
        :verbose => 0
      }
      optparse = OptionParser.new do |opts|
        opts.banner = "Usage: #{$0} [options] "

        opts.on('-h', '--help', "Display this message") do
          puts opts
          exit(0)
        end

        opts.on('-v', "Increase verbosity") do
          @options[:verbose] += 1
        end

        opts.on("-c", "--collections [collections.yml]", "Collection map YAML file") do |file|
          @options[:collections] = file
        end

        opts.on("--sql [sqluri]", "SQL server to connect to") do |uri|
          @options[:sql] = uri
        end

        opts.on("--mongo [mongouri]", "Mongo connection string") do |uri|
          @options[:mongo] = uri
        end

        opts.on("--schema [schema]", "PostgreSQL 'schema' to namespace tables") do |schema|
          @options[:schema] = schema
        end

        opts.on("--ignore-delete", "Ignore delete operations when tailing") do
          @options[:ignore_delete] = true
        end

        opts.on("--tail-from [timestamp]", "Start tailing from the specified UNIX timestamp") do |ts|
          @options[:tail_from] = ts
        end

        opts.on("--service [service]", "Service name to use when storing tailing state") do |service|
          @options[:service] = service
        end

        opts.on("--skip-tail", "Don't tail the oplog, just do the initial import") do
          @options[:skip_tail] = true
        end

        opts.on("--reimport", "Force a data re-import") do
          @options[:reimport] = true
        end

        opts.on("--no-drop-tables", "Don't drop the table if it exists during the initial import") do
          @options[:no_drop_tables] = true
        end
      end

      optparse.parse!(@args)

      log = Log4r::Logger.new('Stripe')
      log.outputters = Log4r::StdoutOutputter.new(STDERR)
      if options[:verbose] >= 1
        log.level = Log4r::DEBUG
      else
        log.level = Log4r::INFO
      end
    end

    def connect_mongo
      @mongo = Mongo::Connection.from_uri(options[:mongo])
      config = @mongo['admin'].command(:ismaster => 1)
      if !config['setName'] && !options[:skip_tail]
        log.warn("`#{options[:mongo]}' is not a replset.")
        log.warn("Will run the initial import, then stop.")
        log.warn("Pass `--skip-tail' to suppress this warning.")
        options[:skip_tail] = true
      end
      options[:service] ||= config['setName']
    end

    def connect_sql
      @sql = MoSQL::SQLAdapter.new(@schemamap, options[:sql], options[:schema])
      if options[:verbose] >= 2
        @sql.db.sql_log_level = :debug
        @sql.db.loggers << Logger.new($stderr)
      end
    end

    def load_collections
      collections = YAML.load_file(@options[:collections])
      @schemamap = MoSQL::Schema.new(collections)
    end

    def init_callbacks
      @schemamap.init_callbacks(@sql.db)
    end

    def run
      parse_args
      load_collections
      connect_sql
      connect_mongo
      init_callbacks

      metadata_table = MoSQL::Tailer.create_table(@sql.db, 'mosql_tailers')

      @tailer = MoSQL::Tailer.new([@mongo], :existing, metadata_table,
                                  :service => options[:service])

      if options[:reimport] || tailer.read_timestamp.seconds == 0
        initial_import
      end

      unless options[:skip_tail]
        optail
      end
    end

    # Helpers

    def collection_for_ns(ns)
      dbname, collection = ns.split(".", 2)
      @mongo.db(dbname).collection(collection)
    end

    def bulk_upsert(table, ns, items)
      begin
        @schemamap.copy_data(table.db, ns, items)
      rescue Sequel::DatabaseError => e
        log.debug("Bulk insert error (#{e}), attempting invidual upserts...")
        cols = @schemamap.all_columns(@schemamap.find_ns(ns))
        items.each do |it|
          h = {}
          cols.zip(it).each { |k,v| h[k] = v }
          @sql.upsert(table, @schemamap.primary_sql_key_for_ns(ns), h)
        end
      end
    end

    def with_retries(tries=10)
      tries.times do |try|
        begin
          yield
        rescue Mongo::ConnectionError, Mongo::ConnectionFailure, Mongo::OperationFailure => e
          # Duplicate key error
          raise if e.kind_of?(Mongo::OperationFailure) && [11000, 11001].include?(e.error_code)
          # Cursor timeout
          raise if e.kind_of?(Mongo::OperationFailure) && e.message =~ /^Query response returned CURSOR_NOT_FOUND/
          delay = 0.5 * (1.5 ** try)
          log.warn("Mongo exception: #{e}, sleeping #{delay}s...")
          sleep(delay)
        end
      end
    end

    def track_time
      start = Time.now
      yield
      Time.now - start
    end

    def initial_import
      @schemamap.create_schema(@sql.db, !options[:no_drop_tables])

      unless options[:skip_tail]
        start_ts = @mongo['local']['oplog.rs'].find_one({}, {:sort => [['$natural', -1]]})['ts']
      end

      want_dbs = @schemamap.all_mongo_dbs & @mongo.database_names
      want_dbs.each do |dbname|
        log.info("Importing for Mongo DB #{dbname}...")
        db = @mongo.db(dbname)
        want = Set.new(@schemamap.collections_for_mongo_db(dbname))
        db.collections.select { |c| want.include?(c.name) }.each do |collection|
          ns = "#{dbname}.#{collection.name}"
          import_collection(ns, collection)
          exit(0) if @done
        end
      end

      tailer.write_timestamp(start_ts) unless options[:skip_tail]
    end

    def import_collection(ns, collection)
      log.info("Importing for #{ns}...")
      count      = 0
      batch_rows = []
      batch_objs = []
      callback   = @schemamap.callback_for_ns(ns)
      table      = @sql.table_for_ns(ns)
      table.truncate unless options[:no_drop_tables]

      start         = Time.now
      sql_time      = 0
      callback_time = 0
      collection.find(nil, :batch_size => BATCH) do |cursor|
        with_retries do
          cursor.each do |obj|
            batch_rows << @schemamap.transform(ns, obj)
            batch_objs << obj if callback
            count += 1

            if batch_rows.length >= BATCH
              sql_time += track_time do
                bulk_upsert(table, ns, batch_rows)
              end
              elapsed = Time.now - start
              batch_rows.clear
              if callback
                callback_time += track_time do
                  batch_objs.each do |obj|
                    callback.after_upsert(obj)
                  end
                end
                batch_objs.clear
              end
              log.info("Imported #{count} rows (#{elapsed}s, #{sql_time}s SQL #{callback_time}s callback)...")
              exit(0) if @done
            end
          end
        end
      end

      unless batch_rows.empty?
        bulk_upsert(table, ns, batch_rows)
        if callback
          batch_objs.each do |obj|
            callback.after_upsert(obj)
          end
        end
      end
    end

    def optail
      tailer.tail_from(options[:tail_from] ?
                       BSON::Timestamp.new(options[:tail_from].to_i, 0) :
                       nil)
      until @done
        tailer.stream(1000) do |op|
          handle_op(op)
        end
      end
    end

    def sync_object(ns, _id)
      primary_sql_key = @schemamap.primary_sql_key_for_ns(ns)
      sqlid           = @sql.transform_one_ns(ns, { '_id' => _id })[primary_sql_key]
      obj             = collection_for_ns(ns).find_one({:_id => _id})
      callback        = @schemamap.callback_for_ns(ns)
      if obj
        @sql.upsert_ns(ns, obj)
        callback.after_upsert(obj) if callback
      else
        @sql.table_for_ns(ns).where(primary_sql_key.to_sym => sqlid).delete()
        callback.after_delete(:_id => sqlid) if callback
      end
    end

    def handle_op(op)
      log.debug("processing op: #{op.inspect}")
      unless op['ns'] && op['op']
        log.warn("Weird op: #{op.inspect}")
        return
      end

      unless @schemamap.find_ns(op['ns'])
        log.debug("Skipping op for unknown ns #{op['ns']}...")
        return
      end

      ns = op['ns']
      dbname, collection_name = ns.split(".", 2)

      case op['op']
      when 'n'
        log.debug("Skipping no-op #{op.inspect}")
      when 'i'
        if collection_name == 'system.indexes'
          log.info("Skipping index update: #{op.inspect}")
        else
          @sql.upsert_ns(ns, op['o'])

          callback = @schemamap.callback_for_ns(ns)
          callback.after_upsert(op['o']) if callback
        end
      when 'u'
        selector = op['o2']
        update   = op['o']
        if update.keys.any? { |k| k.start_with? '$' }
          log.debug("resync #{ns}: #{selector['_id']} (update was: #{update.inspect})")
          sync_object(ns, selector['_id'])
        else
          log.debug("upsert #{ns}: _id=#{selector['_id']}")

          # The update operation replaces the existing object, but
          # preserves its _id field, so grab the _id off of the
          # 'query' field -- it's not guaranteed to be present on the
          # update.
          update = { '_id' => selector['_id'] }.merge(update)
          @sql.upsert_ns(ns, update)

          callback = @schemamap.callback_for_ns(ns)
          callback.after_upsert(update) if callback
        end
      when 'd'
        if options[:ignore_delete]
          log.debug("Ignoring delete op on #{ns} as instructed.")
        else
          @sql.delete_ns(ns, op['o'])

          callback = @schemamap.callback_for_ns(ns)
          callback.after_delete(op['o']) if callback
        end
      else
        log.info("Skipping unknown op #{op.inspect}")
      end
    end
  end
end
