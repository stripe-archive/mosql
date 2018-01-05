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
          @streamer.stop
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

        opts.on("--only-db [dbname]", "Don't scan for mongo dbs, just use the one specified") do |dbname|
          @options[:dbname] = dbname
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

        opts.on("--skip-import", "Don't import data before tailing oplog") do
          @options[:skip_import] = true
        end

        opts.on("--reimport", "Force a data re-import") do
          @options[:reimport] = true
        end

        opts.on("--no-drop-tables", "Don't drop the table if it exists during the initial import") do
          @options[:no_drop_tables] = true
        end

        opts.on("--unsafe", "Ignore rows that cause errors on insert") do
          @options[:unsafe] = true
        end

        # eg, --oplog-filter '{"ns": {"$regex": "^somedb[0-9]*\\.collection$"}}'
        opts.on("--oplog-filter [filter]", "An additional JSON filter for the oplog query") do |filter|
          @options[:oplog_filter] = JSON.parse(filter)
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
      @mongo = Mongo::Client.new(options[:mongo])
      config = @mongo.use('admin').command(:ismaster => 1).documents.first
      if !config['setName'] && !options[:skip_tail]
        log.warn("`#{options[:mongo]}' is not a replset.")
        log.warn("Will run the initial import, then stop.")
        log.warn("Pass `--skip-tail' to suppress this warning.")
        options[:skip_tail] = true
      end
      options[:service] ||= config['setName']
    end

    def connect_sql
      @sql = MoSQL::SQLAdapter.new(@schema, options[:sql], options[:schema])
      if options[:verbose] >= 2
        @sql.db.sql_log_level = :debug
        @sql.db.loggers << Logger.new($stderr)
      end
    end

    def load_collections
      collections = YAML.load_file(@options[:collections])
      begin
        @schema = MoSQL::Schema.new(collections)
      rescue MoSQL::SchemaError => e
        log.error("Error parsing collection map `#{@options[:collections]}':")
        log.error(e.to_s)
        exit(1)
      end
    end

    def run
      parse_args
      load_collections
      connect_sql
      connect_mongo

      metadata_table = MoSQL::Tailer.create_table(@sql.db, 'mosql_tailers')

      @tailer = MoSQL::Tailer.new([@mongo], :existing, metadata_table,
                                  :service => options[:service])

      @streamer = Streamer.new(:options => @options,
                               :tailer  => @tailer,
                               :mongo   => @mongo,
                               :sql     => @sql,
                               :schema  => @schema)

      unless options[:skip_import]
        @streamer.import
      end

      unless options[:skip_tail]
        @streamer.optail
      end
    end
  end
end
