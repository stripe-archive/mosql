module MoSQL
  class Streamer
    include MoSQL::Logging

    BATCH_SIZE = 1000
    # How long to wait before saving unsaved inserts to postgres
    INSERT_BATCH_TIMELIMIT = 5.0

    attr_reader :options, :tailer

    NEW_KEYS = [:options, :tailer, :mongo, :sql, :schema]

    def initialize(opts)
      NEW_KEYS.each do |parm|
        unless opts.key?(parm)
          raise ArgumentError.new("Required argument `#{parm}' not provided to #{self.class.name}#new.")
        end
        instance_variable_set(:"@#{parm.to_s}", opts[parm])
      end

      # Hash to from namespace -> inserts that need to be made
      @batch_insert_lists = Hash.new { |hash, key| hash[key] = [] }
      @done = false
    end

    def stop
      @done = true
    end

    def import
      if options[:reimport] || tailer.read_position.nil?
        initial_import
      end
    end

    def collection_for_ns(ns)
      dbname, collection = ns.split(".", 2)
      @mongo.db(dbname).collection(collection)
    end

    def unsafe_handle_exceptions(ns, obj)
      begin
        yield
      rescue Sequel::DatabaseError => e
        wrapped = e.wrapped_exception
        if wrapped.result && options[:unsafe]
          log.warn("Ignoring row (#{obj.inspect}): #{e}")
        else
          log.error("Error processing #{obj.inspect} for #{ns}.")
          raise e
        end
      end
    end

    def bulk_upsert(table, ns, items)
      begin
        @schema.copy_data(table.db, ns, items)
      rescue Sequel::DatabaseError => e
        log.debug("Bulk insert error (#{e}), attempting invidual upserts...")
        cols = @schema.all_columns(@schema.find_ns(ns))
        items.each do |it|
          h = {}
          cols.zip(it).each { |k,v| h[k] = v }
          unsafe_handle_exceptions(ns, h) do
            @sql.upsert!(table, @schema.primary_sql_key_for_ns(ns), h)
          end
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
      @schema.create_schema(@sql.db, !options[:no_drop_tables])

      unless options[:skip_tail]
        start_state = {
          'time' => nil,
          'position' => @tailer.most_recent_position
        }
      end

      dbnames = []

      if options[:dbname]
        log.info "Skipping DB scan and using db: #{options[:dbname]}"
        dbnames = [ options[:dbname] ]
      else
        dbnames = @mongo.database_names
      end

      dbnames.each do |dbname|
        spec = @schema.find_db(dbname)

        if(spec.nil?)
          log.info("Mongd DB '#{dbname}' not found in config file. Skipping.")
          next
        end

        log.info("Importing for Mongo DB #{dbname}...")
        db = @mongo.db(dbname)
        collections = db.collections.select { |c| spec.key?(c.name) }

        collections.each do |collection|
          ns = "#{dbname}.#{collection.name}"
          import_collection(ns, collection, spec[collection.name][:meta][:filter])
          exit(0) if @done
        end
      end

      tailer.save_state(start_state) unless options[:skip_tail]
    end

    def did_truncate; @did_truncate ||= {}; end

    def import_collection(ns, collection, filter)
      log.info("Importing for #{ns}...")
      count = 0
      batch = []
      table = @sql.table_for_ns(ns)
      unless options[:no_drop_tables] || did_truncate[table.first_source]
        table.truncate
        did_truncate[table.first_source] = true
      end

      start    = Time.now
      sql_time = 0
      collection.find(filter, :batch_size => BATCH_SIZE) do |cursor|
        with_retries do
          cursor.each do |obj|
            batch << @schema.transform(ns, obj)
            count += 1

            if batch.length >= BATCH_SIZE
              sql_time += track_time do
                bulk_upsert(table, ns, batch)
              end
              elapsed = Time.now - start
              log.info("Imported #{count} rows (#{elapsed}s, #{sql_time}s SQL)...")
              batch.clear
              exit(0) if @done
            end
          end
        end
      end

      unless batch.empty?
        bulk_upsert(table, ns, batch)
      end
    end

    def optail
      tail_from = options[:tail_from]
      if tail_from.is_a? Time
        tail_from = tailer.most_recent_position(tail_from)
      end

      last_batch_insert = Time.now
      tailer.tail(:from => tail_from)
      until @done
        tailer.stream(1000) do |op|
          handle_op(op)
        end
        time = Time.now
        if time - last_batch_insert >= INSERT_BATCH_TIMELIMIT
          last_batch_insert = time
          do_batch_inserts
        end
      end

      log.info("Finishing, doing last batch inserts.")
      do_batch_inserts
    end

    # Handle $set, $inc and other operators in updates. Done by querying
    # mongo and setting the value to whatever mongo holds at the time.
    # Note that this somewhat messes with consistency as postgres will be
    # "ahead" everything else if tailer is behind.
    #
    # If no such object is found, try to delete according to primary keys that
    # must be present in selector (and not behind $set and etc).
    def sync_object(ns, selector)
      obj = collection_for_ns(ns).find_one(selector)
      if obj
        unsafe_handle_exceptions(ns, obj) do
          @sql.upsert_ns(ns, obj)
        end
      else
        primary_sql_keys = @schema.primary_sql_key_for_ns(ns)
        schema = @schema.find_ns!(ns)
        query = {}
        primary_sql_keys.each do |key|
          source =  schema[:columns].find {|c| c[:name] == key }[:source]
          query[key] = selector[source]
        end
        @sql.table_for_ns(ns).where(query).delete()
      end
    end

    # Add this op to be batch inserted to namespace
    # next time a non-insert happens
    def queue_to_batch_insert(op, namespace)
      @batch_insert_lists[namespace] << @schema.transform(namespace, op['o'])
      if @batch_insert_lists[namespace].length >= BATCH_SIZE
        do_batch_inserts(namespace)
      end
    end

    # Do a batch insert for that namespace, putting data to postgres.
    # If no namespace is given, all namespaces are done
    def do_batch_inserts(namespace=nil)
      if namespace.nil?
        @batch_insert_lists.keys.each do |ns|
          do_batch_inserts(ns)
        end
      else
        to_batch = @batch_insert_lists[namespace]
        @batch_insert_lists[namespace] = []
        return if to_batch.empty?

        table = @sql.table_for_ns(namespace)
        log.debug("Batch inserting #{to_batch.length} items to #{table} from #{namespace}.")
        bulk_upsert(table, namespace, to_batch)
      end
    end

    def handle_op(op)
      log.debug("processing op: #{op.inspect}")
      unless op['ns'] && op['op']
        log.warn("Weird op: #{op.inspect}")
        return
      end

      # First, check if this was an operation performed via applyOps. If so, call handle_op with
      # for each op that was applied.
      # The oplog format of applyOps commands can be viewed here:
      # https://groups.google.com/forum/#!topic/mongodb-user/dTf5VEJJWvY
      if op['op'] == 'c' && (ops = op['o']['applyOps'])
        ops.each { |op| handle_op(op) }
        return
      end

      unless @schema.find_ns(op['ns'])
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
          queue_to_batch_insert(op, ns)
        end
      when 'u'
        selector = op['o2']
        update   = op['o']
        if update.keys.any? { |k| k.start_with? '$' }
          log.debug("resync #{ns}: #{selector['_id']} (update was: #{update.inspect})")
          sync_object(ns, selector)
        else
          do_batch_inserts(ns)

          # The update operation replaces the existing object, but
          # preserves its _id field, so grab the _id off of the
          # 'query' field -- it's not guaranteed to be present on the
          # update.
          primary_sql_keys = @schema.primary_sql_key_for_ns(ns)
          schema = @schema.find_ns!(ns)
          keys = {}
          primary_sql_keys.each do |key|
            source =  schema[:columns].find {|c| c[:name] == key }[:source]
            keys[key] = selector[source]
          end

          log.debug("upsert #{ns}: #{keys}")

          update = keys.merge(update)
          unsafe_handle_exceptions(ns, update) do
            @sql.upsert_ns(ns, update)
          end
        end
      when 'd'
        do_batch_inserts(ns)

        if options[:ignore_delete]
          log.debug("Ignoring delete op on #{ns} as instructed.")
        else
          @sql.delete_ns(ns, op['o'])
        end
      else
        log.info("Skipping unknown op #{op.inspect}")
      end
    end
  end
end
