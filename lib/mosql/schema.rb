module MoSQL
  class SchemaError < StandardError; end;

  class Schema
    include MoSQL::Logging

    def to_array(lst)
      lst.map do |ent|
        col = nil
        if ent.is_a?(Hash) && ent[:source].is_a?(String) && ent[:type].is_a?(String)
          # new configuration format
          col = {
            :source => ent.fetch(:source),
            :type   => ent.fetch(:type),
            :name   => (ent.keys - [:source, :type]).first,
          }
        elsif ent.is_a?(Hash) && ent.keys.length == 1 && ent.values.first.is_a?(String)
          col = {
            :source => ent.first.first,
            :name   => ent.first.first,
            :type   => ent.first.last
          }
        else
          raise SchemaError.new("Invalid ordered hash entry #{ent.inspect}")
        end

        if !col.key?(:array_type) && /\A(.+)\s+array\z/i.match(col[:type])
          col[:array_type] = $1
        end

        col
      end
    end

    def check_columns!(ns, spec)
      seen = Set.new
      spec[:columns].each do |col|
        if seen.include?(col[:source])
          raise SchemaError.new("Duplicate source #{col[:source]} in column definition #{col[:name]} for #{ns}.")
        end
        seen.add(col[:source])
      end
    end

    def parse_spec(ns, spec)
      out = spec.dup
      out[:columns] = to_array(spec.fetch(:columns))
      check_columns!(ns, out)
      out
    end

    def parse_meta(meta)
      meta = {} if meta.nil?
      meta[:alias] = [] unless meta.key?(:alias)
      meta[:alias] = [meta[:alias]] unless meta[:alias].is_a?(Array)
      meta[:alias] = meta[:alias].map { |r| Regexp.new(r) }
      meta
    end

    def initialize(map)
      @map = {}
      map.each do |dbname, db|
        @map[dbname] = { :meta => parse_meta(db[:meta]) }
        db.each do |cname, spec|
          next unless cname.is_a?(String)
          begin
            @map[dbname][cname] = parse_spec("#{dbname}.#{cname}", spec)
          rescue KeyError => e
            raise SchemaError.new("In spec for #{dbname}.#{cname}: #{e}")
          end
        end
      end

      # Lurky way to force Sequel force all timestamps to use UTC.
      Sequel.default_timezone = :utc
    end

    def create_schema(db, clobber=false)
      @map.values.each do |dbspec|
        dbspec.each do |n, collection|
          next unless n.is_a?(String)
          meta = collection[:meta]
          composite_key = meta[:composite_key]
          keys = []
          log.info("Creating table '#{meta[:table]}'...")
          db.send(clobber ? :create_table! : :create_table?, meta[:table]) do
            collection[:columns].each do |col|
              opts = {}
              if col[:source] == '$timestamp'
                opts[:default] = Sequel.function(:now)
              end
              column col[:name], col[:type], opts

              if composite_key and composite_key.include?(col[:name])
                keys << col[:name].to_sym
              elsif not composite_key and col[:source].to_sym == :_id
                keys << col[:name].to_sym
              end
            end

            primary_key keys
            if meta[:extra_props]
              type =
                case meta[:extra_props]
                when 'JSON'
                  'JSON'
                when 'JSONB'
                  'JSONB'
                else
                  'TEXT'
                end
              column '_extra_props', type
            end
          end
        end
      end
    end

    def find_db(db)
      unless @map.key?(db)
        @map[db] = @map.values.find do |spec|
          spec && spec[:meta][:alias].any? { |a| a.match(db) }
        end
      end
      @map[db]
    end

    def find_ns(ns)
      db, collection = ns.split(".", 2)
      unless spec = find_db(db)
        return nil
      end
      unless schema = spec[collection]
        log.debug("No mapping for ns: #{ns}")
        return nil
      end
      schema
    end

    def find_ns!(ns)
      schema = find_ns(ns)
      raise SchemaError.new("No mapping for namespace: #{ns}") if schema.nil?
      schema
    end

    def fetch_and_delete_dotted(obj, dotted)
      pieces = dotted.split(".")
      breadcrumbs = []
      while pieces.length > 1
        key = pieces.shift
        breadcrumbs << [obj, key]
        obj = obj[key]
        return nil unless obj.is_a?(Hash)
      end

      val = obj.delete(pieces.first)

      breadcrumbs.reverse.each do |obj, key|
        obj.delete(key) if obj[key].empty?
      end

      val
    end

    def fetch_exists(obj, dotted)
      pieces = dotted.split(".")
      while pieces.length > 1
        key = pieces.shift
        obj = obj[key]
        return false unless obj.is_a?(Hash)
      end
      obj.has_key?(pieces.first)
    end

    def fetch_special_source(obj, source, original)
      case source
      when "$timestamp"
        Sequel.function(:now)
      when /^\$exists (.+)/
        # We need to look in the cloned original object, not in the version that
        # has had some fields deleted.
        fetch_exists(original, $1)
      else
        raise SchemaError.new("Unknown source: #{source}")
      end
    end

    def transform_primitive(v, type=nil)
      case v
      when BSON::ObjectId, Symbol
        v.to_s
      when BSON::Binary
        if type.downcase == 'uuid'
          v.data.to_s.unpack("H*").first
        else
          Sequel::SQL::Blob.new(v.data.to_s)
        end
      when Mongo::DBRef
        v.id.to_s
      else
        v
      end
    end

    def transform(ns, obj, schema=nil)
      schema ||= find_ns!(ns)

      original = obj

      # Do a deep clone, because we're potentially going to be
      # mutating embedded objects.
      obj = Marshal.load(Marshal.dump(obj))

      row = []
      schema[:columns].each do |col|

        source = col[:source]
        type = col[:type]

        if source.start_with?("$")
          v = fetch_special_source(obj, source, original)
        else
          v = fetch_and_delete_dotted(obj, source)
          case v
          when Hash
            v = JSON.dump(Hash[v.map { |k,v| [k, transform_primitive(v)] }])
          when Array
            v = v.map { |it| transform_primitive(it) }
            if col[:array_type]
              v = Sequel.pg_array(v, col[:array_type])
            else
              v = JSON.dump(v)
            end
          else
            v = transform_primitive(v, type)
          end
        end
        row << v
      end

      if schema[:meta][:extra_props]
        extra = sanitize(obj)
        row << JSON.dump(extra)
      end

      log.debug { "Transformed: #{row.inspect}" }

      row
    end

    def sanitize(value)
      # Base64-encode binary blobs from _extra_props -- they may
      # contain invalid UTF-8, which to_json will not properly encode.
      case value
      when Hash
        ret = {}
        value.each {|k, v| ret[k] = sanitize(v)}
        ret
      when Array
        value.map {|v| sanitize(v)}
      when BSON::Binary
        Base64.encode64(value.data.to_s)
      when Float
        # NaN is illegal in JSON. Translate into null.
        value.nan? ? nil : value
      else
        value
      end
    end

    def copy_column?(col)
      col[:source] != '$timestamp'
    end

    def all_columns(schema, copy=false)
      cols = []
      schema[:columns].each do |col|
        cols << col[:name] unless copy && !copy_column?(col)
      end
      if schema[:meta][:extra_props]
        cols << "_extra_props"
      end
      cols
    end

    def all_columns_for_copy(schema)
      all_columns(schema, true)
    end

    def copy_data(db, ns, objs)
      schema = find_ns!(ns)
      db.synchronize do |pg|
        sql = "COPY \"#{schema[:meta][:table]}\" " +
          "(#{all_columns_for_copy(schema).map {|c| "\"#{c}\""}.join(",")}) FROM STDIN"
        pg.execute(sql)
        objs.each do |o|
          pg.put_copy_data(transform_to_copy(ns, o, schema) + "\n")
        end
        pg.put_copy_end
        begin
          pg.get_result.check
        rescue PGError => e
          db.send(:raise_error, e)
        end
      end
    end

    def quote_copy(val)
      case val
      when nil
        "\\N"
      when true
        't'
      when false
        'f'
      when Sequel::SQL::Function
        nil
      when DateTime, Time
        val.strftime("%FT%T.%6N %z")
      when Sequel::SQL::Blob
        "\\\\x" + [val].pack("h*")
      else
        val.to_s.gsub(/([\\\t\n\r])/, '\\\\\\1')
      end
    end

    def transform_to_copy(ns, row, schema=nil)
      row.map { |c| quote_copy(c) }.compact.join("\t")
    end

    def table_for_ns(ns)
      find_ns!(ns)[:meta][:table]
    end

    def all_mongo_dbs
      @map.keys
    end

    def collections_for_mongo_db(db)
      (@map[db]||{}).keys
    end

    def primary_sql_key_for_ns(ns)
      ns = find_ns!(ns)
      keys = []
      if ns[:meta][:composite_key]
        keys = ns[:meta][:composite_key]
      else
        keys << ns[:columns].find {|c| c[:source] == '_id'}[:name]
      end

      return keys
    end
  end
end
