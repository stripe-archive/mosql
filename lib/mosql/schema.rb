module MoSQL
  class SchemaError < StandardError; end;

  class Schema
    include MoSQL::Logging

    def to_array(lst)
      array = []
      lst.each do |ent|
        if ent.is_a?(Hash) && ent[:source].is_a?(String) && ent[:type].is_a?(String)
          # new configuration format
          array << {
            :source => ent.fetch(:source),
            :type   => ent.fetch(:type),
            :name   => (ent.keys - [:source, :type]).first,
          }
        elsif ent.is_a?(Hash) && ent.keys.length == 1 && ent.values.first.is_a?(String)
          array << {
            :source => ent.first.first,
            :name   => ent.first.first,
            :type   => ent.first.last
          }
        else
          raise SchemaError.new("Invalid ordered hash entry #{ent.inspect}")
        end

      end
      array
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
    end

    def create_schema(db, clobber=false)
      @map.values.each do |dbspec|
        dbspec.each do |n, collection|
          next unless n.is_a?(String)
          meta = collection[:meta]
          log.info("Creating table '#{meta[:table]}'...")
          db.send(clobber ? :create_table! : :create_table?, meta[:table]) do
            collection[:columns].each do |col|
              opts = {}
              if col[:source] == '$timestamp'
                opts[:default] = Sequel.function(:now)
              end
              column col[:name], col[:type], opts

              if col[:source].to_sym == :_id
                primary_key [col[:name].to_sym]
              end
            end
            if meta[:extra_props]
              type =
                if meta[:extra_props] == "JSON"
                  "JSON"
                else
                  "TEXT"
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
      db, collection = ns.split(".")
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

    def fetch_special_source(obj, source)
      case source
      when "$timestamp"
        Sequel.function(:now)
      else
        raise SchemaError.new("Unknown source: #{source}")
      end
    end

    def transform(ns, obj, schema=nil)
      schema ||= find_ns!(ns)

      obj = obj.dup
      row = []
      schema[:columns].each do |col|

        source = col[:source]
        type = col[:type]

        if source.start_with?("$")
          v = fetch_special_source(obj, source)
        else
          v = fetch_and_delete_dotted(obj, source)
          case v
          when BSON::Binary, BSON::ObjectId, Symbol
            v = v.to_s
          when Hash, Array
            v = JSON.dump(v)
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
        Base64.encode64(value.to_s)
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
      find_ns!(ns)[:columns].find {|c| c[:source] == '_id'}[:name]
    end
  end
end
