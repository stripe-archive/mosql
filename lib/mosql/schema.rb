Sequel.extension :inflector

module MoSQL
  class SchemaError < StandardError; end;

  class Schema
    include MoSQL::Logging

    def to_array(lst)
      lst.map do |ent|
        col = nil
        if ent.is_a?(Hash) && ent[:source].is_a?(String) && ent[:type].is_a?(String)
          # new configuration format
          ment = ent.clone
          col = {}
          col[:source] = ment.delete(:source)
          col[:type] = ment.delete(:type)
          col[:default] = ment.delete(:default) if ent.has_key?(:default)
          col[:notnull] = ment.delete(:notnull) if ent.has_key?(:notnull)


          if col[:type].downcase.include? "not null" or col[:type].downcase.include? "default"
            raise SchemaError.new("Type has modifiers, use fields to modify type instead: #{ent.inspect}")
          end
          if ment.keys.length != 1
              raise SchemaError.new("Invalid new configuration entry #{ent.inspect}")
          end
          col[:name] = ment.keys.first
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

    def parent_scope_column(parent, colname)
      (parent.to_s.singularize + "_" + colname.to_s).to_sym
    end

    def parse_spec(ns, spec, parent_pks=[])
      out = spec.dup
      out[:columns] = to_array(spec.delete(:columns))
      meta = spec.delete(:meta)
      pks = parent_pks + primary_sql_keys_for_schema(out).map { |k| parent_scope_column(meta[:table], k) }

      out[:subtables] = spec.map do |name, subspec|
        subspec = parse_spec(ns , subspec, pks)
        subspec[:meta][:source] = name.to_s
        subspec[:meta][:parent_fkeys] = pks
        subspec
      end
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

    def qualified_table_name(meta)
       if meta.key?(:schema)
          Sequel.qualify(meta[:schema], meta[:table])
       else
          meta[:table].to_sym
       end
    end

    def create_table(db, spec, clobber, parent_table=nil, parent_pks={})
        meta = spec[:meta]
        table_name = qualified_table_name(meta)
        composite_key = meta[:composite_key]
        primary_keys = {}
        log.info("Creating table #{db.literal(table_name)}...")
        db.drop_table?(table_name, :cascade => true) if clobber
        db.create_table(table_name) do
          spec[:columns].each do |col|
            opts = {}
            if col.key?(:default)
              if col[:default] == "now()"
                opts[:default] = Sequel.function(:now)
              else
                opts[:default] = col[:default]
              end
            elsif col[:source] == '$timestamp'
              opts[:default] = Sequel.function(:now)
            end
            if col.key?(:notnull)
               opts[:null] = !col[:notnull]
            end
            column col[:name], col[:type], opts

            if composite_key and composite_key.include?(col[:name])
              primary_keys[col[:name].to_sym] = col[:type]
            elsif not composite_key and col[:source].to_sym == :_id
              primary_keys[col[:name].to_sym] = col[:type]
            end
          end

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

          if !parent_table.nil?
            parent_pks.each do |k, type|
              column k, type
            end
            foreign_key parent_pks.keys, parent_table, {
                :on_delete => :cascade,
                :on_update => :cascade
            }
          end
          primary_key primary_keys.keys + parent_pks.keys
        end

        parent_pks = Hash[primary_keys.map { |k, t| [parent_scope_column(meta[:table], k), t] }].merge(parent_pks)
        spec[:subtables].each do |subspec|
          create_table(db, subspec, clobber, table_name, parent_pks)
        end
    end

    def create_schema(db, clobber=false)
      @map.values.each do |dbspec|
        dbspec.each do |n, collection|
          next unless n.is_a?(String)
          create_table(db, collection, clobber)
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

    def sanity_check_type(v, type)
      type = type.downcase
      if (not v.nil? and not v.is_a? Time and type.include? "timestamp") or
          (v.is_a? Time and not type.include? "timestamp") or
          (v.is_a? Integer and not type.include?('int') and not type.include?('float')) or
          (not v.nil? and not v.is_a? Integer and type.include?('int') and v.modulo(1) != 0)
        false
      else
        true
      end
    end

    def transform_primitive(v, type)
      case v
      when Symbol
        v.to_s
      # Hex decode the object ID to a blob so we insert raw binary.
      when BSON::ObjectId
        Sequel::SQL::Blob.new([v.to_s].pack("H*"))
      when BSON::Binary
        Sequel::SQL::Blob.new(v.to_s)
      when BSON::DBRef
        Sequel::SQL::Blob.new([v.object_id.to_s].pack("H*"))
      when Hash, Array
        JSON.dump(v)
      else
        v
      end
    end

    def transform_value(col, v)
      case v
      when Hash
        JSON.dump(v)
      when Array
        if col[:array_type]
          v = v.map { |it| transform_primitive(it, col[:array_type]) }
          Sequel.pg_array(v, col[:array_type])
        else
          JSON.dump(v)
        end
      else
        transform_primitive(v, col[:type])
      end
    end

    def get_pks_for_debug(schema, obj, parent_pks={})
      pks = parent_pks.clone
      sql_pks = primary_sql_keys_for_schema(schema)
      schema[:columns].each do |col|
        break unless sql_pks.include?(col[:name])

        pks[col[:name]] = bson_dig_dotted(obj, col[:source])
      end
      pks
    end

    def transform_one(schema, obj, parent_pks={})
      original = obj

      # Do a deep clone, because we're potentially going to be
      # mutating embedded objects.
      obj = BSON.deserialize(BSON.serialize(obj))

      row = parent_pks.clone
      schema[:columns].each do |col|
        source = col[:source]
        type = col[:type]
        name = col[:name]

        if source.start_with?("$")
          v = fetch_special_source(obj, source, original)
        else
          v = transform_value(col, fetch_and_delete_dotted(obj, source))
        end

        null_allowed = !col[:notnull] or col.has_key?(:default)
        if v.nil? and not null_allowed
          raise "Invalid null #{source.inspect} for #{get_pks_for_debug(schema, obj, parent_pks)}"
        elsif v.is_a? Sequel::SQL::Blob and type != "bytea"
          raise "Failed to convert binary #{source.inspect} to #{type.inspect} for #{get_pks_for_debug(schema, obj, parent_pks)}"
        elsif col[:array_type] and not v.nil?
          v.each_with_index do |e, i|
            if not sanity_check_type(e, col[:array_type])
              raise "Failed to convert array element #{i} of #{source.inspect} to #{type.inspect}: got #{e.inspect} for #{get_pks_for_debug(schema, obj, parent_pks)}"
            end
         end
        elsif not v.nil? and not sanity_check_type(v, type)
          raise "Failed to convert #{source.inspect} to #{type.inspect}: got #{v.inspect} for #{get_pks_for_debug(schema, obj, parent_pks)}"
        end
        row[name] = v
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

    def all_columns(schema)
      cols = schema[:columns].map { |col| col[:name] }
      if schema[:meta][:extra_props]
        cols << "_extra_props"
      end
      cols
    end

    def primary_table_name_for_ns(ns)
      qualified_table_name(find_ns!(ns)[:meta])
    end

    def table_names_for_schema(schema)
      [qualified_table_name(schema[:meta])] + schema[:subtables].map { |s| table_names_for_schema(s) }.flatten
    end

    def all_table_names_for_ns(ns)
      table_names_for_schema(find_ns!(ns))
    end

    def transform_one_ns(ns, obj)
      transform_one(find_ns!(ns), obj)
    end

    def save_all_pks_for_ns(ns, new, old)
      schema = find_ns!(ns)
      # We only save top level keys.
      primary_sql_keys = primary_sql_keys_for_schema(schema)

      primary_sql_keys.each do |key|
        source =  schema[:columns].find {|c| c[:name] == key }[:source]
        new[source] = old[source] unless new.has_key? source
      end

      new
    end

    def bson_dig(obj, *keys)
      keys.each do |k|
        obj = obj[k.to_s]
        break if obj.nil?
      end
      obj
    end

    def bson_dig_dotted(obj, path)
      bson_dig(obj, *path.split("."))
    end

    def all_transforms_for_obj(schema, obj, parent_pks={}, &block)
      table_ident = qualified_table_name(schema[:meta])
      primary_keys = primary_sql_keys_for_schema(schema)

      # Make sure to add in the primary keys from any parent tables, since we
      # might not automatically have them.
      transformed = transform_one(schema, obj, parent_pks)

      yield table_ident, primary_keys, transformed

      pks = Hash[primary_keys.map { |k| [
            parent_scope_column(schema[:meta][:table], k),
            transformed[k]
      ] } ].update(parent_pks)
      schema[:subtables].each do |subspec|
        source = subspec[:meta][:source]
        subobjs = bson_dig_dotted(obj, source)
        break if subobjs.nil?

        subobjs.each do |subobj|
          all_transforms_for_obj(subspec, subobj, pks, &block)
        end
      end
    end

    def all_transforms_for_ns(ns, documents, &block)
      schema = find_ns!(ns)
      documents.each do |obj|
        all_transforms_for_obj(schema, obj, &block)
      end
    end

    def all_mongo_dbs
      @map.keys
    end

    def collections_for_mongo_db(db)
      (@map[db]||{}).keys
    end

    def primary_sql_keys_for_schema(schema)
      keys = []
      if schema[:meta][:composite_key]
        keys = schema[:meta][:composite_key].map{ |k| k.to_sym }
      else
        keys << schema[:columns].find {|c| c[:source] == '_id'}[:name]
      end

      return keys
    end

    def primary_sql_keys_for_ns(ns)
      primary_sql_keys_for_schema(find_ns!(ns))
    end
  end
end
