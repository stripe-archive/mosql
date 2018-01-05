require File.join(File.dirname(__FILE__), '../../../_lib.rb')

class MoSQL::Test::SchemaTest < MoSQL::Test
  TEST_MAP = <<EOF
---
db:
  collection:
    :meta:
      :table: sqltable
    :columns:
      - id:
        :source: _id
        :type: TEXT
      - var: INTEGER
      - str: TEXT
      - arry: INTEGER ARRAY
  with_extra_props:
    :meta:
      :table: sqltable2
      :extra_props: true
    :columns:
      - id:
        :source: _id
        :type: TEXT
  old_conf_syntax:
    :columns:
      - _id: TEXT
    :meta:
      :table: sqltable3
  with_extra_props_type:
    :meta:
      :table: sqltable4
      :extra_props: JSON
    :columns:
      - _id: TEXT
  treat_array_as_string:
    :columns:
      - _id: TEXT
      - arry: TEXT
    :meta:
      :table: sqltable5
  with_composite_key:
    :meta:
      :table: sqltable6
      :composite_key:
        - store
        - time
    :columns:
      - store:
        :source: _id.s
        :type: TEXT
      - time:
        :source: id.t
        :type: TEXT
      - var:
        :source: var
        :type: TEXT

EOF

  before do
    Sequel.extension(:pg_array)
    @map = MoSQL::Schema.new(YAML.load(TEST_MAP))
  end

  it 'Loads the schema' do
    assert(@map)
  end

  it 'Can find an ns' do
    assert(@map.find_ns("db.collection"))
    assert_nil(@map.find_ns("db.other_collection"))
    assert_raises(MoSQL::SchemaError) do
      @map.find_ns!("db.other_collection")
    end
  end

  it 'Converts columns to an array' do
    table = @map.find_ns("db.collection")
    assert(table[:columns].is_a?(Array))

    id_mapping = table[:columns].find{|c| c[:source] == '_id'}
    assert id_mapping
    assert_equal '_id', id_mapping[:source]
    assert_equal 'id', id_mapping[:name]
    assert_equal 'TEXT', id_mapping[:type]

    var_mapping = table[:columns].find{|c| c[:source] == 'var'}
    assert var_mapping
    assert_equal 'var', var_mapping[:source]
    assert_equal 'var', var_mapping[:name]
    assert_equal 'INTEGER', var_mapping[:type]
  end

  it 'Can handle the old configuration format' do
    table = @map.find_ns('db.old_conf_syntax')
    assert(table[:columns].is_a?(Array))

    id_mapping = table[:columns].find{|c| c[:source] == '_id'}
    assert id_mapping
    assert_equal '_id', id_mapping[:source]
    assert_equal '_id', id_mapping[:name]
    assert_equal 'TEXT', id_mapping[:type]
  end

  it 'Can find the primary key of the SQL table' do
    assert_equal(['id'], @map.primary_sql_key_for_ns('db.collection'))
    assert_equal(['_id'], @map.primary_sql_key_for_ns('db.old_conf_syntax'))
  end

  it 'can create a SQL schema' do
    db = stub()
    db.expects(:create_table?).with('sqltable')
    db.expects(:create_table?).with('sqltable2')
    db.expects(:create_table?).with('sqltable3')
    db.expects(:create_table?).with('sqltable4')
    db.expects(:create_table?).with('sqltable5')
    db.expects(:create_table?).with('sqltable6')

    @map.create_schema(db)
  end

  it 'creates a SQL schema with the right fields' do
    db = {}
    stub_1 = stub('table 1')
    stub_1.expects(:column).with('id', 'TEXT', {})
    stub_1.expects(:column).with('var', 'INTEGER', {})
    stub_1.expects(:column).with('str', 'TEXT', {})
    stub_1.expects(:column).with('arry', 'INTEGER ARRAY', {})
    stub_1.expects(:column).with('_extra_props').never
    stub_1.expects(:primary_key).with([:id])
    stub_2 = stub('table 2')
    stub_2.expects(:column).with('id', 'TEXT', {})
    stub_2.expects(:column).with('_extra_props', 'TEXT')
    stub_2.expects(:primary_key).with([:id])
    stub_3 = stub('table 3')
    stub_3.expects(:column).with('_id', 'TEXT', {})
    stub_3.expects(:column).with('_extra_props').never
    stub_3.expects(:primary_key).with([:_id])
    stub_4 = stub('table 4')
    stub_4.expects(:column).with('_id', 'TEXT', {})
    stub_4.expects(:column).with('_extra_props', 'JSON')
    stub_4.expects(:primary_key).with([:_id])
    stub_5 = stub('table 5')
    stub_5.expects(:column).with('_id', 'TEXT', {})
    stub_5.expects(:column).with('arry', 'TEXT', {})
    stub_5.expects(:primary_key).with([:_id])
    stub_6 = stub('table 6')
    stub_6.expects(:column).with('store', 'TEXT', {})
    stub_6.expects(:column).with('time', 'TEXT', {})
    stub_6.expects(:column).with('var', 'TEXT', {})
    stub_6.expects(:primary_key).with([:store, :time])
    (class << db; self; end).send(:define_method, :create_table?) do |tbl, &blk|
      case tbl
      when "sqltable"
        o = stub_1
      when "sqltable2"
        o = stub_2
      when "sqltable3"
        o = stub_3
      when "sqltable4"
        o = stub_4
      when "sqltable5"
        o = stub_5
      when "sqltable6"
        o = stub_6
      else
        assert(false, "Tried to create an unexpected table: #{tbl}")
      end
      o.instance_eval(&blk)
    end
    @map.create_schema(db)
  end

  describe 'when transforming' do
    it 'transforms rows' do
      out = @map.transform('db.collection', {'_id' => "row 1", 'var' => 6, 'str' => 'a string', 'arry' => [1,2,3]})
      assert_equal(["row 1", 6, 'a string', [1,2,3]], out)
    end

    it 'Includes extra props' do
      out = @map.transform('db.with_extra_props', {'_id' => 7, 'var' => 6, 'other var' => {'key' => 'value'}})
      assert_equal(2, out.length)
      assert_equal(7, out[0])
      assert_equal({'var' => 6, 'other var' => {'key' => 'value'}}, JSON.parse(out[1]))
    end

    it 'gets all_columns right' do
      assert_equal(['id', 'var', 'str', 'arry'], @map.all_columns(@map.find_ns('db.collection')))
      assert_equal(['id', '_extra_props'], @map.all_columns(@map.find_ns('db.with_extra_props')))
    end

    it 'stringifies symbols' do
      out = @map.transform('db.collection', {'_id' => "row 1", 'str' => :stringy, 'arry' => [1,2,3]})
      assert_equal(["row 1", nil, 'stringy', [1,2,3]], out)
    end

    it 'extracts object ids from a DBRef' do
      oid = BSON::ObjectId.new
      out = @map.transform('db.collection', {'_id' => "row 1",
          'str' => Mongo::DBRef.new('db.otherns', oid)})
      assert_equal(["row 1", nil, oid.to_s, nil], out)
    end

    it 'converts DBRef to object id in arrays' do
      oid = [ BSON::ObjectId.new, BSON::ObjectId.new]
      o = {'_id' => "row 1", "str" => [ Mongo::DBRef.new('db.otherns', oid[0]), Mongo::DBRef.new('db.otherns', oid[1]) ] }
      out = @map.transform('db.collection', o)
      assert_equal(["row 1", nil, JSON.dump(oid.map! {|o| o.to_s}), nil ], out)
    end

    it 'changes NaN to null in extra_props' do
      out = @map.transform('db.with_extra_props', {'_id' => 7, 'nancy' => 0.0/0.0})
      extra = JSON.parse(out[1])
      assert(extra.key?('nancy'))
      assert_equal(nil, extra['nancy'])
    end

    it 'base64-encodes BSON::Binary blobs in extra_props' do
      out = @map.transform('db.with_extra_props',
        {'_id' => 7,
          'blob' => BSON::Binary.new("\x00\x00\x00", :generic),
          'embedded' => {'thing' => BSON::Binary.new("\x00\x00\x00", :generic)}})
      extra = JSON.parse(out[1])
      assert(extra.key?('blob'))
      assert_equal('AAAA', extra['blob'].strip)
      refute_nil(extra['embedded'])
      refute_nil(extra['embedded']['thing'])
      assert_equal('AAAA', extra['embedded']['thing'].strip)
    end

    it 'will treat arrays as strings when schame says to' do
      out = @map.transform('db.treat_array_as_string', {'_id' => 1, 'arry' => [1, 2, 3]})
      assert_equal(out[0], 1)
      assert_equal(out[1], '[1,2,3]')
    end
  end

  describe 'when copying data' do
    it 'quotes special characters' do
      assert_equal(%q{\\\\}, @map.quote_copy(%q{\\}))
      assert_equal(%Q{\\\t}, @map.quote_copy( %Q{\t}))
      assert_equal(%Q{\\\n}, @map.quote_copy( %Q{\n}))
      assert_equal(%Q{some text}, @map.quote_copy(%Q{some text}))
    end
  end

  describe 'fetch_and_delete_dotted' do
    def check(orig, path, expect, result)
      assert_equal(expect, @map.fetch_and_delete_dotted(orig, path))
      assert_equal(result, orig)
    end

    it 'works on things without dots' do
      check({'a' => 1, 'b' => 2},
            'a', 1,
            {'b' => 2})
    end

    it 'works if the key does not exist' do
      check({'a' => 1, 'b' => 2},
            'c', nil,
            {'a' => 1, 'b' => 2})
    end

    it 'fetches nested hashes' do
      check({'a' => 1, 'b' => { 'c' => 1, 'd' => 2 }},
            'b.d', 2,
            {'a' => 1, 'b' => { 'c' => 1 }})
    end

    it 'fetches deeply nested hashes' do
      check({'a' => 1, 'b' => { 'c' => { 'e' => 8, 'f' => 9 }, 'd' => 2 }},
            'b.c.e', 8,
            {'a' => 1, 'b' => { 'c' => { 'f' => 9 }, 'd' => 2 }})
    end

    it 'cleans up empty hashes' do
      check({'a' => { 'b' => 4}},
            'a.b', 4,
            {})
      check({'a' => { 'b' => { 'c' => 5 }, 'd' => 9}},
            'a.b.c', 5,
            {'a' => { 'd' => 9 }})
    end

    it 'recursively cleans' do
      check({'a' => { 'b' => { 'c' => { 'd' => 99 }}}},
            'a.b.c.d', 99,
            {})
    end

    it 'handles missing path components' do
      check({'a' => { 'c' => 4 }},
            'a.b.c.d', nil,
            {'a' => { 'c' => 4 }})
    end
  end

  describe 'when handling a map with aliases' do
  ALIAS_MAP = <<EOF
---
db:
  :meta:
    :alias: db_[0-9]+
  collection:
    :meta:
      :table: sqltable
    :columns:
      - _id: TEXT
      - var: INTEGER
EOF
    before do
      @map = MoSQL::Schema.new(YAML.load(ALIAS_MAP))
    end

    it 'can look up collections by aliases' do
      ns = @map.find_ns("db.collection")
      assert_equal(ns, @map.find_ns("db_00.collection"))
      assert_equal(ns, @map.find_ns("db_01.collection"))
    end

    it 'caches negative lookups' do
      assert_equal(nil, @map.find_ns("nosuchdb.foo"))
      assert(@map.instance_variable_get(:@map).key?("nosuchdb"))
    end

    it 'can do lookups after a negative cache' do
      @map.find_ns("nosuchdb.foo")
      assert_nil(@map.find_ns("otherdb.collection"))
    end
  end

  describe 'parsing magic source values' do
  OTHER_MAP = <<EOF
---
db:
  collection:
    :meta:
      :table: a_table
    :columns:
      - _id: TEXT
      - mosql_created:
        :source: $timestamp
        :type: timestamp
  existence:
    :meta:
      :table: b_table
    :columns:
      - _id: TEXT
      - has_foo:
        :source: $exists foo
        :type: BOOLEAN
      - has_foo_bar:
        :source: $exists foo.bar
        :type: BOOLEAN
  exists_and_value:
    :meta:
      :table: c_table
    :columns:
      - _id: TEXT
      - foo: TEXT
      - has_foo:
        :source: $exists foo
        :type: BOOLEAN
  invalid:
    :meta:
      :table: invalid
    :columns:
      - _id: TEXT
      - magic:
        :source: $magic
        :type: timestamp
EOF

    before do
      @othermap = MoSQL::Schema.new(YAML.load(OTHER_MAP))
    end

    it 'translates $timestamp' do
      r = @othermap.transform('db.collection', { '_id' => 'a' })
      assert_equal(['a', Sequel.function(:now)], r)
    end

    it 'translates $exists' do
      r = @othermap.transform('db.existence', { '_id' => 'a' })
      assert_equal(['a', false, false], r)
      r = @othermap.transform('db.existence', { '_id' => 'a', 'foo' => nil })
      assert_equal(['a', true, false], r)
      r = @othermap.transform('db.existence', { '_id' => 'a', 'foo' => {} })
      assert_equal(['a', true, false], r)
      r = @othermap.transform('db.existence', { '_id' => 'a', 'foo' => {'bar' => nil} })
      assert_equal(['a', true, true], r)
      r = @othermap.transform('db.existence', { '_id' => 'a', 'foo' => {'bar' => 42} })
      assert_equal(['a', true, true], r)
    end

    it 'can get $exists and value' do
      r = @othermap.transform('db.exists_and_value', { '_id' => 'a' })
      assert_equal(['a', nil, false], r)
      r = @othermap.transform('db.exists_and_value', { '_id' => 'a', 'foo' => nil })
      assert_equal(['a', nil, true], r)
      r = @othermap.transform('db.exists_and_value', { '_id' => 'a', 'foo' => 'xxx' })
      assert_equal(['a', 'xxx', true], r)
    end

    it 'rejects unknown specials' do
      assert_raises(MoSQL::SchemaError) do
        r = @othermap.transform('db.invalid', { '_id' => 'a' })
      end
    end
  end

  describe 'dotted names' do
    MAP = <<EOF
db:
  my.collection:
    :meta:
      :table: table
    :columns:
      - _id: TEXT
EOF

    it 'handles dotted names' do
      @map = MoSQL::Schema.new(YAML.load(MAP))
      collections = @map.collections_for_mongo_db('db')
      assert(collections.include?('my.collection'),
        "#{collections} doesn't include `my.collection`")
      assert(@map.find_ns('db.my.collection'))
    end
  end
end
