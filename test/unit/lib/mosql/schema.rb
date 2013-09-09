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
EOF

  before do
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
    assert_equal('id', @map.primary_sql_key_for_ns('db.collection'))
    assert_equal('_id', @map.primary_sql_key_for_ns('db.old_conf_syntax'))
  end

  it 'can create a SQL schema' do
    db = stub()
    db.expects(:create_table?).with('sqltable')
    db.expects(:create_table?).with('sqltable2')
    db.expects(:create_table?).with('sqltable3')

    @map.create_schema(db)
  end

  it 'creates a SQL schema with the right fields' do
    db = {}
    stub_1 = stub()
    stub_1.expects(:column).with('id', 'TEXT')
    stub_1.expects(:column).with('var', 'INTEGER')
    stub_1.expects(:column).with('str', 'TEXT')
    stub_1.expects(:column).with('_extra_props').never
    stub_1.expects(:primary_key).with([:id])
    stub_2 = stub()
    stub_2.expects(:column).with('id', 'TEXT')
    stub_2.expects(:column).with('_extra_props', 'TEXT')
    stub_2.expects(:primary_key).with([:id])
    stub_3 = stub()
    stub_3.expects(:column).with('_id', 'TEXT')
    stub_3.expects(:column).with('_extra_props').never
    stub_3.expects(:primary_key).with([:_id])
    (class << db; self; end).send(:define_method, :create_table?) do |tbl, &blk|
      case tbl
      when "sqltable"
        o = stub_1
      when "sqltable2"
        o = stub_2
      when "sqltable3"
        o = stub_3
      else
        assert(false, "Tried to create an unexpected table: #{tbl}")
      end
      o.instance_eval(&blk)
    end
    @map.create_schema(db)
  end

  describe 'when transforming' do
    it 'transforms rows' do
      out = @map.transform('db.collection', {'_id' => "row 1", 'var' => 6, 'str' => 'a string'})
      assert_equal(["row 1", 6, 'a string'], out)
    end

    it 'Includes extra props' do
      out = @map.transform('db.with_extra_props', {'_id' => 7, 'var' => 6, 'other var' => {'key' => 'value'}})
      assert_equal(2, out.length)
      assert_equal(7, out[0])
      assert_equal({'var' => 6, 'other var' => {'key' => 'value'}}, JSON.parse(out[1]))
    end

    it 'gets all_columns right' do
      assert_equal(['id', 'var', 'str'], @map.all_columns(@map.find_ns('db.collection')))
      assert_equal(['id', '_extra_props'], @map.all_columns(@map.find_ns('db.with_extra_props')))
    end

    it 'stringifies symbols' do
      out = @map.transform('db.collection', {'_id' => "row 1", 'str' => :stringy})
      assert_equal(["row 1", nil, 'stringy'], out)
    end

    it 'changes NaN to null in extra_props' do
      out = @map.transform('db.with_extra_props', {'_id' => 7, 'nancy' => 0.0/0.0})
      extra = JSON.parse(out[1])
      assert(extra.key?('nancy'))
      assert_equal(nil, extra['nancy'])
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
  end
end
