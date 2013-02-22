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
      - var:
        :source: var
        :type: INTEGER
  with_extra_props:
    :meta:
      :table: sqltable2
      :extra_props: true
    :columns:
      - id:
        :source: _id
        :type: INTEGER
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
    #assert_equal(['_id', 'var'], table[:columns].keys)
  end

  it 'can create a SQL schema' do
    db = stub()
    db.expects(:create_table?).with('sqltable')
    db.expects(:create_table?).with('sqltable2')

    @map.create_schema(db)
  end

  it 'creates a SQL schema with the right fields' do
    db = {}
    stub_1 = stub()
    stub_1.expects(:column).with('id', 'TEXT')
    stub_1.expects(:column).with('var', 'INTEGER')
    stub_1.expects(:column).with('_extra_props').never
    stub_2 = stub()
    stub_2.expects(:column).with('id', 'INTEGER')
    stub_2.expects(:column).with('_extra_props', 'TEXT')
    (class << db; self; end).send(:define_method, :create_table?) do |tbl, &blk|
      case tbl
      when "sqltable"
        o = stub_1
      when "sqltable2"
        o = stub_2
      else
        assert(false, "Tried to create an unexpeced table: #{tbl}")
      end
      o.expects(:primary_key).with([:id])
      o.instance_eval(&blk)
    end
    @map.create_schema(db)
  end

  describe 'when transforming' do
    it 'transforms rows' do
      out = @map.transform('db.collection', {'_id' => "row 1", 'var' => 6})
      assert_equal(["row 1", 6], out)
    end

    it 'Includes extra props' do
      out = @map.transform('db.with_extra_props', {'_id' => 7, 'var' => 6, 'other var' => {'key' => 'value'}})
      assert_equal(2, out.length)
      assert_equal(7, out[0])
      assert_equal({'var' => 6, 'other var' => {'key' => 'value'}}, JSON.parse(out[1]))
    end

    it 'gets all_columns right' do
      assert_equal(['id', 'var'], @map.all_columns(@map.find_ns('db.collection')))
      assert_equal(['id', '_extra_props'], @map.all_columns(@map.find_ns('db.with_extra_props')))
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
end
