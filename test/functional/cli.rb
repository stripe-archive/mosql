require File.join(File.dirname(__FILE__), '_lib.rb')
require 'mosql/cli'

class MoSQL::Test::Functional::CLITest < MoSQL::Test::Functional
  TEST_MAP = <<EOF
---
mosql_test:
  collection:
    :meta:
      :table: sqltable
    :columns:
      - _id: TEXT
      - var: INTEGER
EOF

  def fake_cli
    # This is a hack. We should refactor cli.rb to be more testable.
    MoSQL::CLI.any_instance.expects(:setup_signal_handlers)
    cli = MoSQL::CLI.new([])
    cli.instance_variable_set(:@mongo, mongo)
    cli.instance_variable_set(:@schemamap, @map)
    cli.instance_variable_set(:@sql, @adapter)
    cli.instance_variable_set(:@options, {})
    cli
  end

  before do
    @map = MoSQL::Schema.new(YAML.load(TEST_MAP))
    @adapter = MoSQL::SQLAdapter.new(@map, sql_test_uri)

    @sequel.drop_table?(:sqltable)
    @map.create_schema(@sequel)

    @cli = fake_cli
  end

  it 'handle "u" ops without _id' do
    o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
    @adapter.upsert_ns('mosql_test.collection', o)

    @cli.handle_op({ 'ns' => 'mosql_test.collection',
                     'op' => 'u',
                     'o2' => { '_id' => o['_id'] },
                     'o'  => { 'var' => 27 }
                   })
    assert_equal(27, sequel[:sqltable].where(:_id => o['_id'].to_s).select.first[:var])
  end

  it 'handle "d" ops with BSON::ObjectIds' do
    o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
    @adapter.upsert_ns('mosql_test.collection', o)

    @cli.handle_op({ 'ns' => 'mosql_test.collection',
                     'op' => 'd',
                     'o' => { '_id' => o['_id'] },
                   })
    assert_equal(0, sequel[:sqltable].where(:_id => o['_id'].to_s).count)
  end

  it 'handle "u" ops with $set and BSON::ObjectIDs' do
    o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
    @adapter.upsert_ns('mosql_test.collection', o)

    # $set's are currently a bit of a hack where we read the object
    # from the db, so make sure the new object exists in mongo
    connect_mongo['mosql_test']['collection'].insert(o.merge('var' => 100),
                                                     :w => 1)

    @cli.handle_op({ 'ns' => 'mosql_test.collection',
                     'op' => 'u',
                     'o2' => { '_id' => o['_id'] },
                     'o'  => { '$set' => { 'var' => 100 } },
                   })
    assert_equal(100, sequel[:sqltable].where(:_id => o['_id'].to_s).select.first[:var])
  end
end
