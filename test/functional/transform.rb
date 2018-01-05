require File.join(File.dirname(__FILE__), '_lib.rb')
require 'mosql/cli'

class MoSQL::Test::Functional::TransformTest < MoSQL::Test::Functional
  TESTCASES = [
    [
      BSON::ObjectId.from_string('5405fae77c584947fc000001'),
      'TEXT',
      '5405fae77c584947fc000001'
    ],
    [
      Time.utc(2006,01,02, 15,04,05,678000),
      'TIMESTAMP',
      Time.utc(2006,01,02, 15,04,05,678000)
    ],
    [
      :stringy,
      'TEXT',
      'stringy'
    ],
    [
      Mongo::DBRef.new('db.otherns', BSON::ObjectId.from_string('5405fae77c584947fc000001')).id,
      'TEXT',
      '5405fae77c584947fc000001'
    ],
    [
      [
        BSON::ObjectId.from_string('5405fae77c584947fc000001'),
        BSON::ObjectId.from_string('5405fae77c584947fc000002')
      ],
      'TEXT ARRAY',
      ['5405fae77c584947fc000001', '5405fae77c584947fc000002']
    ],
    [
      [
        BSON::ObjectId.from_string('5405fae77c584947fc000001'),
        BSON::ObjectId.from_string('5405fae77c584947fc000002')
      ],
      'TEXT',
      ['5405fae77c584947fc000001', '5405fae77c584947fc000002'].to_json,
    ],
    [
      [
        Mongo::DBRef.new('db.otherns', BSON::ObjectId.from_string('5405fae77c584947fc000001')).id,
        Mongo::DBRef.new('db.otherns', BSON::ObjectId.from_string('5405fae77c584947fc000002')).id
      ],
      'TEXT ARRAY',
      ['5405fae77c584947fc000001', '5405fae77c584947fc000002']
    ],
    [
      [
        Mongo::DBRef.new('db.otherns', BSON::ObjectId.from_string('5405fae77c584947fc000001')).id,
        Mongo::DBRef.new('db.otherns', BSON::ObjectId.from_string('5405fae77c584947fc000002')).id
      ],
      'TEXT',
      ['5405fae77c584947fc000001', '5405fae77c584947fc000002'].to_json
    ],
    [
      BSON::Binary.new(["2d931510d99f494a8c6787feb05e1594"].pack("H*"), :uuid),
      'UUID',
      "2d931510-d99f-494a-8c67-87feb05e1594"
    ],
    [
      BSON::Binary.new(["deadbeefcafebabe"].pack("H*"), :generic),
      'BYTEA',
      ["deadbeefcafebabe"].pack("H*")
    ]
  ]

  TESTCASES.each do |mongo, typ, sql|
    it "Can transform a #{mongo.class} into a #{typ}" do
      map = {'test' => {'test_transform' =>
          {
            meta: {
              table: 'test_transform'
            },
            columns: [
              {'_id'   => 'TEXT'},
              {'value' => typ},
            ]
          }}}
      schema = MoSQL::Schema.new(map)
      adapter = MoSQL::SQLAdapter.new(schema, sql_test_uri)
      @sequel.drop_table?(:test_transform)
      collection = @mongo.use('test')['test_transform']
      collection.drop

      schema.create_schema(@sequel)
      streamer = MoSQL::Streamer.new(:mongo => self.mongo,
        :tailer => nil,
        :options => {skip_tail: true},
        :sql => adapter,
        :schema => schema)

      # Test initial import
      id = 'imported'
      collection.insert_one({_id: id, value: mongo})
      streamer.initial_import

      got = @sequel[:test_transform].where(_id: id).to_a
      assert_equal(sql, got.first[:value], "was able to transform a #{typ} field on initial import")

      # Test streaming an insert
      id = 'inserted'
      collection.insert_one({_id: id, value: mongo})
      streamer.handle_op(
        {
          "ts" => {"t" => 1408647630, "i" => 4},
          "h"  => -965650193548512059,
          "v"  => 2,
          "op" => "i",
          "ns" => "test.test_transform",
          "o"  => collection.find(_id: id).first
        })

      got = @sequel[:test_transform].where(_id: id).to_a
      assert_equal(sql, got.first[:value], "was able to transform a #{typ} field while streaming")
    end
  end
end
