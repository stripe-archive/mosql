require File.join(File.dirname(__FILE__), '_lib.rb')
require 'mosql/cli'

class MoSQL::Test::Functional::StreamerTest < MoSQL::Test::Functional
  def build_streamer
    MoSQL::Streamer.new(:mongo => mongo,
                        :tailer => nil,
                        :options => {},
                        :sql => @adapter,
                        :schema => @map)
  end

  describe 'with a basic schema' do
    TEST_MAP = <<EOF
---
mosql_test:
  collection:
    :meta:
      :table: sqltable
    :columns:
      - _id: TEXT
      - var: INTEGER
      - arry: INTEGER ARRAY
  renameid:
    :meta:
      :table: sqltable2
    :columns:
      - id:
        :source: _id
        :type: TEXT
      - goats: INTEGER

filter_test:
  collection:
    :meta:
      :table: filter_sqltable
      :filter:
        :_id:
          '$gte': !ruby/object:BSON::ObjectId
            data:
            - 83
            - 179
            - 75
            - 128
            - 0
            - 0
            - 0
            - 0
            - 0
            - 0
            - 0
            - 0
    :columns:
      - _id: TEXT
      - var: INTEGER

composite_key_test:
  collection:
    :meta:
      :table: composite_table
      :composite_key:
        - store
        - time
    :columns:
      - store:
        :source: _id.s
        :type: TEXT
      - time:
        :source: _id.t
        :type: TIMESTAMP
      - var: TEXT
EOF

    before do
      @map = MoSQL::Schema.new(YAML.load(TEST_MAP))
      @adapter = MoSQL::SQLAdapter.new(@map, sql_test_uri)

      @sequel.drop_table?(:sqltable)
      @sequel.drop_table?(:sqltable2)
      @sequel.drop_table?(:composite_table)
      @map.create_schema(@sequel)

      @streamer = build_streamer
    end

    it 'handle "u" ops without _id' do
      o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
      @adapter.upsert_ns('mosql_test.collection', o)

      @streamer.handle_op({ 'ns' => 'mosql_test.collection',
                            'op' => 'u',
                            'o2' => { '_id' => o['_id'] },
                            'o'  => { 'var' => 27 }
                          })
      assert_equal(27, sequel[:sqltable].where(:_id => o['_id'].to_s).select.first[:var])
    end

    it 'handle "u" ops without _id and a renamed _id mapping' do
      o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
      @adapter.upsert_ns('mosql_test.renameid', o)

      @streamer.handle_op({ 'ns' => 'mosql_test.renameid',
                            'op' => 'u',
                            'o2' => { '_id' => o['_id'] },
                            'o'  => { 'goats' => 27 }
                          })
      assert_equal(27, sequel[:sqltable2].where(:id => o['_id'].to_s).select.first[:goats])
    end

    it 'applies ops performed via applyOps' do
      o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
      @adapter.upsert_ns('mosql_test.collection', o)

      op = { 'ns' => 'mosql_test.collection',
             'op' => 'u',
             'o2' => { '_id' => o['_id'] },
             'o'  => { 'var' => 27 }
           }
      @streamer.handle_op({ 'op' => 'c',
                            'ns' => 'mosql_test.$cmd',
                            'o' => { 'applyOps' => [op] }
                          })
      assert_equal(27, sequel[:sqltable].where(:_id => o['_id'].to_s).select.first[:var])
    end

    it 'handle "d" ops with BSON::ObjectIds' do
      o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
      @adapter.upsert_ns('mosql_test.collection', o)

      @streamer.handle_op({ 'ns' => 'mosql_test.collection',
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
      mongo.use('mosql_test')['collection'].insert_one(o.merge('var' => 100),
                                                               :w => 1)

      @streamer.handle_op({ 'ns' => 'mosql_test.collection',
                            'op' => 'u',
                            'o2' => { '_id' => o['_id'] },
                            'o'  => { '$set' => { 'var' => 100 } },
                          })
      assert_equal(100, sequel[:sqltable].where(:_id => o['_id'].to_s).select.first[:var])
    end

    it 'handle "u" ops with $set, BSON::ObjectID, and a deleted row' do
      o = { '_id' => BSON::ObjectId.new, 'var' => 17 }
      @adapter.upsert_ns('mosql_test.collection', o)

      # Don't store the row in mongo, which will cause the 'u' op to
      # delete it from SQL.

      @streamer.handle_op({ 'ns' => 'mosql_test.collection',
                            'op' => 'u',
                            'o2' => { '_id' => o['_id'] },
                            'o'  => { '$set' => { 'var' => 100 } },
                          })
      assert_equal(0, sequel[:sqltable].count(:_id => o['_id'].to_s))
    end

    it 'handle "u" ops with $set and a renamed _id' do
      o = { '_id' => BSON::ObjectId.new, 'goats' => 96 }
      @adapter.upsert_ns('mosql_test.renameid', o)

      # $set's are currently a bit of a hack where we read the object
      # from the db, so make sure the new object exists in mongo
      #connect_mongo['mosql_test'].insert(o.merge('goats' => 0),
      #mongo['mosql_test'].insert_one(o.merge('goats' => 0),
      mongo.use('mosql_test')['renameid'].insert_one(o.merge('goats' => 0),
                                                             :w => 1)

      @streamer.handle_op({ 'ns' => 'mosql_test.renameid',
                            'op' => 'u',
                            'o2' => { '_id' => o['_id'] },
                            'o'  => { '$set' => { 'goats' => 0 } },
                          })
      assert_equal(0, sequel[:sqltable2].where(:id => o['_id'].to_s).select.first[:goats])
    end

    it 'handles "d" ops with a renamed id' do
      o = { '_id' => BSON::ObjectId.new, 'goats' => 1 }
      @adapter.upsert_ns('mosql_test.renameid', o)

      @streamer.handle_op({ 'ns' => 'mosql_test.renameid',
                            'op' => 'd',
                            'o' => { '_id' => o['_id'] },
                          })
      assert_equal(0, sequel[:sqltable2].where(:id => o['_id'].to_s).count)
    end

    it 'filters unwanted records' do
      data = [{:_id => BSON::ObjectId.from_time(Time.utc(2014, 7, 1)), :var => 2},
              {:_id => BSON::ObjectId.from_time(Time.utc(2014, 7, 2)), :var => 3}]
      collection = mongo.use('filter_test')['collection']
      collection.drop
      data.map { |rec| collection.insert_one(rec)}

      @streamer.options[:skip_tail] = true
      @streamer.initial_import

      inserted_records = @sequel[:filter_sqltable].select
      assert_equal(1, inserted_records.count)
      record = inserted_records.first
      data[1][:_id] = data[1][:_id].to_s
      assert_equal(data[1], record)
    end

    it 'handles "u" ops with a compsite key' do
      date = Time.utc(2014, 7, 1)
      o = {'_id' => {'s' => 'asdf', 't' => date}, 'var' => 'data'}
      collection = mongo.use('composite_key_test')['collection']
      collection.drop
      collection.insert_one(o)

      @streamer.options[:skip_tail] = true
      @streamer.initial_import

      collection.update_one({ '_id' => { 's' => 'asdf', 't' => date}}, { '$set' => { 'var' => 'new_data'}})
      @streamer.handle_op({'ns' => 'composite_key_test.collection',
                           'op' => 'u',
                           'o2' => { '_id' => { 's' => 'asdf', 't' => date}},
                           'o'  => { '$set' => { 'var' => 'new_data'}}
                           })

      assert_equal(0, @sequel[:composite_table].where(:var => "data").count)
      assert_equal(1, @sequel[:composite_table].where(:var => "new_data").count)
    end

    it 'handles composite keys' do
      o = {'_id' => {'s' => 'asdf', 't' => Time.new}, 'var' => 'data'}
      collection = mongo.use('composite_key_test')['collection']
      collection.drop
      collection.insert_one(o)

      @streamer.options[:skip_tail] = true
      @streamer.initial_import

      assert_equal(1, @sequel[:composite_table].count)
    end

    describe '.bulk_upsert' do
      it 'inserts multiple rows' do
        objs = [
                { '_id' => BSON::ObjectId.new, 'var' => 0 },
                { '_id' => BSON::ObjectId.new, 'var' => 1, 'arry' => [1, 2] },
                { '_id' => BSON::ObjectId.new, 'var' => 3 },
               ].map { |o| @map.transform('mosql_test.collection', o) }

        @streamer.bulk_upsert(sequel[:sqltable], 'mosql_test.collection',
                              objs)

        assert(sequel[:sqltable].where(:_id => objs[0].first, :var => 0).count)
        assert(sequel[:sqltable].where(:_id => objs[1].first, :var => 1).count)
        assert(sequel[:sqltable].where(:_id => objs[2].first, :var => 3).count)
      end

      it 'upserts' do
        _id = BSON::ObjectId.new
        objs = [
                { '_id' => _id, 'var' => 0 },
                { '_id' => BSON::ObjectId.new, 'var' => 1 },
                { '_id' => BSON::ObjectId.new, 'var' => 3 },
               ].map { |o| @map.transform('mosql_test.collection', o) }

        @streamer.bulk_upsert(sequel[:sqltable], 'mosql_test.collection',
                              objs)

        newobjs = [
                   { '_id' => _id, 'var' => 117 },
                   { '_id' => BSON::ObjectId.new, 'var' => 32 },
                  ].map { |o| @map.transform('mosql_test.collection', o) }
        @streamer.bulk_upsert(sequel[:sqltable], 'mosql_test.collection',
                              newobjs)


        assert(sequel[:sqltable].where(:_id => newobjs[0].first, :var => 117).count)
        assert(sequel[:sqltable].where(:_id => newobjs[1].first, :var => 32).count)
      end

      describe 'when working with --unsafe' do
        it 'raises on error by default' do
          assert_raises(Sequel::DatabaseError) do
            @streamer.handle_op({ 'ns' => 'mosql_test.collection',
                                  'op' => 'u',
                                  'o2' => { '_id' => 'a' },
                                  'o'  => { 'var' => 1 << 62 },
                                })
          end
        end

        it 'does not raises on error with :unsafe' do
          @streamer.options[:unsafe] = true
          @streamer.handle_op({ 'ns' => 'mosql_test.collection',
                                'op' => 'u',
                                'o2' => { '_id' => 'a' },
                                'o'  => { 'var' => 1 << 62 },
                              })
          assert_equal(0, sequel[:sqltable].where(:_id => 'a').count)
        end
      end
    end
  end

  describe 'when dealing with aliased dbs' do
  ALIAS_MAP = <<EOF
---
test:
  :meta:
    :alias: test_[0-9]+
  collection:
    :meta:
      :table: sqltable
    :columns:
      - _id: TEXT
      - var: INTEGER
EOF
    before do
      @map = MoSQL::Schema.new(YAML.load(ALIAS_MAP))
      @adapter = MoSQL::SQLAdapter.new(@map, sql_test_uri)

      @sequel.drop_table?(:sqltable)
      @map.create_schema(@sequel)

      @streamer = build_streamer
    end

    it 'imports from all dbs' do
      ids = (1.upto(4)).map { BSON::ObjectId.new }
      ids.each_with_index do |_id, i|
        collection = mongo.use("test_#{i}")['collection']
        collection.drop
        collection.insert_one({:_id => _id, :var => i}, :w => 1)
      end

      @streamer.options[:skip_tail] = true
      @streamer.initial_import

      sqlobjs = @sequel[:sqltable].select.to_a
      assert_equal(ids.map(&:to_s).sort, sqlobjs.map { |o| o[:_id] }.sort)
    end
  end
  describe 'timestamps' do
  TIMESTAMP_MAP = <<EOF
---
db:
  has_timestamp:
    :meta:
      :table: has_timestamp
    :columns:
      - _id: TEXT
      - ts: timestamp
EOF

    before do
      @map = MoSQL::Schema.new(YAML.load(TIMESTAMP_MAP))
      @adapter = MoSQL::SQLAdapter.new(@map, sql_test_uri)

      mongo.use('db')['has_timestamp'].drop
      @sequel.drop_table?(:has_timestamp)
      @map.create_schema(@sequel)

      @streamer = build_streamer
    end

    it 'preserves milliseconds on import' do
      ts = Time.utc(2014, 8, 7, 6, 54, 32, 123000)
      mongo.use('db')['has_timestamp'].insert_one({ts: ts})
      @streamer.options[:skip_tail] = true
      @streamer.initial_import

      row = @sequel[:has_timestamp].select.to_a
      assert_equal(1, row.length)
      assert_equal(ts.to_i, row.first[:ts].to_i)
      assert_equal(ts.tv_usec, row.first[:ts].tv_usec)
    end

    it 'preserves milliseconds on tailing' do
      ts = Time.utc(2006,01,02, 15,04,05,678000)
      id = mongo.use('db')['has_timestamp'].insert_one({ts: ts}).inserted_id
      @streamer.handle_op(
        {
          "ts" => {"t" => 1408647630, "i" => 4},
          "h"  => -965650193548512059,
          "v"  => 2,
          "op" => "i",
          "ns" => "db.has_timestamp",
          "o"  => mongo.use('db')['has_timestamp'].find({_id: id}).first
        })
      got = @sequel[:has_timestamp].where(:_id => id.to_s).select.first[:ts]
      assert_equal(ts.to_i, got.to_i)
      assert_equal(ts.tv_usec, got.tv_usec)
    end
 end
end
