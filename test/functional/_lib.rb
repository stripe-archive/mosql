require File.join(File.dirname(__FILE__), "../_lib")

module MoSQL
  class Test::Functional < MoSQL::Test
    attr_reader :sequel, :mongo

    def sql_test_uri
      ENV['MONGOSQL_TEST_SQL'] || 'postgres:///test'
    end
    def mongo_test_uri
      ENV['MONGOSQL_TEST_MONGO'] || 'mongodb://localhost'
    end
    def mongo_test_dbname
      ENV['MONGOSQL_TEST_MONGO_DB'] || 'test'
    end

    def connect_sql
      begin
        conn = Sequel.connect(sql_test_uri)
        conn.extension :pg_array
        conn.test_connection
        conn
      rescue Sequel::DatabaseConnectionError
        $stderr.puts <<EOF

*********************************************************************
Unable to connect to PostgreSQL database at `#{sql_test_uri}'. Either
configure a PostgresSQL database running locally without
authentication with a 'test' database, or set \$MONGOSQL_TEST_SQL in
the environment.
*********************************************************************

EOF
        exit(1)
      end
    end

    def connect_mongo
      begin
        mongo = Mongo::Client.new(mongo_test_uri, { database: mongo_test_dbname } )
      rescue Mongo::Error
        $stderr.puts <<EOF

*********************************************************************
Unable to connect to MongoDB database at `#{mongo_test_uri}'. Either
configure a MongoDB database running on localhost without
authentication with a 'test' database, or set \$MONGOSQL_TEST_MONGO in
the environment.
*********************************************************************

EOF
        exit(1)
      end
    end

    def setup
      Sequel.default_timezone = :utc
      @sequel = connect_sql
      @mongo  = connect_mongo
      super
    end
  end
end
