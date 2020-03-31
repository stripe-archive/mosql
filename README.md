# MoSQL: a MongoDB → SQL streaming translator

> _**MoSQL is no longer being actively maintained.**_
> _If you are interested in helping maintain this repository, please let us know.  We would love for it to find a forever home with someone who can give it the love it needs!_

At Stripe, we love MongoDB. We love the flexibility it gives us in
changing data schemas as we grow and learn, and we love its
operational properties. We love replsets. We love the uniform query
language that doesn't require generating and parsing strings, tracking
placeholder parameters, or any of that nonsense.

The thing is, we also love SQL. We love the ease of doing ad-hoc data
analysis over small-to-mid-size datasets in SQL. We love doing JOINs
to pull together reports summarizing properties across multiple
datasets. We love the fact that virtually every employee we hire
already knows SQL and is comfortable using it to ask and answer
questions about data.

So, we thought, why can't we have the best of both worlds? Thus:
MoSQL.

# MoSQL: Put Mo' SQL in your NoSQL

![MoSQL](https://stripe.com/img/blog/posts/mosql/mosql.png)

MoSQL imports the contents of your MongoDB database cluster into a
PostgreSQL instance, using an oplog tailer to keep the SQL mirror live
up-to-date. This lets you run production services against a MongoDB
database, and then run offline analytics or reporting using the full
power of SQL.

## Installation

Install from Rubygems as:

    $ gem install mosql

Or build from source by:

    $ gem build mosql.gemspec

And then install the built gem.

## The Collection Map file

In order to define a SQL schema and import your data, MoSQL needs a
collection map file describing the schema of your MongoDB data. (Don't
worry -- MoSQL can handle it if your mongo data doesn't always exactly
fit the stated schema. More on that later).

The collection map is a YAML file describing the databases and
collections in Mongo that you want to import, in terms of their SQL
types. An example collection map might be:


    mongodb:
      blog_posts:
        :columns:
        - id:
          :source: _id
          :type: TEXT
        - author_name:
          :source: author.name
          :type: TEXT
        - author_bio:
          :source: author.bio
          :type: TEXT
        - title: TEXT
        - created: DOUBLE PRECISION
        :meta:
          :table: blog_posts
          :extra_props: true

Said another way, the collection map is a YAML file containing a hash
mapping

    <Mongo DB name> -> { <Mongo Collection Name> -> <Collection Definition> }

Where a `<Collection Definition>` is a hash with `:columns` and
`:meta` fields.

`:columns` is a list of hashes mapping SQL column names to an hash
describing that column. This hash may contain the following fields:

  * `:source`: The name of the attribute inside of MongoDB.
  * `:type`: (Mandatory) The SQL type.


Use of the `:source` attribute allows for renaming attributes, and
extracting elements of a nested hash using MongoDB's
[dot notation][dot-notation]. In the above example, the `name` and
`bio` fields of the `author` sub-document will be expanded, and the
MongoDB `_id` field will be mapped to an SQL `id` column.

At present, MoSQL does not support using the dot notation to access
elements inside arrays.

As a shorthand, you can specify a one-element hash of the form `name:
TYPE`, in which case `name` will be used for both the source attribute
and the name of the destination column. You can see this shorthand for
the `title` and `created` attributes, above.

Every defined collection must include a mapping for the `_id`
attribute.

`:meta` contains metadata about this collection/table. It is
required to include at least `:table`, naming the SQL table this
collection will be mapped to. `extra_props` determines the handling of
unknown fields in MongoDB objects -- more about that later.

By default, `mosql` looks for a collection map in a file named
`collections.yml` in your current working directory, but you can
specify a different one with `-c` or `--collections`.

[dot-notation]: http://docs.mongodb.org/manual/core/document/#dot-notation

## Usage

Once you have a collection map. MoSQL usage is easy. The basic form
is:

    mosql [-c collections.yml] [--sql postgres://sql-server/sql-db] [--mongo mongodb://mongo-uri]

By default, `mosql` connects to both PostgreSQL and MongoDB instances
running on default ports on localhost without authentication. You can
point it at different targets using the `--sql` and `--mongo`
command-line parameters.

`mosql` will:

 1. Create the appropriate SQL tables
 2. Import data from the Mongo database
 3. Start tailing the mongo oplog, propagating changes from MongoDB to SQL.


After the first run, `mosql` will store the status of the optailer in
the `mongo_sql` table in your SQL database, and automatically resume
where it left off. `mosql` uses the replset name to keep track of
which mongo database it's tailing, so that you can tail multiple
databases into the same SQL database. If you want to tail the same
replSet, or multiple replSets with the same name, for some reason, you
can use the `--service` flag to change the name `mosql` uses to track
state.

You likely want to run `mosql` against a secondary node, at least for
the initial import, which will cause large amounts of disk activity on
the target node. One option is to specify this in your connect URI:

    mosql --mongo mongodb://node1,node2,node3?readPreference=secondary

(Note that this requires you be using at least version 1.8.3 of
`mongo-ruby-driver`)

## Advanced usage

For advanced scenarios, you can pass options to control mosql's
behavior. If you pass `--skip-tail`, mosql will do the initial import,
but not tail the oplog. This could be used, for example, to do an
import off of a backup snapshot, and then start the tailer on the live
cluster. This can also be useful for hosted services where you do not
have access to the oplog.

If you need to force a fresh reimport, run `--reimport`, which will
cause `mosql` to drop tables, create them anew, and do another import.

Normaly, MoSQL will scan through a list of the databases on the mongo
server you connect to. You avoid this behavior by specifiying a specific
mongo db to connect to with the `--only-db [dbname]` option. This is
useful for hosted services which do not let you list all databases (via
the `listDatabases` command).

## Schema mismatches and _extra_props

If MoSQL encounters values in the MongoDB database that don't fit
within the stated schema (e.g. a floating-point value in a INTEGER
field), it will log a warning, ignore the entire object, and continue.

If it encounters a MongoDB object with fields not listed in the
collection map, it will discard the extra fields, unless
`:extra_props` is set in the `:meta` hash. If it is, it will collect
any missing fields, JSON-encode them in a hash, and store the
resulting text in `_extra_props` in SQL. You can set `:extra_props`
to use `JSON`, `JSONB`, or `TEXT`.

As of PostgreSQL 9.3, you can declare columns as type "JSON" and use
the [native JSON support][pg-json] to inspect inside of JSON-encoded
types. In earlier versions, you can write code in an extension
language, such as [plv8][plv8].

[pg-json]: http://www.postgresql.org/docs/9.3/static/functions-json.html

## Non-scalar types

MoSQL supports array types, using the `INTEGER ARRAY` array type
syntax. This will cause MoSQL to create the column as an array type in
PostgreSQL, and insert rows appropriately-formatted.

Fields with hash values, or array values that are not in an
ARRAY-typed column, will be transformed into JSON TEXT strings before
being inserted into PostgreSQL.

[plv8]: http://code.google.com/p/plv8js/

## Authentication

At present, in order to use MoSQL with a MongoDB instance requiring
authentication, you must:

- Have a user with access to the admin database.
- Specify the `admin` database in the `--mongo` argument
- Specify the username and password in the `--mongo` argument

e.g.

```
mosql --mongo mongodb://$USER:$PASSWORD@$HOST/admin
```

In order to use MongoDB 2.4's "roles" support (which is different from that in
2.6), you need to create the user in the admin database, give it explicit read
access to the databases you want to copy *and* to the `local` database, and
specify authSource in the URL.  eg, connect to `mydb/admin` with the mongo shell
and run:

```
> db.addUser({user: "replicator", pwd: "PASSWORD", roles: [], otherDBRoles: {local: ["read"], sourceDb: ["read"]}})
```

(Note that `roles: []` ensures that this user has no special access to the
`admin` database.)  Now specify:

```
mosql --mongo mongodb://$USER:$PASSWORD@$HOST/sourceDb?authSource=admin
```

I have not yet tested using MoSQL with 2.6's rewritten "roles" support. Drop me
a note if you figure out anything I should know.

## Sharded clusters

MoSQL does not have special support for sharded Mongo clusters at this
time. It should be possible to run a separate MoSQL instance against
each of the individual backend shard replica sets, streaming into
separate PostgreSQL instances, but we have not actually tested this
yet.

# Development

Patches and contributions are welcome! Please fork the project and
open a pull request on [github][github], or just report issues.

MoSQL includes a small but hopefully-growing test suite. It assumes a
running PostgreSQL and MongoDB instance on the local host. To run the
test suite, first install all of MoSQL's dependencies:
```shell
bundle install
```
Then, run the tests:
```shell
rake test
```
You can also point the suite at a different target via environment
variables; See `test/functional/_lib.rb` for more information.

[github]: https://github.com/stripe/mosql
