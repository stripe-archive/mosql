# MoSQL: a MongoDB → SQL streaming translator

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
        - author:
          :source: author
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

This syntax allows to rename MongoDB's attributes during the
import. For example the MonogDB `_id` attribute will be transferred to
a SQL column named `id`.

As a shorthand, you can specify a one-elment hash of the form `name:
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
 3. Start tailing the mongo oplog, propogating changes from MongoDB to SQL.


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

    mosql --mongo mongodb://node1,node2,node3?slaveOk=true

(You should be able to specify `?readPreference=secondary`, but the
Mongo Ruby driver does not appear to support that usage. I've filed a
[bug with 10gen][bug-read-pref] about this omission).

[bug-read-pref]: https://jira.mongodb.org/browse/RUBY-547

## Advanced usage

For advanced scenarios, you can pass options to control mosql's
behavior. If you pass `--skip-tail`, mosql will do the initial import,
but not tail the oplog. This could be used, for example, to do an
import off of a backup snapshot, and then start the tailer on the live
cluster.

If you need to force a fresh reimport, run `--reimport`, which will
cause `mosql` to drop tables, create them anew, and do another import.


### Callbacks

It's possible to define special callback on a collection basis to allow the
execution of custom code whenever an item is inserted, updated or deleted from
MongoDB.

Callbacks are implemented by subclassing the `MoSQL::Callback' class and
defining one these methods:

  * `after_upsert(obj)`: this is called whenever a MongoDB object is inserted
    or updated. The callback is invoked after the object has been written to
    PostgreSQL.
    `obj` is a MongoDB object as seen from the oplog; hence it might contain
    also the attributes that are not being mapped to PostgreSQL's columns.
  * `after_delete(obj)`: this is called whenever an object is delted from
    MongoDB. The callback is invoked after the operation has been propagated to
    PostgreSQL.
    `obj` is the a MongoDB object as seen from the oplog; hence it might contain
    also the attributes that are not being mapped to PostgreSQL's columns.

The `MoSQL::Callback` class provides a `log` method which allows to use MoSQL's
logging facility.

Access to the PostgreSQL database can be obtained usign the `@sql` class intance
variable.

Callbacks must then be defined inside of the `meta` section of the collection
map file.

A possible use of callbacks is the creation of relational tables.

For example, consider a simple Rails application that includes a model for
customers and a model for orders. Each customer can have many orders.

Inside of MongoDB this relation is implemented using a `customer_id`
attribute inside of the `orders` collection.

Inside of PostgreSQL the `order` table is going to have two 'special' columns:

  * `customer_id`: this is going to store the SQL id of the customer.
  * `customer_mongodb_id`: this is going to store the `BSON::ObjectId` of
    the customer.

This is the configuration file used by mosql:

    db:
      customer:
        :columns:
        - _id: TEXT
        - name: TEXT
        :meta:
          :table: customers
          :import_order: 1
          :extra_props: false
      orders:
        :columns:
        - _id: TEXT
        - customer_mongodb_id:
          :source: customer_id
          :type: TEXT
        :meta:
          :table: orders
          :callback: my_callbacks/orders_callback
          :extra_props: false

The configuration file will automaticaly map `db.orders.customer_id` to the
`customer_mongodb_id` inside of PostgreSQL.

During the initial import of the database the `customers` table is going to
imported before the `orders` table. This is ensured using the `import_order = 1`
directive inside of the `meta` section. That is required to have the callback
work also during the initial import.

The callback class is defined inside of `my_callbacks/orders_callback`:

```ruby

  require 'mosql'

  class OrdersCallback < MoSQL::Callback
    def after_upsert(obj)
      customer_mongodb_id = obj['customer_id']
      return unless customer_mongodb_id

      customer_sql_id = @db[:repositories].where(
        :_id => customer_mongodb_id.to_s
      ).get(:id)
      unless customer_sql_id
        log.warn "Cannot find customer with _id #{customer_mongodb_id}"
        return
      end

      @db[:orders].where(
        :_id => obj['_id'].to_s
      ).update(
        :customer_id => customer_sql_id
      )
      log.debug "Fixed customer_id association for order with _id #{obj['_id']}"
    rescue Exception => e
      log.error(
        "Something went wrong with fixing the customer_id association for order " +
        "with _id #{obj['_id']}: #{e.to_s}"
      )
    end

  end

```

As you can see the callback takes care of findining and setting the SQL id of the
associated customer every time an order item is created or updated.


## Schema mismatches and _extra_props

If MoSQL encounters values in the MongoDB database that don't fit
within the stated schema (e.g. a floating-point value in a INTEGER
field), it will log a warning, ignore the entire object, and continue.

If it encounters a MongoDB object with fields not listed in the
collection map, it will discard the extra fields, unless
`:extra_props` is set in the `:meta` hash. If it is, it will collect
any missing fields, JSON-encode them in a hash, and store the
resulting text in `_extra_props` in SQL. It's up to you to do
something useful with the JSON. One option is to use [plv8][plv8] to
parse them inside PostgreSQL, or you can just pull the JSON out whole
and parse it in application code.

This is also currently the only way to handle array or object values
inside records -- specify `:extra_props`, and they'll get JSON-encoded
into `_extra_props`. There's no reason we couldn't support
JSON-encoded values for individual columns/fields, but we haven't
written that code yet.

[plv8]: http://code.google.com/p/plv8js/

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
running PostgreSQL and MongoDB instance on the local host; You can
point it at a different target via environment variables; See
`test/functional/_lib.rb` for more information.

[github]: https://github.com/stripe/mosql
