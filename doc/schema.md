# Schema

In databases there are two approaches for imposing integrity constraints on
structured data collections: _schema-on-read_ or _schema-on-write_.
The _schema-on-read_ approach assumes an implicit structure of the data where
the structure is only interpreted at read level. Document databases like
[MongoDB](https://www.mongodb.com/) or key value stores like
[Redis](https://redis.io/) are examples for this kind of schema flexibility. In
contrast, the _schema-on-write_ approach has an explicit assumption of the data
model where the database ensures that all data written is conform to a defined
data model. The traditional relational databases like
[PostgreSQL](https://www.postgresql.org/) as well as modern column-based
databases like [Cassandra](https://cassandra.apache.org/) fall under this
category.

Datahike supports both approaches which can be chosen at creation time but can
not be changed afterwards.

Have a look at the `core`-namespace in the `examples/store-intro` folder for
example configuration and transactions.

## Schema-on-read

By inheriting most of the code from
[Datascript](https://github.com/tonsky/datascript) the default approach was
_schema-on-read_ where you could add any arbitrary Clojure data structures to
the database with a small set of helper definitions that added information
about references and cardinality. Even though Datahike's API moved to a
_schema-on-write_ approach, the schema-less behavior is still supported. On
database creation you may opt out by setting the `:schema-flexibility` parameter to `:read`.

```clojure
(require '[datahike.api :as d])

(def cfg {:store {:backend :mem :id "schemaless"} :schema-flexibility :read})

(d/create-database cfg)

(def conn (d/connect cfg))

;; now you can add any arbitrary data
(d/transact conn {:tx-data [{:any "Data"}]})
```

## Schema-on-write

With the release of version `0.2.0` Datahike enforces by default an explicit
schema where you have to define your expected data shapes in advance. The
schema itself is present in the database index, so you can simply transact it
like any other datom.

```clojure
(require '[datahike.api :as d])

;; since the :write approach is the default value we may also skip the setting
(def cfg {:store {:backend :mem :id "schema-on-write"} :schema-flexibility :write})

(d/create-database cfg)

(def conn (d/connect cfg))

;; define a simple schema
(def schema [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

;; transact it
(d/transact conn {:tx-data schema})

;; now we can transact data based on the provided schema
(d/transact conn {:tx-data [{:name "Alice"}]})
```

The schema definition is for the most part compliant with Datomic's
[approach](https://docs.datomic.com/on-prem/schema.html). Required are three
attributes:

- `:db/ident`: the name of the attribute, defined as a keyword with optional
  namespace, e.g. `:user/name`
- `:db/valueType`: the type of the value associated with an attribute, e.g. `db.type/string`, see
  [below](#supported-value-types) for supported types
- `:db/cardinality`: the cardinality of the value, whether the value is a single
  value or a set of values, can be either `:db.cardinality/one` or `db.cardinality/many`

Additionally, the following optional attributes are supported:

- `db/doc`: the documentation for the attribute as a string
- `db/unique`: a uniqueness constraint on the attribute for a given value, can
  be either `db.unique/value` (only one entity with this attribute can have the same value) or `db.unique/identity`(only one entity can have
  the value for this attribute with upsert enabled)
- `db/index`: indicates whether an index for the attribute's value should be
  created as a boolean
- `db/isComponent`: indicates that an attribute of type `:db.type/ref` references a subcomponent of the entity that has the attribute (for cascading retractions)
- if `:db/valueType` is `:db.type/tuple`, one of:
  - `db/tupleAttrs`: a collection of attributes that make up the tuple (for [composite tuples](https://docs.datomic.com/on-prem/schema/schema.html#composite-tuples))
  - `db/tupleTypes`: a collection of 2-8 types that make up the tuple (for [heterogeneous fixed length tuples](https://docs.datomic.com/on-prem/schema/schema.html#heterogeneous-tuples))
  - `db/tupleType`: the type of the tuple elements (for [homogeneous variable length tuples](https://docs.datomic.com/on-prem/schema/schema.html#homogeneous-tuples))

### Supported value types

The following types are currently support in datahike:

| Value Type        | Corresponding Type   |
| ----------------- | -------------------- |
| `db.type/bigdec`  | java.math.BigDecimal |
| `db.type/bigint`  | java.math.BigInteger |
| `db.type/boolean` | Boolean              |
| `db.type/double`  | Double               |
| `db.type/float`   | Double or Float      |
| `db.type/instant` | java.util.Date       |
| `db.type/keyword` | clojure.lang.Keyword |
| `db.type/long`    | java.lang.Long       |
| `db.type/ref`     | java.lang.Long       |
| `db.type/string`  | String               |
| `db.type/symbol`  | clojure.lang.Symbol  |
| `db.type/uuid`    | java.util.UUID       |
| `db.type/tuple`   | clojure.lang.Vector  |

The schema is validated using [clojure.spec](https://clojure.org/guides/spec).
See `src/datahike/schema.cljc` for the implementation details.

### Migration

Updating an existing schema is discouraged as it may lead to inconsistencies
in your data. Therefore, only schema updates for `db.cardinality` and `db.unique`
are supported. Rather than updating an existing attribute, it is recommended to create
a new attribute and migrate data accordingly. Alternatively, if you want to maintain your
old attribute names, export your data except the schema, transform it to the new
schema, create a new database with the new schema, and import the transformed data.
