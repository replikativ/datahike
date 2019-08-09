# Datahike database configuration

At database creation *datahike* supports features that can be be
configured based on the application's requirements. As of version `0.2.0`
 configuration for the [storage backend](#storage-backend), the [schema
flexibility](#schema-flexibility), and the 
[time variance](#time-variance) is supported.
Be aware: all these features can be set at database creation 
but can not be changed afterwards.

## Storage Backend
The backend configuration can be encoded within a base uri we can connect to. It has the
following scheme:

`datahike:<backend>://(<username>:<password>@)<host>:<port><path>`

Each backend needs a different set of provided parameters. See definition
[below](#storage-backend) for further information. For simple and fast creation
you can simple type:

```clojure
(:require '[datahike.api :as d])
(def uri "datahike:mem://example")
(d/create-database uri)
```

If you prefer data structures for configuration you may use a hash map. The example
above looks like:

```clojure
(:require '[datahike.api :as d])
(def config {:backend :mem :host "example"})
(d/create-database config)
```

At the moment we support four different backends: [in-memory](#in-memory) ,[file-based](#file-based),
[LevelDB](#leveldb), and [PosgreSQL](#postgresl). 

### in-memory
- `<backend>`: `mem`
- `host`: name of the database
- uri example: `datahike:mem://mem-example`
- hash map example: `{:backend :mem :host "mem-example"}`

### file-based
- `<backend>`: `file`
- `path`: absolute path to the storage folder
- uri example: `datahike:file:///tmp/file-example`
- hash map example: `{:backend :mem :host "mem-example"}`

### LevelDB
- `<backend>`: `level`
- `path`: absolute path to the LevelDB instance
- uri example: `datahike:file:///tmp/level-example`
- hash map example: `{:backend :mem :host "mem-example"}`

### PostgreSQL
- `<backend>`: `pg`
- `username`: PostgreSQL instance username
- `password`: PostgreSQL instance password
- `host`: PostgreSQL instance host
- `port`: PostgreSQL instance port
- `path`: name of the PostgreSQL database, must be present in the instance
- uri example: `datahike:pg://alice:foobar@localhost:5432/pg_example`
- hash map example: `{:backend :pg 
                      :host "localhost" 
                      :port 5432 
                      :username "alice" 
                      :password "foobar" 
                      :path "/pg_example"}`

## Schema Flexibility
Per default the datahike api uses a schema-on-write appoach with strict value
types that need to be defined in advance. If you are not sure how your data
model looks like and you want to transact any kind of data into the database you
can set `schema-on-read` to `true` by adding it as optional parameter at
database creation. You may add basic schema definitions like `:db/unique`,
`:db/cardinality` or `db.type/ref` where these kind of structure is needed.

```clojure
(:require '[datahike.api :as d])
(d/create-database "datahike:mem://example" :schema-on-read true)
```

Have a look at the [schema documation](./schema.md) for more information.

## Temporal Variance
Datahike has the capability to inspect and query historical data within temporal
indices. If your application does not require any temporal data, you may 
set `:temporal-index` to `false`. 

```clojure
(:require '[datahike.api :as d])
(d/create-database "datahike:mem://example" :temporal-index false)
```

Be aware: when deactivating he temporal index you may not use any temporal databases like `history`, `as-of`, or
`since`.

Refer to the [time variance documentation](./time_variance.md) for more information.
