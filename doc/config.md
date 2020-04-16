# Datahike database configuration

At database creation _datahike_ supports features that can be
configured based on the application's requirements. As of version `0.2.0`
configuration for the [storage backend](#storage-backend), the [schema
flexibility](#schema-flexibility), and the
[time variance](#time-variance) is supported.
Be aware: all these features can be set at database creation
but can not be changed afterwards.

## Configuration

Configuring datahike is now possible via the [environ library made by weavejester](https://github.com/weavejester/environ). That means you can use the lein-environ plugins for leiningen or boot to read variables from `.lein-env` or `.boot.env`. Without using the plugins you can use environment variables, java system properties and passing a config-map as argument.

The sources are resolved in following order:
1. A .lein-env file in the project directory
2. A .boot-env file on the classpath
3. Environment variables
4. Java system properties
5. Argument to load-config

That means passing a config as argument overwrites java system properties and using java system properties overwrite environment variables etc. Currently the configuration map looks like this per default:

```
{:store {:backend    :mem     ;keyword
         :username   nil      ;string
         :password   nil      ;string
         :path       nil      ;string
         :host       nil      ;string
         :port       nil      ;int
         :dbname     nil      ;string
         :ssl        false    ;boolean
         :sslfactory nil      ;string
 :schema-on-read     false    ;boolean
 :temporal-index     true}}   ;boolean
```

Please refer to the documentation of the [environ library](https://github.com/weavejester/environ) on how to use it. When you want to pass a map to the configuration you can pass the above map or parts of it to the reload-config function in the datahike.config namespace like this:
```
(require '[datahike.api :as d]
         '[datahike.config :as c])
(c/reload-config {:store
                  {:backend :file
                   :path "/tmp/datahike"}})
(d/create-database)
```

This is best done before creating the database so that the database does not have to be recreated and dataloss is avoided. If you want to pass the config as environment variables or Java system properties you need to name them like following:

properties              | envvar
--------------------------|--------------------------
datahike.store.backend  | DATAHIKE_STORE_BACKEND
datahike.store.username | DATAHIKE_STORE_USERNAME
datahike.schema.on.read | DATAHIKE_SCHEMA_ON_READ
datahike.temporal.index | DATAHIKE_TEMPORAL_INDEX

etc.

*Do not use `:` in the keyword strings, it will be added automatically.*

Example configuration for connecting to postgresql locally with ssl:
```
{:store {:backend    :pg
         :username   "datahike"
         :password   "password"
         :path       nil
         :host       "localhost"
         :port       5432
         :dbname     "datahike"
         :ssl        true
         :sslfactory "org.postgresql.ssl.NonValidatingFactory"}
 :schema-on-read     false
 :temporal-index     true}}
```

## Storage Backend

Each backend needs a different set of provided parameters. See definition
[below](#storage-backend) for further information. For simple and fast creation
you can simply use the defaults which creates an in-memory database:

```clojure
(require '[datahike.api :as d])
(d/create-database)
```

At the moment we support four different backends: [in-memory](#in-memory) ,[file-based](#file-based),
[LevelDB](#leveldb), and [PostgreSQL](#postgresql).

### in-memory

- `<backend>`: `mem`
- `host`: name of the database
- uri example: `datahike:mem://mem-example`
- hash map example: `{:backend :mem :host "mem-example"}`

### file-based

- `<backend>`: `file`
- `path`: absolute path to the storage folder
- uri example: `datahike:file:///tmp/file-example`
- hash map example: `{:backend :file :path "/tmp/file-example"}`

### LevelDB

- `<backend>`: `level`
- `path`: absolute path to the LevelDB instance
- uri example: `datahike:level:///tmp/level-example`
- hash map example: `{:backend :level :path "/tmp/level-example"}`

### PostgreSQL

- `<backend>`: `pg`
- `username`: PostgreSQL instance username
- `password`: PostgreSQL instance password
- `host`: PostgreSQL instance host
- `port`: PostgreSQL instance port
- `path`: name of the PostgreSQL database, must be present in the instance
- uri example: `datahike:pg://alice:foobar@localhost:5432/pg_example`
- hash map example: `{:backend :pg :host "localhost" :port 5432 :username "alice" :password "foobar" :path "/pg_example"}`

## Schema Flexibility

By default the datahike api uses a `schema-on-write` approach with strict value
types that need to be defined in advance. If you are not sure how your data
model looks like and you want to transact any kind of data into the database you
can set `schema-on-read` to `true` by adding it as optional parameter at
database creation. You may add basic schema definitions like `:db/unique`,
`:db/cardinality` or `db.type/ref` where these kind of structure is needed.

```clojure
(require '[datahike.api :as d]
         '[datahike.config :as c])
(c/reload-config {:schema {:schema-on-read true}})
(d/create-database)
```

Have a look at the [schema documentation](./schema.md) for more information.

## Time Variance

Datahike has the capability to inspect and query historical data within temporal
indices. If your application does not require any temporal data, you may
set `:temporal-index` to `false`.

```clojure
(require '[datahike.api :as d]
         '[datahike.config :as c])
(c/reload-config {:schema {:temporal-index false}})
(d/create-database)
```

Be aware: when deactivating the temporal index you may not use any temporal databases like `history`, `as-of`, or
`since`.

Refer to the [time variance documentation](./time_variance.md) for more information.
