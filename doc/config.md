# Datahike database configuration

At database creation _datahike_ supports features that can be
configured based on the application's requirements. As of version `0.2.0`
configuration for the [storage backend](#storage-backend), the [schema
flexibility](#schema-flexibility), and [time variance](#time-variance) is supported.
Be aware: all these features can be set at database creation
but can not be changed afterwards. You can still migrate the data to a new configuration.

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
{:store {:backend  :mem        ;keyword
         :id       "default"   ;string
         :username nil         ;string
         :password nil         ;string
         :path     nil         ;string
         :host     nil         ;string
         :port     nil}        ;int
 :name (generated)             ;string
 :schema-flexibility :write    ;keyword
 :keep-history?      true}}    ;boolean
```

Please refer to the documentation of the [environ library](https://github.com/weavejester/environ) on how to use it. If you want to pass the config as environment variables or Java system properties you need to name them like following:

properties                  | envvar
----------------------------|--------------------------
datahike.store.backend      | DATAHIKE_STORE_BACKEND
datahike.store.username     | DATAHIKE_STORE_USERNAME
datahike.schema.flexibility | DATAHIKE_SCHEMA_FLEXIBILITY
datahike.keep.history       | DATAHIKE_KEEP_HISTORY
datahike.name               | DATAHIKE_NAME
etc.

*Do not use `:` in the keyword strings, it will be added automatically.*

## Storage Backend

Each backend needs a different set of provided parameters. See definition
[below](#storage-backend) for further information. For simple and fast creation
you can simply use the defaults which creates an in-memory database with ID `"default"`, write schema flexibility, and history support:

```clojure
(require '[datahike.api :as d])
(d/create-database)
```

At the moment we support two different backends from within Datahike: [in-memory](#in-memory) and [file-based](#file-based).
[LevelDB](#leveldb) and [PostgreSQL](#postgresql) is supported via external libraries: [datahike-postgres](https://github.com/replikativ/datahike-postgres/) and [datahike-leveldb](https://github.com/replikativ/datahike-leveldb)

### in-memory

- `<backend>`: `mem`
- `id`: ID of the database
- example: `{:store {backend :mem :id "mem-example"}}`
- uri example (deprecated): `datahike:mem://mem-example`

### file-based

- `<backend>`: `file`
- `path`: absolute path to the storage folder
- example: `{:store {:backend :file :path "/tmp/file-example"}}`
- uri example (deprecated): `datahike:file:///tmp/file-example`

### Supported External Backends

#### LevelDB

- `<backend>`: `level`
- `path`: absolute path to the LevelDB instance
- example: `{:store {:backend :level :path "/tmp/level-example"}}`
- uri example (deprecated): `datahike:level:///tmp/level-example`

#### PostgreSQL

- `<backend>`: `pg`
- `username`: PostgreSQL instance username
- `password`: PostgreSQL instance password
- `host`: PostgreSQL instance host
- `port`: PostgreSQL instance port
- `path`: name of the PostgreSQL database, must be present in the instance
- example: `{:store {:backend :pg :host "localhost" :port 5432 :username "alice" :password "foobar" :path "/pg_example"}}`
- uri example: `datahike:pg://alice:foobar@localhost:5432/pg_example`

## Name

By default datahike generates a name for your database for you. If you want to set
the name yourself just set a name for it in your config. It helps to specify the
database you want to use, in case you are using multiple datahike databases in
your application (to be seen in datahike-server).

## Schema Flexibility

By default the datahike api uses a schema on `:write` approach with strict value
types that need to be defined in advance. If you are not sure how your data
model looks like and you want to transact any kind of data into the database you
can set `:schema-flexibility` to `read`. You may add basic schema definitions like `:db/unique`,
`:db/cardinality` or `db.type/ref` where these kind of structure is needed.

```clojure
(require '[datahike.api :as d])

(d/create-database {:schema-flexibility :read})
```

Have a look at the [schema documentation](./schema.md) for more information.

## Historical Data

Datahike has the capability to inspect and query historical data within temporal
indices. If your application does not require any temporal data, you may
set `:keep-history?` to `false`.

```clojure
(require '[datahike.api :as d])
(d/create-database {:keep-history? true})
```

Be aware: when deactivating the temporal index you may not use any temporal databases like `history`, `as-of`, or
`since`.

Refer to the [time variance documentation](./time_variance.md) for more information.


## Deprecation Notice
Starting from version `0.3.0` it is encouraged to use the new hashmap configuration since it is more flexible than the previously used URI scheme. Datahike still supports the old configuration so you don't need to migrate yourself. The differences for the configuration are as following:

- optional parameters are added in the configuration map instead of optional parameters
- `:temporal-index` renamed to `:keep-history?`
- `:schema-on-read` renamed to `:schema-flexibility` with values `:read` and `:write`
- store configuration for backends moved into `:store` atttribute
- `:initial-tx` also added as attribute in configuration
- the store configuration is now more flexible, so it fits better with its backends
- all backend configuration remains the same except for `:mem`
- naming attribute for `:mem` backend is moved to `:id` from `:host` or `:path`
- optional `clojure.spec` validation has been added
