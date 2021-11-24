# Datahike database configuration

At database creation _Datahike_ supports features that can be configured based on the application's requirements. As of version `0.2.0` configuration for the [storage backend](#storage-backend), the [schema flexibility](#schema-flexibility), and [time variance](#historical-data) is supported.  Be aware: all these features can be set at database creation but can not be changed afterwards. You can still migrate the data to a new configuration.

## Configuration

Configuring _Datahike_ is now possible via the [environ library made by weavejester](https://github.com/weavejester/environ). You can use environment variables, java system properties and passing a config-map as argument.

The sources are resolved in following order:
1. Environment variables
2. Java system properties
3. Argument to load-config

That means passing a config as argument overwrites java system properties and using java system properties overwrite environment variables etc. Currently the configuration map looks like this per default:

```clojure
{:store              {:backend  :mem        ;keyword
                      :id        "default"} ;string
 :name               (generated)            ;string
 :schema-flexibility :write                 ;keyword
 :keep-history?      true}                  ;boolean
```

If you are using a backend different from the builtins `:mem` or `:file`, please have a look at the README in the corresponding Github repository. The configuration is outsourced to the backends so you will find the configuration documentation there. An example for `:mem`, `:file`, and `:jdbc`-backend you can see below. Please refer to the documentation of the [environ library](https://github.com/weavejester/environ) on how to use it. If you want to pass the config as environment variables or Java system properties you need to name them like following:

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
Additionally, [JDBC](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) databases like [PostgreSQL](#postgresql) are supported via an external library: [datahike-jdbc](https://github.com/replikativ/datahike-jdbc/).

### in-memory

- `<backend>`: `mem`
- `id`: ID of the database
- example: 
```clojure 
  {:store {:backend :mem 
           :id "mem-example"}}
```
- via environment variables:
```bash
DATAHIKE_STORE_BACKEND=mem
DATAHIKE_STORE_CONFIG='{:id "mem-example"}'
```

### file-based

- `<backend>`: `file`
- `path`: absolute path to the storage folder
- example: 
```clojure 
  {:store {:backend :file 
           :path "/tmp/file-example"}}
```
- via environment variables:
```bash
DATAHIKE_STORE_BACKEND=file
DATAHIKE_STORE_CONFIG='{:path "/tmp/file-example"}'
```

### Supported External Backends

#### JDBC

- `<backend>`: `jdbc`
- `dbtype`: [JDBC supported database](https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.737/doc/getting-started)
- `user`: PostgreSQL user
- `password`: password for PostgreSQL user
- `dbname`: name of the PostgreSQL database
- example:
```clojure 
 {:store {:backend :jdbc
          :dbtype "postgresql"
          :user "datahike"
          :password "datahike"
          :dbname "datahike"}}
```
- via environment variables:
```bash
DATAHIKE_STORE_BACKEND=jdbc
DATAHIKE_STORE_CONFIG='{:dbtype "postgresql" :user "datahike" :password "datahike" :dbname "datahike"}'
```


## Name

By default _Datahike_ generates a name for your database for you. If you want to set
the name yourself just set a name for it in your config. It helps to specify the
database you want to use, in case you are using multiple _Datahike_ databases in
your application (to be seen in datahike-server).

## Schema Flexibility

By default the _Datahike_ api uses a schema on `:write` approach with strict value
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
