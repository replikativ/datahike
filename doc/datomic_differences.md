# Datomic Differences

Although datahike supports a part of [Datomic's
API](https://docs.datomic.com/client-api/datomic.client.api.html), some
behaviour is different using the different functions. The following functions
from the `datomic.client.api` namespace are supported in datahike: 
- [as-of](#as-of)
- [connect](#connect)
- [create-database](#create-database)
- [datoms](#datoms)
- [db](#db)
- [delete-database](#delete-database)
- [history](#history)
- [index-range](#index-range)
- [pull](#pull)
- [q](#q)
- [since](#since)
- [transact](#transact)
- [with](#with)
- [with-db](#with-db)

See the subsections for differences and migration strategies.

## as-of

Behaves the same.

## connect

Connects to an existing database given a `URL` or host configuration hash-map. The schema for
the `URL` and hash-map can be found [here](./config.md).

```clojure
(connect "datahike:mem://datomic-diff")

```

## create-database

Creates a new database given a `URL` or configuration hash-map with additional
toggles for `schema-on-read` and `temporal-index` capabilities. 

## datoms

Signature is different. Index can be selected: `:eavt`, `:aevt`, `:avet` and
components to be matched using entity, attribute and values.

```clojure
(datoms @conn :eavt :name)
```

## db

Just a little helper, since in datahike you can just `deref` the connection to
get the current database.
```clojure
(db conn)
@conn
```

## delete-database

Supports only `URI` as parameter.

## history

Behaves the same.

## index-range

## pull

## q

## since

Behaves the same.

## transact

## with

## with-db
