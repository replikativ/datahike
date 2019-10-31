# Datomic Differences

for more information as the

Although datahike supports a part of [Datomic's
API](https://docs.datomic.com/client-api/datomic.client.api.html), some
behavior is different using the different functions. Datahike supports part of the `datomic.client.api`:

Without differences you may use the following like in Datomic:

- as-of
- history
- pull
- q
- since

The following functions from the `datahike.api` namespace are different from Datomic's client API:

- [connect](#connect)
- [create-database](#create-database)
- [datoms](#datoms)
- [db](#db)
- [delete-database](#delete-database)
- [transact](#transact)
- [with](#with)

Additionally, datahike supports also almost all functions from
[datascript](https://github.com/tonsky/datascript) in the `datahike.core` namespace.

## connect

Connects to an existing database given a `URL` or host configuration hash-map. The schema for
the `URL` and hash-map can be found [here](./config.md).

```clojure
(connect "datahike:mem://datomic-diff")
```

## create-database

Creates a new database given a `URL` or configuration hash-map with additional
optional parameters for `schema-on-read` and `temporal-index` capabilities. Have
a look at the [configuration documentation](./config.md) for details.

```clojure
(create-database "datahike:mem://datomic-diff" :schema-on-read true
:temporal-index false)
```

## datoms

The signature is different. Index can be selected: `:eavt`, `:aevt`, `:avet` and
components to be matched using entity, attribute and values. Find more examples
and documentation either in the examples project or in the code documentation.

```clojure
(datoms @conn :eavt :name)
```

## db

Since the database can be just de-referenced from the connection this function is
just a small wrapper for Datomic compliance.

```clojure
(db conn)
```

## delete-database

The deletion supports only URIs at the moment.

## transact

Returns a different hash map as a report. Have a look at the clojure docs for
more examples.

## with

Applies transactions to an immutable database value and returns a new database snapshot. It does not change the
mutable database inside the connection unlike [transact](#transact).
