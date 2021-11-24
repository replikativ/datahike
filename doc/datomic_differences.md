# Differences to Datomic Client API

Although Datahike supports a part of [Datomic's
API](https://docs.datomic.com/client-api/datomic.client.api.html), some
behavior is different using the different functions. Datahike supports part of the `datomic.client.api`:

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/io.replikativ/datahike)

Without differences in the signature you may use the following like in Datomic:

- as-of
- datoms
- [db](#db)
- history
- pull
- q
- since
- [transact](#transact)

Please be aware that `q` returns sets instead of vectors. Only when using aggregates or `:with`, `q` returns a
vector. That is the behavior of Datascript.

The following functions from the `datahike.api` namespace are different from Datomic's client API:

- [connect](#connect)
- [create-database](#create-database)
- [database-exists?](#database-exists)
- [db-with](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#db-with)
- [delete-database](#delete-database)
- [entity](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#entity)
- [entity-db](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#entity-db)
- [filter](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#filter)
- [is-filtered](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#is-filtered)
- [load-entities](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#load-entities)
- [pull-many](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#pull-many)
- [release](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#release)
- [seek-datoms](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#seek-datoms)
- [tempid](https://cljdoc.org/d/io.replikativ/datahike/0.3.6/api/datahike.api#tempid)
- [with](#with)

Additionally, datahike supports most functions from [datascript](https://github.com/tonsky/datascript)
in the `datahike.api` namespace.

These functions of Datomic are not yet implemented but considered candidates for future development:
- tx-range
- index-pull
- with-db

These functions are part of Datomics' distributed implementation and are currently not part of the
Datahike API:
- client
- administer-system
- db-stats
- list-databases
- sync

Async support is on our roadmap as well as running distributed Datahike.

## connect

Connects to an existing database given the configuration hash-map where Datomic
takes a client as argument. The specification for the configuration can be found
[here](./config.md).

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/io.replikativ/datahike)

## create-database

Creates a new database with the given configuration hash-map where Datomic takes a client and an
arg-map as arguments. Additional optional parameters are `schema-flexibility`, `keep-history?`
and `initial-tx`. Have a look at the [configuration documentation](./config.md) for details.

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/io.replikativ/datahike)

## delete-database

Deletes a database with the configuration hash-map as argument.

## database-exists?

Checks if a database exists with the configuration hash-map as argument.

## db

Since the database can be just de-referenced from the connection this function is
just a small wrapper for Datomic compliance.

```clojure
(db conn)
```
equivalent to:
```
@conn
```

## transact

Returns a hash map different from Datomics' as a report. The `:tx-meta` is not part
of Datomics' transaction report but apart from that the same keys are present. The
values are different records though.

[![cljdoc](https://badgen.net/badge/cljdoc/datahike/blue)](https://cljdoc.org/d/io.replikativ/datahike)

## with

Applies transactions to an immutable database value and returns a new database
snapshot. It does not change the mutable database inside the connection unlike
[transact](#transact). It works quite the same as Datomics' `with` but does
not need a `with-db` function to work.
