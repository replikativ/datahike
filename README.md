<h1 align="center">
    Datahike
</h1>
<p align="center">
<a href="https://clojurians.slack.com/archives/CB7GJAN0L"><img src="https://img.shields.io/badge/clojurians%20slack-join%20channel-blueviolet"/></a>
<a href="https://gitter.im/replikativ/replikativ?utm_source=badge&amp;utm_medium=badge&amp;utm_campaign=pr-badge&amp;utm_content=badge"><img src="https://camo.githubusercontent.com/da2edb525cde1455a622c58c0effc3a90b9a181c/68747470733a2f2f6261646765732e6769747465722e696d2f4a6f696e253230436861742e737667" alt="Gitter" data-canonical-src="https://badges.gitter.im/Join%20Chat.svg" style="max-width:100%;"></a>
<a href="https://clojars.org/io.replikativ/datahike"> <img src="https://img.shields.io/clojars/v/io.replikativ/datahike.svg" /></a>
<a href="https://circleci.com/gh/replikativ/datahike"><img src="https://circleci.com/gh/replikativ/datahike.svg?style=shield"/></a>
<a href="https://github.com/replikativ/datahike/tree/development"><img src="https://img.shields.io/github/last-commit/replikativ/datahike/development"/></a>
<a href="https://versions.deps.co/replikativ/datahike" title="Dependencies Status"><img src="https://versions.deps.co/replikativ/datahike/status.svg" /></a>
</p>

Datahike is a durable [Datalog](https://en.wikipedia.org/wiki/Datalog) database
powered by an efficient Datalog query engine. This project started as a port of
[DataScript](https://github.com/tonsky/DataScript) to the
[hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree). All
DataScript tests are passing, but we are still working on the internals. Having
said this we consider Datahike usable for medium sized projects, since DataScript is
very mature and deployed in many applications and the hitchhiker-tree
implementation is heavily tested through generative testing. We are
building on the two projects and the storage backends for the hitchhiker-tree
through [konserve](https://github.com/replikativ/konserve). We would like to
hear experience reports and are happy if you join us.

You may find articles on Datahike on our company's [blog page](https://lambdaforge.io/articles).

We presented Datahike also at meetups,for example at:

- [2019 scicloj online meetup](https://www.youtube.com/watch?v=Hjo4TEV81sQ).
- [2019 Vancouver Meetup](https://www.youtube.com/watch?v=A2CZwOHOb6U).
- [2018 Dutch clojure meetup](https://www.youtube.com/watch?v=W6Z1mkvqp3g).

## Usage

Add to your leiningen dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/datahike/latest-version.svg)](http://clojars.org/io.replikativ/datahike)

We provide a small stable API for the JVM at the moment, but the on-disk schema
is not fixed yet. We will provide a migration guide until we have reached a
stable on-disk schema. _Take a look at the ChangeLog before upgrading_.

```clojure
(require '[datahike.api :as d])


;; use the filesystem as storage medium
(def cfg {:store {:backend :file :path "/tmp/example"}})

;; create a database at this place, per default configuration we enforce a strict
;; schema and keep all historical data
(d/create-database cfg)

(def conn (d/connect cfg))

;; the first transaction will be the schema we are using
;; you may also add this within database creation by adding :initial-tx
;; to the configuration
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one }
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one }])

;; lets add some data and wait for the transaction
(d/transact conn [{:name  "Alice", :age   20 }
                  {:name  "Bob", :age   30 }
                  {:name  "Charlie", :age   40 }
                  {:age 15 }])

;; search the data
(d/q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
  @conn)
;; => #{[3 "Alice" 20] [4 "Bob" 30] [5 "Charlie" 40]}

;; add new entity data using a hash map
(d/transact conn {:tx-data [{:db/id 3 :age 25}]})

;; if you want to work with queries like in
;; https://grishaev.me/en/datomic-query/,
;; you may use a hashmap
(d/q {:query '{:find [?e ?n ?a ]
               :where [[?e :name ?n]
                       [?e :age ?a]]}
      :args [@conn]})
;; => #{[5 "Charlie" 40] [4 "Bob" 30] [3 "Alice" 25]}

;; query the history of the data
(d/q '[:find ?a
       :where
       [?e :name "Alice"]
       [?e :age ?a]]
  (d/history @conn))
;; => #{[20] [25]}

;; you might need to release the connection for specific stores like leveldb
(d/release conn)

;; clean up the database if it is not need any more
(d/delete-database cfg)
```

The API namespace provides compatibility to a subset of Datomic functionality
and should work as a drop-in replacement on the JVM. The rest of Datahike will
be ported to core.async to coordinate IO in a platform-neutral manner.

Refer to the docs for more information:

- [configuration](./doc/config.md)
- [schema flexibility](./doc/schema.md)
- [time variance](./doc/time_variance.md)
- [differences from Datomic](./doc/datomic_differences.md)
- [backend development](./doc/backend-development.md)
- [logging and error handling](./doc/logging_and_error_handling.md)
- [releasing Datahike](./doc/release.md)

For simple examples have a look at the projects in the `examples` folder.

## Example projects

- [Invoice creation](https://gitlab.com/replikativ/datahike-invoice)
  demonstrated at the [Dutch Clojure
  Meetup](https://www.meetup.com/de-DE/The-Dutch-Clojure-Meetup/events/trmqnpyxjbrb/).

## Relationship to Datomic and DataScript

Datahike provides similar functionality to [Datomic](http://Datomic.com) and can
be used as a drop-in replacement for a subset of it. The goal of Datahike is not
to provide an open-source reimplementation of Datomic, but it is part of the
[replikativ](https://github.com/replikativ) toolbox aimed to build distributed
data management solutions. We have spoken to many backend engineers and Clojure
developers, who tried to stay away from Datomic just because of its proprietary
nature and we think in this regard Datahike should make an approach to Datomic
easier and vice-versa people who only want to use the goodness of Datalog in
small scale applications should not worry about setting up and depending on
Datomic.

Some differences are:

- Datahike runs locally on one peer. A transactor might be provided in the
  future and can also be realized through any linearizing write mechanism, e.g.
  Apache Kafka. If you are interested, please contact us.
- Datahike provides the database as a transparent value, i.e. you can directly
  access the index datastructures (hitchhiker-tree) and leverage their
  persistent nature for replication. These internals are not guaranteed to stay
  stable, but provide useful insight into what is going on and can be optimized.
- Datomic has a REST interface and a Java API
- Datomic provides timeouts

Datomic is a full-fledged scalable database (as a service) built from the
authors of Clojure and people with a lot of experience. If you need this kind
of professional support, you should definitely stick to Datomic.

Datahike's query engine and most of its codebase come from
[DataScript](https://github.com/tonsky/DataScript). Without the work on
DataScript, Datahike would not have been possible. Differences to Datomic with
respect to the query engine are documented there.

## When should I pick what?

### Datahike

Pick Datahike if your app has modest requirements towards a typical durable
database, e.g. a single machine and a few millions of entities at maximum.
Similarly if you want to have an open-source solution and be able to study and
tinker with the codebase of your database, Datahike provides a comparatively
small and well composed codebase to tweak it to your needs. You should also
always be able to migrate to Datomic later easily.

### Datomic

Pick Datomic if you already know that you will need scalability later or if you
need a network API for your database. There is also plenty of material about
Datomic online already. Most of it applies in some form or another to Datahike,
but it might be easier to use Datomic directly when you first learn Datalog.

### DataScript

Pick DataScript if you want the fastest possible query performance and do not
have a huge amount of data. You can easily persist the write operations
separately and use the fast in-memory index datastructure of DataScript then.
Datahike also at the moment does not support ClojureScript anymore, although we
plan to recover this functionality.

## ClojureScript support

ClojureScript support is planned and work in progress. Please see [Roadmap](https://github.com/replikativ/datahike#roadmap).

## Migration & Backup

The database can be exported to a flat file with:

```clojure
(require '[datahike.migrate :refer [export-db import-db]])
(export-db @conn "/tmp/eavt-dump")
```

You must do so before upgrading to a Datahike version that has changed the
on-disk format. This can happen as long as we are arriving at version `1.0.0`
and will always be communicated through the Changelog. After you have bumped the
Datahike version you can use

```clojure
;; ... setup new-conn (recreate with correct schema)

(import-db new-conn "/tmp/eavt-dump")
```

to reimport your data into the new format.

The datoms are stored as strings in a line-based format, so you can easily check
whether your dump is containing reasonable data. You can also use it to do some
string based editing of the DB. You can also use the export as a backup.

If you are upgrading from pre `0.1.2` where we have not had the migration code
yet, then just evaluate the `datahike.migrate` namespace manually in your
project before exporting.

Have a look at the [change log](./CHANGELOG.md) for recent updates.

## Roadmap

### 0.3.0

- clojure.spec for api functions
- conceptualize schema upgrades
- Java API
- remote HTTP interface
- Docker image
- further schema types: bytes, tuples

### 0.4.0

- identity and access management
- CRDT type schema support
- fast redis backend support
- query planner and optimizer
- transaction monitoring

### 0.5.0

- optionally use core.async to handle storage IO
- ClojureScript support both in the browser and on node

### 0.6.0

- support GC or eager deletion of fragments
- use hitchhiker-tree synchronization for replication
- run comprehensive query suite and compare to DataScript and Datomic
- support anomaly errors (?)

### 1.0.0

- support optimistic write support through attributes with conflict resolution
  (CRDT-like)
- investigate https://github.com/usethesource/capsule for faster hh-tree durability

## Commercial support

We are happy to provide commercial support with
[lambdaforge](https://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2014–2020 Konrad Kühne, Christian Weilbach, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
