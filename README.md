# datahike <a href="https://gitter.im/replikativ/replikativ?utm_source=badge&amp;utm_medium=badge&amp;utm_campaign=pr-badge&amp;utm_content=badge"><img src="https://camo.githubusercontent.com/da2edb525cde1455a622c58c0effc3a90b9a181c/68747470733a2f2f6261646765732e6769747465722e696d2f4a6f696e253230436861742e737667" alt="Gitter" data-canonical-src="https://badges.gitter.im/Join%20Chat.svg" style="max-width:100%;"></a>

datahike is a durable database with an efficient datalog query engine. This
project is a port of [datascript](https://github.com/tonsky/datascript) to the
[hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree). All
datascript tests are passing, but we are still working on the internals. Having
said this we consider datahike usable for small projects, since datascript is
very mature and deployed in many applications and the hitchhiker-tree
implementation is at least heavily tested through generative testing. We are
building on the two projects and the storage backends for the hitchhiker-tree
through [konserve](https://github.com/replikativ/konserve). We would like to
hear experience reports and are happy if you join us.

## Usage

Add to your leiningen dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/datahike/latest-version.svg)](http://clojars.org/io.replikativ/datahike)


We provide a small stable API for the JVM at the moment, but the on-disk schema
is not fixed yet. We will provide a migration guide until we have reached a
stable on-disk schema. _Take a look at the ChangeLog before upgrading_.

~~~clojure
(require '[datahike.api :refer :all])


;; use the filesystem as storage medium
(def uri #_"datahike:mem:///test"
    "datahike:file:///tmp/api-test"
    #_"datahike:level:///tmp/api-test1")
	
;; create a database at this place
(create-database uri)
	
(def conn (connect uri))

;; lets add some data and wait for the transaction
@(transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                 { :db/id 2, :name  "Petr", :age   37 }
                 { :db/id 3, :name  "Ivan", :age   37 }
                 { :db/id 4, :age 15 }])
				 
				 
(q '[:find ?e
     :where [?e :name]]
  @conn)			 
  
;; => #{[3] [2] [1]}

;; you might need to release the connection, e.g. for leveldb
(release conn)

(delete-database uri)
~~~

The API namespace provides compatibility to a subset of Datomic functionality
and should work as a drop-in replacement for them on the JVM. The rest of
datahike will be ported to core.async to coordinate IO in a platform-neutral
manner.


## Relationship to Datomic and datascript

datahike provides similar functionality to [Datomic](http://Datomic.com) and can
be used as a drop-in replacement for a subset of it. The goal of datahike is not
to provide an open-source reimplementation of Datomic, but it is part of the
[replikativ](https://github.com/replikativ) toolbox aimed to build distributed
data management solutions. We have spoken to many backend engineers and Clojure
developers, who tried to stay away from Datomic just because of its proprietary
nature and we think in this regard datahike should make an approach to Datomic
easier and vice-versa people who only want to use the goodness of datalog in
small scale applications should not worry about setting up and depending on
Datomic.

Some differences are:

- datahike runs locally on one peer. A transactor might be provided in the
  future and can also be realized through any linearizing write mechanism, e.g.
  Apache Kafka. If you are interested, please contact us.
- datahike provides the database as a transparent value, i.e. you can directly
  access the index datastructures (hitchhiker-tree) and leverage their
  persistent nature for replication. These internals are not guaranteed to stay
  stable, but provide useful insight into what is going on and can be optimized.
- datahike does not provide historical information out of the box yet
- datahike does not provide an API for transactor functions yet
- Datomic has a REST interface and a Java API
- Datomic provides timeouts

Datomic is a full-fledged scalable database (as a service) built from the
authors of Clojure and people with a lot of experience. If you need this kind
of professional support, you should definitely stick to Datomic.

datahike's query engine and most of its codebase come from
[datascript](https://github.com/tonsky/datascript). Without the work on
datascript datahike would not have been possible. Differences to Datomic with
respect to the query engine are documented there.


## When should I pick what?

### datahike

Pick datahike if your app has modest requirements towards a typical durable
database, e.g. a single machine and a few millions of entities at maximum.
Similarly if you want to have an open-source solution and be able to study and
tinker with the codebase of your database, datahike provides a comparatively
small and well composed codebase to tweak it to your needs. You should also
always be able to migrate to Datomic later easily.

### Datomic

Pick Datomic if you already know that you will need scalability later or if you
need a network API for your database. There is also plenty of material about
Datomic online already. Most of it applies in some form or another to datahike,
but it might be easier to use Datomic directly when you first learn Datalog.

### datascript

Pick datascript if you want the fastest possible query performance and do not
have a huge amount of data. You can easily persist the write operations
separately and use the fast in-memory index datastructure of datascript then.

## ClojureScript support

In general all [datascript
documentation](https://github.com/tonsky/datascript/wiki/Getting-started)
applies for namespaces beyond `datahike.api**. We are working towards a portable
version of datahike on top of core.async. Feel free to provide some help :).


## Migration & Backup

The database can be exported to a flat file with:

~~~clojure
(require '[datahike.migrate :refer [export-db import-db]])
(export-db @conn "/tmp/eavt-dump")
~~~

You must do so before upgrading to a datahike version that has changed the
on-disk format. This can happen as long as we are arriving at version `1.0.0`
and will always be communicated through the Changelog. After you have bumped the
datahike version you can use

~~~clojure
;; ... setup new-conn (recreate with correct schema)

(import-db new-conn "/tmp/eavt-dump")
~~~

to reimport your data into the new format.

The datoms are stored as strings in a line-based format, so you can easily check
whether your dump is containing reasonable data. You can also use it to do some
string based editing of the DB. You can also use the export as a backup.

If you are upgrading from pre `0.1.2` where we have not had the migration code
yet, then just evaluate the `datahike.migrate` namespace manually in your
project before exporting.


## Changelog


### 0.1.2

- *disk layout change, migration needed*
- write root nodes of indices efficiently; reduces garbage by ~40 times and
  halves transaction times
- support export/import functionality


### 0.1.1

- preliminary support for datascript style schemas through
  `create-database-with-schema`
- support storage of BigDecimal and BigInteger values

### 0.1.0

- small, but stable JVM API
- caching for fast query performance in konserve


- reactive reflection warnings?
- schema support
- remove eavt-durable
- remove redundant slicing code
- generalize interface to indices
- integration factui/reactive?

## Roadmap

### 0.2.0

- cleanup interface to hitchhiker-tree
- optionally use core.async to handle storage IO
- ClojureScript support both in the browser and on node
- conceptualize schema upgrades
- fast redis backend support
- explore support for other index structures, e.g. FoundationDB

### 0.3.0

- support GC or eager deletion of fragments
- use hitchhiker-tree synchronization for replication
- run comprehensive query suite and compare to datascript and Datomic
- support anomaly errors (?)

### 1.0.0

- support optimistic write support through attributes with conflict resolution
  (CRDT-like)
- reactive datalog for materialized views
- provide some network access
- investigate https://github.com/usethesource/capsule for faster hh-tree durability
 

## Commercial support

We are happy to provide commercial support with
[lambdaforge](http://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2014–2018 Christian Weilbach, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
