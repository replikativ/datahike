# datahike <a href="https://gitter.im/replikativ/replikativ?utm_source=badge&amp;utm_medium=badge&amp;utm_campaign=pr-badge&amp;utm_content=badge"><img src="https://camo.githubusercontent.com/da2edb525cde1455a622c58c0effc3a90b9a181c/68747470733a2f2f6261646765732e6769747465722e696d2f4a6f696e253230436861742e737667" alt="Gitter" data-canonical-src="https://badges.gitter.im/Join%20Chat.svg" style="max-width:100%;"></a>

This project is a port of [datascript](https://github.com/tonsky/datascript) to
the [hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree). All
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
stable on-disk schema.

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

;; you might need to lease the connection, e.g. for leveldb
(release conn)

(delete-database uri)
~~~

The API namespace provides compatibility to a subset of Datomic functions and
should work as a drop-in replacement for them on the JVM. The rest of datahike
will use core.async to coordinate IO in a platform-neutral manner.


## Relationship to Datomic and datascript

datahike provides similar functionality to [datomic](http://datomic.com) and can
be used as a drop-in replacement for a subset of it. The goal of datahike is not
to provide an open-source reimplementation of Datomic, but it is part of the
[replikativ](https://github.com/replikativ) toolbox to build distributed data
management solutions. We have spoken to many clients and Clojure developers, who
tried to stay away from Datomic because of its proprietary nature and we think
in this regard datahike and datomic can very much complement each other.

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
- datomic provides timeouts


datahike's query engine and most of its codebase comes from
[datascript](https://github.com/tonsky/datascript). The differences to Datomic
are documented there.

## ClojureScript support

In general all [datascript
documentation](https://github.com/tonsky/datascript/wiki/Getting-started)
applies for namespaces beyond `datahike.api`. We are working towards a portable
version of datahike on top of core.async. Feel free to provide some help :).

## Roadmap/Changelog

### 0.1.0

- small, but stable JVM API
- caching for fast query performance in konserve

### 0.2.0

- cleanup interface to hitchhiker-tree
- use core.async in the future to provide durability also in a ClojureScript
environment. core.async needs to be balanced with query performance though.

### 0.3.0

- GC or eager deletion of fragments
- use hitchhiker-tree synchronization for replication
- run comprehensive query suite and compare to datascript and datomic

 

## Commercial support

We can provide commercial support with [lambdaforge](http://lambdaforge.io). If
you are interested in a particular feature, please let us know.

## License

Copyright © 2014–2018 Christian Weilbach, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
