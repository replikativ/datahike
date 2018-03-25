# datahike <a href="https://gitter.im/replikativ/replikativ?utm_source=badge&amp;utm_medium=badge&amp;utm_campaign=pr-badge&amp;utm_content=badge"><img src="https://camo.githubusercontent.com/da2edb525cde1455a622c58c0effc3a90b9a181c/68747470733a2f2f6261646765732e6769747465722e696d2f4a6f696e253230436861742e737667" alt="Gitter" data-canonical-src="https://badges.gitter.im/Join%20Chat.svg" style="max-width:100%;"></a>

This project is a port of [datascript](https://github.com/tonsky/datascript) to
the [hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree). All
datascript tests are passing, but we are still working on the internals. Having
said this we consider datahike usable for small projects, since datascript is
very mature and deployed in many applications and the hitchhiker-tree
implementation is at least heavily tested through generative testing. We are
only providing the glue code between the two projects and the storage backends
for the hitchhiker-tree
through [konserve](https://github.com/replikativ/konserve). The codebase *is
subject to change* though. We would like to hear experience reports and are
happy if you join us.

## Usage

Add to your leiningen dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/datahike/latest-version.svg)](http://clojars.org/io.replikativ/datahike)


In general all [datascript documentation](https://github.com/tonsky/datascript/wiki/Getting-started) applies.

The code is currently a drop-in replacement for datascript on the JVM. If you
are interested in this topic, please play around and give suggestions.

The following example is taken from the store test which tests writing and
reading the hitchhiker-tree. Notice that you have control when you flush to the
store and can also use batching.

~~~clojure
(require '[datahike.api :refer :all])

(let [uri "datahike:file:///tmp/api-test"
          _ (create-database uri)
          conn (connect uri)
          tx-report @(transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                                     { :db/id 2, :name  "Petr", :age   37 }
                                     { :db/id 3, :name  "Ivan", :age   37 }
                                     { :db/id 4, :age 15 }])]
      (= (q '[:find ?e
              :where [?e :name]]
            @conn)
         #{[3] [2] [1]})
      (delete-database uri))
~~~


## TODO
- GC or eager deletion of fragments
- use core.async in the future to provide durability also in a ClojureScript
environment. core.async needs to be balanced with query performance though.
- run comprehensive query suite and compare to datascript and datomic

## License

Copyright © 2014–2018 Christian Weilbach, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
