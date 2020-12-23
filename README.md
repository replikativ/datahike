<h1 align="center">
    Datahike
</h1>
<p align="center">

__Disclaimer: This is experimental work in progress and subject to change.__


# ClojureScript and IndexedDB support for Datahike

This branch contains initial work on ClojureScript support for Datahike with persistence to IndexedDB in the browser. Our goal is a full port on a rebase of the Datahike `development` branch including new features such as tuple support and improved transaction performance in Q1 2021. We will support all major browsers, web workers, node.js and embedded JS environment. 
Our vision is a distributed unified address space for client and server side databases along the lines of the semantic web, but built based on fast P2P replication of our read scalable, immutable fractal tree data structure. 



## What can I do with this preview?

This preview is supposed to be a basis for prototyping to help us guide our development and make sure Datahike will fit your use case. It is not intended for production use yet.

## Ways to contribute

We are interested in tooling experience and suggestions. In particular we care about our API design, performance, integration into existing ecosystems ([react](https://github.com/homebaseio/homebase-react) with our partner [Homebase](https://www.homebase.io/), Electron, JS backends, Chrome apps, react native).

- Join us on [Discord](https://discord.com/invite/kEBzMvb)
- Report issues
- Support us on [Open Collective](https://opencollective.com/datahike)


# Usage

**Functionality included:**

- Create a store
  - In memory
  - IndexedDB
- Transact
- Query
- Entity

**Not yet included:**

- History functionality
- Pull
- Some special cases

### Setup

You can include this preview in your `deps.edn` with the code below. This is currently only tested with `shadow-cljs` using the `browser-repl`. Further work is needed for advanced compilation (feedback on experiences welcome in Discord).

```clojure
io.replikativ/datahike {:git/url "https://github.com/replikativ/datahike.git"
                        :sha "684cf719d93618a4630565078f3efb4b85858070"}
```

The following is an example of how to use Datahike in your project.

```clojure
(ns my-app.prototype
  (:require [datahike.api :as d]
            [datahike.impl.entity :as de]
            [clojure.core.async :as async :refer [go <!]]))

(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/index       true
              :db/unique      :db.unique/identity
              :db/valueType   :db.type/string}
             {:db/ident       :sibling
              :db/cardinality :db.cardinality/many
              :db/valueType   :db.type/ref}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/number}
             {:db/ident       :friend
              :db/cardinality :db.cardinality/many
              :db/valueType :db.type/ref}])


(def cfg-idb {:store  {:backend :indexeddb :id "idb-sandbox"}
              :keep-history? false
              :schema-flexibility :write
              :initial-tx schema})
```

Instead of `:backend` `:indexeddb` you can also use the memory backend with `:memory`.

### Database interaction

We now show how to interact with the database interactively. All API calls return core.async channels, so if you want to use it inside of your application logic you need to wrap them in a `go` block and take from each resulting channel.

```clojure
  ;; Create an indexeddb store.
  (d/create-database cfg-idb)

  ;; Connect to the indexeddb store.
  (go (def conn-idb (<! (d/connect cfg-idb))))

  ;; Transact some data to the store.
  (d/transact conn-idb [{:name "Alice"
                         :age  26}
                        {:name "Bob"
                         :age  35
                         :_friend [{:name "Mike"
                                    :age 28}]}
                        {:name  "Charlie"
                         :age   45
                         :sibling [[:name "Alice"] [:name "Bob"]]}])

  ;; Run a query against the store.
  (go (println (<! (d/q '[:find ?e ?a ?v ?t
                          :in $ ?a
                          :where [?e :name ?v ?t] [?e :age ?a]]
                        @conn-idb
                        35))))

  ;; Use the Entity API for "Bob".
  (go
    (let [e (<! (d/entity @conn-idb 6))]
      (println (<! (e :name)))))

  ;; Release the connection from the store.
  ;; This is necessary for deletion.
  (d/release conn-idb)

  ;; Delete the store. 
  ;; This can be done immediately after creation.
  ;; In a fresh browser session or after releasing the connection. 
  (d/delete-database cfg-idb) 

```

You can inspect the resulting IndexedDB through your browser dev tools and should be able to retrieve the durable data after reloading your browser tab.

## How we currently develop this

To hack on this prototype clone this repository. The namespace for the api sandbox is located:
- `dev/api_sandbox.cljs`

You will need **Clojure** and **shadow-cljs** installed.  

The settings for starting your ClojureScript repl is as follows:

- Project type: `shadow-cljs`
- Build selection: `:app`
- Build to connect to: `browser-repl`


# Datahike background

Datahike is a durable [Datalog](https://en.wikipedia.org/wiki/Datalog) database powered by an efficient Datalog query engine. This project started as a port of [DataScript](https://github.com/tonsky/DataScript) to the [hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree). All DataScript tests are passing, but we are still working on the internals. Having said this we consider Datahike usable for medium sized projects, since DataScript is very mature and deployed in many applications and the hitchhiker-tree implementation is heavily tested through generative testing. We are building on the two projects and the storage backends for the hitchhiker-tree through [konserve](https://github.com/replikativ/konserve). We would like to hear experience reports and are happy if you join us.

You may find articles on Datahike on our company's [blog page](https://lambdaforge.io/articles).

We presented Datahike also at meetups,for example at:

- [2019 scicloj online meetup](https://www.youtube.com/watch?v=Hjo4TEV81sQ).
- [2019 Vancouver Meetup](https://www.youtube.com/watch?v=A2CZwOHOb6U).
- [2018 Dutch clojure meetup](https://www.youtube.com/watch?v=W6Z1mkvqp3g).


Refer to the JVM docs for more information:

- [configuration](./doc/config.md)
- [schema flexibility](./doc/schema.md)
- [entity spec](./doc/entity_spec.md)
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




# Roadmap 
 
## Datahike

### 0.4.1 
- GC
- query performance improvements
- updating of all konserve backends including DynamoDB/S3
     

### 0.4.2
- better error messages
- safe Datalog clause reorderings (static analysis)
- GraalVM support (if possible)

### 0.5.0 
- ClojureScript support on IndexedDB
- remote connection support for datahike-server transactor
- general URI based address scheme for databases globally
- cross-platform joins between frontend and backend databases

   
### 0.6.0 
- RDF compatible schemas and translation (semantic web)
- resource/cost model for query execution and planning
- general interface definitions and semantics independent of Clojure
- attribute-based CRDT support (preview)
    
### 1.0.0 
- full CRDT support including fully decentralized P2P version
- run queries on distributed infrastructure
- JIT compiled Datalog runtime for Clojure/ClojureScript
- integration of Datalog for static analysis in Clojure compiler/macroexpander
- non-Clojure Datalog query runtime implementation for e.g. Julia, Python,
  Ruby or Rust

     
## Datahike server

### 0.1.0 
- swagger documented REST API
- basic authentication
- managed implementation for large cloud provider, e.g. Azure
    
### 0.2.0
- basic smart contract support (already implemented for Datopia)
- exposure of konserve store or immutable iterators for remote connections
- experimental P2P replication through dat

## Commercial support

We are happy to provide commercial support with
[lambdaforge](https://lambdaforge.io). If you are interested in a particular
feature, please let us know.

## License

Copyright © 2014–2020 Konrad Kühne, Christian Weilbach, Wade Dominic, Chrislain Razafimahefa, Timo Kramer, Judith Massa, Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
