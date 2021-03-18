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
- History functionality
- Pull


### Setup

You can include this preview in your `deps.edn` with the code below. This is currently only tested with `shadow-cljs` using the `browser-repl`. Further work is needed for advanced compilation (feedback on experiences welcome in Discord).

```clojure
io.replikativ/datahike {:git/url "https://github.com/replikativ/datahike.git"
                        :sha "b07247fc80ee06858e3417fa04d3015761e61975"}
```

The following is an example of how to use Datahike in your project.

```clojure
(ns my-app.prototype
  (:require [datahike.api :as d]
            [datahike.impl.entity :as de]
            [clojure.core.async :as async :refer [go <!]]))

;; Define your schema
(def people-schema [{:db/ident       :name
                     :db/cardinality :db.cardinality/one
                     :db/index       true
                     :db/unique      :db.unique/identity
                     :db/valueType   :db.type/string}
                    {:db/ident       :age
                     :db/cardinality :db.cardinality/one
                     :db/valueType   :db.type/number}
                    {:db/ident       :country
                     :db/cardinality :db.cardinality/one
                     :db/valueType   :db.type/string}
                    {:db/ident       :siblings
                     :db/cardinality :db.cardinality/many
                     :db/valueType   :db.type/ref}
                    {:db/ident       :friend
                     :db/cardinality :db.cardinality/many
                     :db/valueType :db.type/ref}])

;; Define you db configuration
(def people-idb {:store  {:backend :indexeddb :id "people-idb"}
                :keep-history? true
                :schema-flexibility :write
                :initial-tx people-schema})


;; You can also set up a schemaless db which provides schema on read
(def national-dish-idb {:store  {:backend :indexeddb :id "national-dish-idb"}
                        :keep-history? false
                        :schema-flexibility :read})

```

Instead of `:backend` `:indexeddb` you can also use the memory backend with `:memory`.

### Database interaction

We now show how to interact with the database interactively. All API calls return core.async channels, so if you want to use it inside of your application logic you need to wrap them in a `go` block and take from each resulting channel. 

```clojure
  ;; Create an indexeddb store.
  (d/create-database cfg-idb)

  ;; Connect to the indexeddb store.
  (go (def conn-idb (<! (d/connect cfg-idb))))

  ;; Add people to our database
  (go (<! (d/transact conn-idb [{:name "Alice"
                                 :age  26
                                 :country "Indonesia"}
                                {:name "Bob"
                                 :age  35
                                 :country "Germany"
                                 :_friend [{:name "Mike"
                                            :age 28
                                            :country "United Kingdom"}]}
                                {:name  "Charlie"
                                 :age   45
                                 :country "Italy"
                                 :siblings [[:name "Alice"] [:name "Bob"]]}])))


    ;; Find the :name of the person with :age of 26
  (go (println (<! (d/q '[:find ?v
                          :in $ ?a
                          :where
                          [?e :name ?v]
                          [?e :age ?a]]
                        @conn-idb
                        26))))


  ;; Use the pull API
  (go (println (<! (d/pull @conn-idb [:name, :age] 7))))
  (go (println (<! (d/pull @conn-idb '[*] 7))))
  (go (println (<! (d/pull-many @conn-idb '[:name :age] [5 6]))))


  ;; Use the Entity API
  (go (def touched-entity (<! (de/touch (<! (d/entity @conn-idb 8))))))

  (go (println (:name touched-entity)))
  (go (println (:age touched-entity)))
  (go (println (:siblings touched-entity)))

  (go (println (count touched-entity)))
  (go (println (keys touched-entity)))
  (go (println (vals touched-entity)))
  (go (println (contains? touched-entity :siblings)))



  ;; 🕰 History functionality  
  (go (println (<! (d/q '[:find ?a ?v
                          :in $
                          :where [?e :name ?v] [?e :age ?a]]
                        @conn-idb))))
  ;; #{[26 Alice] [35 Bob] [28 Mike] [45 Charlie]}
  ;; We get back the age and name of each entity in the database we initially transacted
   
   
  ;; Let's proceed to make changes to the database

  (d/transact conn-idb [{:name "Alice"
                         :age  20}])

  (def first-date-snapshot (js/Date.)) 

  (d/transact conn-idb [{:name "Alice"
                         :age  40}
                        {:name "Bob"
                         :age  20}])

  (def second-date-snapshot (js/Date.))


  (d/transact conn-idb [{:name "Alice"
                         :age  55}])

  (go (println (<! (d/q '[:find ?a ?v
                          :in $
                          :where [?e :name ?v] [?e :age ?a]]
                        @conn-idb))))
  ;; Result: #{[55 Alice] [20 Bob] [28 Mike] [45 Charlie]}
  ;; Now Alice and Bob have changed their ages


  ;; 👇 Get the full history of ages that has existed for "Alice" in the database and it's transaction
  (go (println (<! (d/q '[:find ?a ?t
                          :where
                          [?e :name "Alice"]
                          [?e :age ?a ?t]]
                        (d/history @conn-idb)))))
  ;; Result: #{[55 536870917] [20 -536870916] [20 536870915] [26 -536870915] [26 536870914] [40 -536870917] [40 536870916]}
  



  ;; 👇 We can also query the state of the db at a certain point in time. Using the transaction id or date.
  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb 536870915)))))
   ;; Result: #{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}
   ;; 536870915 = transaction-id when Alice was set to 20    
     
  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb first-date-snapshot)))))
   ;; Result: #{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}
   ;; You can also pass the date in as a parameter   

  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb second-date-snapshot)))))
   ;; Result: #{[Bob 20] [Mike 28] [Charlie 45] [Alice 40]}


  ;; 👇 All the changes since a point in time or transaction id
  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb first-date-snapshot)))))
  ;; Result: #{[Alice 55] [Bob 20] [Alice 40]}


  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb second-date-snapshot)))))
  ;#{[Alice 55]}


  ;; 🎉 Cross-db joins for some fun.

  ;; Create a second database and connect to it. We will use this later.
  (go (<! (d/create-database national-dish-idb))
      (def conn-idb-2 (<! (d/connect national-dish-idb))))  


  ;; Add some national dishes to our second database
  (go (<! (d/transact conn-idb-2 [{:country "Italy"
                                   :dishes #{"Pizza" "Pasta"}}
                                  {:country  "Indonesia"
                                   :dishes #{"Nasi goreng" "Satay" "Gado gado"}}
                                  {:country "United Kingdom"
                                   :dishes #{"Fish and Chips" "Sunday roast"}}
                                  {:country "Germany"
                                   :dishes #{"Döner kebab" "Currywurst" "Sauerbraten"}}])))

  ;; Now we query the second db we set up
  (go (println (<! (d/q '[:find ?n ?d
                          :in $ ?n
                          :where
                          [?e :country ?n]
                          [?e :dishes ?d]]
                        @conn-idb-2
                        "United Kingdom"))))


  ;; Let's do a cross db join to see what dishes Alice might like based on their country
  (go (println (<! (d/q '[:find ?name ?d
                          :in $1 $2 ?name
                          :where
                          [$1 ?e :name ?name]
                          [$1 ?e :country ?c]
                          [$2 ?e2 :country ?c]
                          [$2 ?e2 :dishes ?d]]
                        @conn-idb
                        @conn-idb-2
                        "Alice"))))


  ;; We can even pass a list of people to the query and get the results of what dishes they might like by using the national dish db
  (go (println (<! (d/q '[:find ?name ?d
                          :in $1 $2 [?name ...]
                          :where
                          [$1 ?e :name ?name]
                          [$1 ?e :country ?c]
                          [$2 ?e2 :country ?c]
                          [$2 ?e2 :dishes ?d]]
                        @conn-idb
                        @conn-idb-2
                        ["Alice" "Mike" "Charlie"]))))



  ;; You must release the connection before deleting the database. 
  ;; However if the database has been freshly created of you have refreshed the browser without connecting you can delete it straight away.

  (d/release conn-idb)
  (d/delete-database people-idb)

  (d/release conn-idb-2)
  (d/delete-database national-dish-idb)

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
