__Disclaimer: This is experimental work in progress and subject to change.__


# Clojurescript and IndexedDB support for Datahike

This branch contains initial work on Clojurescript support for Datahike with persistence to IndexedDB in the browser. Our goal is a full port on a rebase of the Datahike `development` branch including new features such as tuple support and improved transaction performance in Q1 2021. We will support all major browsers, web workers, node.js and embedded JS environment. 
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
- a bunch of special cases

### Setup

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

The settings for starting your clojurescript repl is as follows:

- Project type: `shadow-cljs`
- Build selection: `:app`
- Build to connect to: `browser-repl`





