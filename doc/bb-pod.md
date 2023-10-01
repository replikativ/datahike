# Datahike Babashka Pod

With the help of [the Datahike cli](doc/cli.md) we provide the possibility to run Datahike as a[babashka pod](https://book.babashka.org/#pods).

## Why Datahike as a babashka pod?

A babashka pod is a way to use Datahike in babashka without Datahike being built into the babashka binary. When you
don't want to run a whole JVM for a little script you can use babashka and write your script in your preferred language.
Babashka already has ways to persist data to other databases but Datahike adds the possibility to write to a durable
datalog database.

## Compilation

We plan to provide the native binaries of Datahike via GitHub-Releases in the future. Unfortunately you have to compile it yourself for now. To do that you need to clone the [Datahike repository](https://github.com/replikativ/datahike), have babashka and clojure as well as a JDK installed.

Then please run `bb ni-cli` inside the datahike repository.

## Maturity

This feature is not used in production so far. Please try it and [open issues on GitHub](https://github.com/replikativ/datahike/issues/new/choose) if you find a problem.

### Supported functionality

- as-of
- connect
- create-database
- database-exists?
- datoms (returns a seq of datoms as maps)
- db
- db-with
- delete-database
- entity (returns a simple map without further functionality)
- history
- metrics
- pull
- pull-many
- q
- release-db (release the DB-object to free memory)
- since
- schema
- transact (returns the transaction-report as map)
- with-db (a macro that avoids storing the DB-object)

## Example usage

```
(ns pod
  (:require [babashka.pods :as pods]))

(pods/load-pod "./dhi")

(require '[datahike.pod :as d])

(def config {:store  {:backend :file
                      :path "/tmp/bb-datahike-pod"}
             :keep-history? true
             :schema-flexibility :read})

(d/delete-database config)

(d/create-database config)

(def conn (d/connect config))

(d/transact conn [{:name  "Alice", :age   20}
                  {:name  "Bob", :age   30}
                  {:name  "Charlie", :age   40}
                  {:age 15}])

(def db (d/db conn))

(d/q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
     db)

(release-db db)

(d/pull (d/db conn) '[*] 3)

(with-db [db (db conn)]
  (q {:query '{:find [?e ?a ?v]
      :where
      [[?e ?a ?v]]}}
     db))
```
