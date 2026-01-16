# Datahike babashka Pod

With the help of [the Datahike cli](doc/cli.md) we provide the possibility to run Datahike as a [babashka pod](https://book.babashka.org/#pods).

## Why Datahike as a babashka pod?

A babashka pod is a way to use Datahike in babashka without Datahike being built into the babashka binary. When you
don't want to run a whole JVM for a little script you can use babashka and write your script in your preferred language.
Babashka already has ways to persist data to other databases but Datahike adds the possibility to write to a durable
datalog database.

## babashka pod-registry

You can use the [official babashka pod-registry](https://github.com/babashka/pod-registry/tree/master/manifests/replikativ/datahike) to download the latest version of Datahike as a pod and run it with babashka.

## Compilation

Please run `bb ni-cli` inside the datahike repository. You'll need [GraalVM-JDK](https://www.graalvm.org/latest/getting-started/) installed with `native-compile` on your path, [babashka](https://babashka.org/) and [Clojure](https://clojure.org/guides/install_clojure).

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

```clojure
(ns pod
  (:require [babashka.pods :as pods]))

(pods/load-pod 'replikativ/datahike "CURRENT") ;; Check https://github.com/babashka/pod-registry for latest version

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
