# Backend Development

Implementing a new backend to use for datahike does not require much effort as there are only a handful of methods for multimethods must be created. 
In order to keep things tidy, we have agreed on some conventions:

1. Keep all implementations for a backend within one namespace.
2. Name this namespace `datahike-{backendname}.core`.
3. Make sure, all multimethods are defined for the new backend you are developing.

As an example, you may have a look at the implementation of our [Postgres](https://www.postgresql.org/) backend, i.e.
[datahike-postgres](https://github.com/replikativ/datahike-postgres).

##  Template

Here, we provide a basic template for a backend implementation. The bracketed text pieces defines placeholder values, you should replace as follows:
- **backendname** surprisingly should be the name of your backend.
- **backendID** should be a `keyword` to identify your backend on request. At this moment, datahike ships with backends identified by `:mem` and `:file`, so do not use those.
- **indexID** should be a `keyword` identifying an index to be used as default for your backend. So far, you can choose between the following: 
  - `:datahike.index/hitchhiker-tree` 
  - `:datahike.index/persistent-set`
- **configSpec** optional `clojure.spec` definition for configuration validation

You may add any configuration attributes to the store configuration. Only `:backend` is mandatory which refers to **backendID**.

In your *core.clj*:
```clojure
(ns datahike-{backendname}.core
  (:require [datahike.store :as s]
            ;; your imports        
  ))


(defmethod s/empty-store {backendID} [config]
  ;; your implementation
  )

(defmethod s/delete-store {backendID} [config]
  ;; your implementation
  )

(defmethod connect-store {backendID} [config]
  ;; your implementation
  )

(defmethod release-store {backendID} [config store]
  ;; your implementation
  )

(defmethod scheme->index {backendID} [_]
  {indexID}
  )
  
(defmethod default-config {backendID} [config]
  ;; your implementation for default values e.g. from env vars or values from best practices
 )
 
(defmethod config-spec {backendID} [_] {configSpec})
```
