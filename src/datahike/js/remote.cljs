(ns datahike.js.remote
  "JavaScript boundary of the thin HTTP client: the Datahike API against a
   server, with no database engine in the bundle.

   Isomorphic to the ClojureScript API: a configuration whose `:remote-peer`
   names the server (`{:backend :datahike-server :url … :token …}`) is what
   `createDatabase` and `connect` take, and every function returns a Promise
   of the same value the JVM client returns. Connections, databases and
   entities are opaque handles that go back to the server as they came."
  (:refer-clojure :exclude [filter uuid])
  (:require [datahike.http.client]
            [datahike.js.convert]
            [datahike.remote])
  (:require-macros [datahike.js.api-macros :refer [emit-js-remote-api]]))

(defn wrap
  "The JavaScript boundary around one client function: arguments converted
   in, the channel's value converted out as a Promise."
  [operation client-fn]
  (fn [& args]
    (-> (apply client-fn (datahike.js.convert/js->clj-api-args operation args))
        datahike.js.convert/maybe-chan->promise
        (.then datahike.js.convert/clj->js-recursive))))

(emit-js-remote-api)

(defn ^:export isPromise
  "Check if a value is a Promise."
  [x]
  (instance? js/Promise x))

(defn ^:export uuid
  "Create a Datahike UUID value from a string, for a store id or a
  :db.type/uuid attribute value; UUID strings are never auto-detected."
  [s]
  (cljs.core/uuid s))

(defn ^:export randomUuid
  "A random UUID value."
  []
  (random-uuid))
