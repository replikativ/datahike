(ns datahike.test.cljs-tiered-storage-test
  "CLJS validation of the tiered-store durability fix (#854): the
   `datahike.index/with-storage` :cljs branch (BTSet ctor) was only
   JVM-exercised. This is the config the bug targets — a memory frontend
   over a durable (node-filestore) backend. We write through the tiered
   store, then COLD-READ the durable backend directly via a plain :file
   store at the same path/id (a process restart / second peer). Without
   the fix the backend holds a root pointing at node blobs stranded in an
   orphaned buffer → `Node not found in storage`; with it the data is
   recoverable, including a write made after a cold reconnect."
  (:require [cljs.test :refer [deftest is async]]
            [datahike.api :as d]
            [konserve.node-filestore] ;; register :file backend for Node.js
            [cljs.nodejs :as nodejs]
            [cljs.core.async :refer [go <!] :include-macros true]))

(def os (nodejs/require "os"))
(def path (nodejs/require "path"))
(defn- tmp-dir [] (.join path (.tmpdir os) (str "dh-cljs-tiered-" (rand-int 1000000))))

(deftest tiered-frontend-deletion-recovery-test
  (async
   done
   (go
     (let [store-id  (random-uuid)
           dir       (tmp-dir)
           front-cfg {:backend :memory :id store-id}
           tiered    {:store {:backend :tiered :id store-id
                              :frontend-config front-cfg
                              :backend-config {:backend :file :path dir :id store-id}}
                      :keep-history? false :schema-flexibility :read
                      :index :datahike.index/persistent-set}
           ;; the durable backend alone — no frontend (cold read)
           backend   {:store {:backend :file :path dir :id store-id}
                      :keep-history? false :schema-flexibility :read
                      :index :datahike.index/persistent-set}]
       (try
         (<! (d/delete-database tiered))
         (<! (d/create-database tiered))
         (let [conn (<! (d/connect tiered {:sync? false}))]
           (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one}
                                  {:name "Alice"}]))
           (d/release conn))

         ;; COLD read #1 — plain :file store over the same backend
         (let [conn (<! (d/connect backend {:sync? false}))
               res  (try (d/q '[:find ?n :where [_ :name ?n]] @conn)
                         (catch :default e (str "THREW " (.-message e))))]
           (is (= #{["Alice"]} res)
               "tiered write must reach the durable backend (cold :file read)")
           (d/release conn))

         ;; write again through a RECONNECTED tiered store …
         (let [conn (<! (d/connect tiered {:sync? false}))]
           (<! (d/transact! conn [{:name "Bob"}]))
           (d/release conn))

         ;; COLD read #2 — the post-reconnect write must also be durable
         (let [conn (<! (d/connect backend {:sync? false}))
               res  (try (d/q '[:find ?n :where [_ :name ?n]] @conn)
                         (catch :default e (str "THREW " (.-message e))))]
           (is (= #{["Alice"] ["Bob"]} res)
               "post-cold-reconnect write must also be durable")
           (d/release conn))

         (<! (d/delete-database tiered))
         (catch :default e (is false (str "unexpected throw: " (.-message e)))))
       (done)))))
