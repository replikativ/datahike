(ns sandbox
  (:require
   [datahike.api :as d]
   [konserve.core :as k]
   #_[datahike.nodejs]
   [konserve.indexeddb :as idb]
   [datahike.store :as ds]
   [konserve.tiered :as kt]
   [org.replikativ.persistent-sorted-set :as pss]
   [clojure.core.async :refer [<! >! chan put! close!]])
  (:require-macros [clojure.core.async :refer [go]]))

#_(go
    (def idb-store (<! (idb/connect-idb-store "sandbox-store39"))))

(def idb-store (ds/add-cache-and-handlers idb-store cfg ))

(go (prn (<! (k/get idb-store :db nil {:sync? false}))))

(def store (:store @(:wrapped-atom conn)))

(keys @(:state (:frontend-store store)))

(:backend-store store)

(def eavt-address (.-address (:eavt-key (k/get store :db nil {:sync? true}))))

(def first-address (first (.-addresses (k/get store eavt-address nil {:sync? true}))))

(.-keys (k/get store (last (.-addresses (k/get store first-address nil {:sync? true}))) nil {:sync? true}))


(pss/slice (:eavt-key (k/get store :db nil {:sync? true})) nil nil)


(uuid [1 2 3])

(uuid 1)




(go
  (def tiered-store (<! (kt/connect-memory-tiered-store fs-store))))

#_(go
    (prn (<! (k/assoc tiered-store "foo" "bar2"))))

#_(go (prn (<! (k/get tiered-store "foo"))))

#_(k/get tiered-store "foo" nil {:sync? true})


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
              :db/valueType   :db.type/long}])

(def cfg {:store {:backend :file
                    :path "/tmp/file-store2"}
          #_{:backend :tiered
           :frontend-store {:backend :mem :id (hasch.core/uuid)}
           :backend-store {:backend :indexeddb
                           :name "sandbox-store46"}
           #_{:backend :file :path "/tmp/sandbox-store9"}}
          :keep-history? true
          :schema-flexibility :write
          :attribute-refs? false})

 

(go
  (def cfg (<! (d/create-database cfg))))

(throw cfg)

(go
  (def test-connected (<! (ds/empty-store (assoc (:store cfg) :opts {:sync? false})))))

(go
  (def conn (<! (d/connect cfg {:sync? false}))))


(go (.log js/console "synced" (<!  (kt/maybe-sync-on-connect (:store @(:wrapped-atom conn)) {:sync? false}))))

(throw conn)


(go
  (def tx-report (<! (d/transact! conn schema))))

(throw tx-report)

(go
  (def tx-report2 (<! (d/transact! conn [{:name "Peters"
                                          :age 42}]))))

;; transact 1000 dummy entities
(go
  (def tx-report3 (<! (d/transact! conn (map (fn [i] {:name (str "Name" i)
                                                      :age  i})
                                             (range 1000))))))

;; this does not properly load after restart
(d/q '[:find (count ?e) :where [?e :name ?n]] @conn)


(into {} (d/entity @conn 4))


(d/pull @conn '[:db/id :name] 4)

(d/q '[:find (count ?e) :where [?e :name ?n]] (d/as-of @conn (js/Date.)))


(d/history @conn)

(d/q '[:find (count ?e) :where [?e :name ?n]] (d/history @conn))

(d/datoms @conn :eavt)
