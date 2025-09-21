(ns sandbox
  (:require
   [datahike.api :as d]
   [konserve.core :as k]
   [datahike.store :as ds]
   #_[konserve.indexeddb :as idb]
   [konserve.node-filestore :as fs]
   [konserve.tiered :as kt]
   [clojure.core.async :refer [<! >! chan put! close!]])
  (:require-macros [clojure.core.async :refer [go]]))


#_(go
    (def idb-store (<! (idb/connect-idb-store "sandbox"))))

#_(go
  (def fs-store (<! (fs/connect-fs-store "/tmp/tiered-sandbox"))))

#_(k/get (:store @(:wrapped-atom conn)) #uuid "2bcd3ae3-845e-4636-8ac0-e28f00854ad4" nil {:sync? true})




#_(go
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

(def cfg {:store {:backend :file :path "/tmp/tiered-sandbox"}
          :keep-history? true
          :schema-flexibility :write
          :attribute-refs? false})


(go
  (def cfg (<! (d/create-database cfg))))

(def conn (d/connect cfg))

(go
  (def tx-report (<! (d/transact! conn schema))))

(go
  (def tx-report2 (<! (d/transact! conn [{:name "Petersjj"
                                          :age 42}]))))

;; transact 1000 dummy entityies
(go
  (def tx-report3 (<! (d/transact! conn (map (fn [i] {:name (str "Name" i)
                                                      :age  i})
                                             (range 1000))))))

;; this does not properly load after restart
(d/q '[:find ?e ?v :where [?e :name ?v]] @conn)


(into {} (d/entity @conn 4))


(d/pull @conn '[:db/id :name] 4)

(d/as-of @conn (js/Date.))


(d/history @conn)

(d/datoms @conn :eavt)
