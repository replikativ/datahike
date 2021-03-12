(ns datahike.test.ident-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [clojure.core.async :refer [go <!]]
   [datahike.core :as d]
   [datahike.impl.entity :as de]))

(def db
  #?(:cljs
     #(go (-> (<! (d/empty-db {:ref {:db/valueType :db.type/ref}}))
             (d/db-with [[:db/add 1 :db/ident :ent1]
                         [:db/add 2 :db/ident :ent2]
                         [:db/add 2 :ref 1]])
             (<!)))
     :clj
     (-> (d/empty-db {:ref {:db/valueType :db.type/ref}})
         (d/db-with [[:db/add 1 :db/ident :ent1]
                     [:db/add 2 :db/ident :ent2]
                     [:db/add 2 :ref 1]]))))


(deftest test-q
  #?(:cljs
     (t/async done
              (go
                (is (= 1 (<! (d/q '[:find ?v .
                                    :where [:ent2 :ref ?v]] (<! (db))))))
                (is (= 2 (<! (d/q '[:find ?f .
                                    :where [?f :ref :ent1]] (<! (db))))))
                (done)))
     :clj
     (do
       (is (= 1 (d/q '[:find ?v .
                       :where [:ent2 :ref ?v]] db)))
       (is (= 2 (d/q '[:find ?f .
                       :where [?f :ref :ent1]] db))))))

(deftest test-transact!
  #?(:cljs 
     (t/async done
              (go
                (let [db' (<! (d/db-with (<! (db)) [[:db/add :ent1 :ref :ent2]]))]
                  (is (= 2 (-> (<! (de/touch (<! (d/entity db' :ent1)))) 
                               :ref 
                               (de/touch)
                               (<!)
                               :db/id))))
                (done)))
     :clj
     (let [db' (d/db-with db [[:db/add :ent1 :ref :ent2]])]
       (is (= 2 (-> (d/entity db' :ent1) :ref :db/id))))))

(deftest test-entity
  #?(:cljs
     (t/async done 
              (go
                (is (= {:db/ident :ent1}
                       (into {} (<! (de/touch (<! (d/entity (<! (db)) :ent1)))))))
                (done)))
     :clj
     (is (= {:db/ident :ent1}
            (into {} (d/entity db :ent1))))))

(deftest test-pull
  #?(:cljs 
     (t/async done
              (go (is (= {:db/id 1, :db/ident :ent1}
                         (<! (d/pull (<! (db)) '[*] :ent1))))
                  (done)))
     :clj (is (= {:db/id 1, :db/ident :ent1}
                 (d/pull db '[*] :ent1)))))

#_(user/test-var #'test-transact!)
#_(t/test-ns 'datahike.test.ident)
