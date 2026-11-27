(ns datahike.test.migrate-attribute-refs-schema-test
  "Export/import round trips of schema-level values under `:attribute-refs?`:
   a long-valued schema attribute whose value equals a system entity id, entity
   specs, and a map-form attribute rename. The live-path half is
   `datahike.test.attribute-refs.schema-values-test`."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.manifest :as mman])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write :attribute-refs? true})

(defn- fresh-conn [] (d/connect (doto (cfg) d/create-database)))

(defn- teardown [conn]
  (let [c (:config @conn)] (d/release conn) (d/delete-database c)))

(defn- dump-dir []
  (str (Files/createTempDirectory "dh-attr-refs-schema" (make-array FileAttribute 0)) "/dump"))

(defn- root-message [e]
  (loop [x e] (if-let [c (.getCause ^Throwable x)] (recur c) (ex-message x))))

(deftest schema-values-survive-export-import
  (let [src (fresh-conn)
        dst (fresh-conn)
        dir (dump-dir)]
    (try
      (d/transact src [{:db/ident :account/email :db/valueType :db.type/string
                        :db/unique :db.unique/identity :db/cardinality :db.cardinality/one
                        ;; 50 is the id of the system entity :dh.ref/temporal
                        :db/maxLength 50}
                       {:db/ident :account/nick :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :account/balance :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
      (d/transact src [{:db/ident :account/guard
                        :db.entity/attrs [:account/email :account/balance]}])
      (d/transact src [{:account/email "a@x" :account/balance 1 :account/nick "ann"
                        :db/ensure :account/guard}])
      (let [aid (:db/id (d/entity @src :account/nick))]
        (d/transact src [{:db/id aid :db/ident :account/handle}]))
      (m/export-db @src dir)
      (m/import-db dst dir)
      (testing ":db/maxLength keeps its number"
        (is (= 50 (:db/maxLength (get (:schema @dst) :account/email)))))
      (testing "the rename survives and the data follows it"
        (is (= #{["ann"]} (d/q '[:find ?v :where [_ :account/handle ?v]] @dst))))
      (testing "the entity spec survives and is enforced on the target"
        (is (= #{:account/email :account/balance}
               (set (:db.entity/attrs (d/pull @dst '[:db.entity/attrs] [:db/ident :account/guard])))))
        (d/transact dst [{:account/email "c@x" :account/balance 2 :db/ensure :account/guard}])
        (is (re-find #"missing attributes"
                     (try (d/transact dst [{:account/email "b@x" :db/ensure :account/guard}]) ""
                          (catch Exception e (root-message e))))))
      (finally (teardown src) (teardown dst)))))

(defn- collision [dst records opts]
  (try (m/import-source dst (m/records->chunk-src records 10)
                        (assoc-in opts [:source-meta :expected-count] (count records)))
       nil
       (catch clojure.lang.ExceptionInfo e
         (some #(:error (ex-data %))
               (filter #(= :import/system-entity-collision (:error (ex-data %)))
                       (take-while some? (iterate ex-cause e)))))))

(deftest a-source-may-not-put-data-on-a-system-entity
  (let [t 536870913
        records (fn [e] [[t :db/txInstant #inst "2021-01-01" t true]
                         [200 :db/ident :p/name t true]
                         [200 :db/valueType :db.type/string t true]
                         [200 :db/cardinality :db.cardinality/one t true]
                         [e :p/name "Ann" t true]])]
    (doseq [[label recs opts]
            [["no declared numbering: taken to share the target's"
              :sys {}]
             ["an :xform that moves an entity onto one"
              201 {:xform (map (fn [[e a v t op :as r]] (if (= 201 e) :sys r)))}]
             ["a declared numbering that says the id is a system entity"
              :sys :declared]]]
      (testing label
        (let [dst (fresh-conn)
              ;; a system entity every :attribute-refs? database has
              sys-e (:db/id (d/pull @dst '[:db/id] [:db/ident :db/doc]))
              before (count (d/datoms @dst :eavt sys-e))
              recs (records (if (= :sys recs) sys-e recs))
              opts (cond
                     (= :declared opts) {:source-meta {:system-idents (mman/system-idents @dst)}}
                     (:xform opts) {:xform (map (fn [r] (if (= 201 (first r)) (assoc r 0 sys-e) r)))}
                     :else opts)]
          (try
            (is (= :import/system-entity-collision (collision dst recs opts)))
            (is (= before (count (d/datoms @dst :eavt sys-e))) "nothing was attached to it")
            (finally (d/release dst))))))))
