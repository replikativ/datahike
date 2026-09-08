(ns datahike.test.dependency-tracking-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing]])
            [datahike.core :as dc]
            [datahike.api :as d]
            [datahike.dependency-tracking :as deps]))

(def selector
  {:attributes #{:meta/flag}
   :namespaces #{"catalog"}
   :namespace-prefixes #{"app.catalog."}})

(defn- apply-tx
  ([db data] (:db-after (dc/with db data)))
  ([db data options] (:db-after (dc/with db data nil options))))

(defn- fixture-db
  ([] (fixture-db false))
  ([refs?]
   (apply-tx
    (dc/empty-db {} {:schema-flexibility :write :attribute-refs? refs?
                     :keep-history? true})
    (into (mapv (fn [attr]
                  {:db/ident attr :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one})
                [:meta/flag :catalog/value :app.catalog.types/value
                 :app.catalogue/value :row/value])
          [{:db/id 10000 :meta/flag 0 :catalog/value 0
            :app.catalog.types/value 0 :app.catalogue/value 0 :row/value 0}]))))

(defn- enrolled [db]
  (apply-tx db [] {:track-dependencies {::catalog selector}}))

(deftest enrollment-is-an-immutable-snapshot-transition
  (let [source (fixture-db)
        tracked (enrolled source)
        token (deps/token tracked ::catalog)]
    (is (nil? (deps/token source ::catalog)))
    (is (false? (deps/valid? nil source ::catalog)))
    (is (some? token))
    (is (true? (deps/valid? token tracked ::catalog)))
    (is (false? (deps/valid? token source ::catalog)))
    (is (false? (deps/valid? token tracked ::unknown)))
    (is (identical? token (deps/token (enrolled tracked) ::catalog)))
    (is (nil? (deps/token (fixture-db) ::catalog)))))

(deftest selector-aware-tokens-require-the-consumers-complete-selector
  (let [source (fixture-db)
        expected {:attributes #{:meta/flag :catalog/value}}
        normalized (assoc expected :namespaces #{} :namespace-prefixes #{}
                          :exclude-attributes #{})
        db (apply-tx source [] {:track-dependencies {::catalog expected}})
        captured (deps/token db ::catalog expected)
        narrower {:attributes #{:meta/flag}}
        replaced (apply-tx db [] {:track-dependencies {::catalog narrower}})
        narrow-token (deps/token replaced ::catalog)
        after-omitted-change (apply-tx replaced [[:db/add 10000 :catalog/value 1]])]
    (is (some? captured))
    (is (identical? captured (deps/token db ::catalog normalized)))
    (is (true? (deps/valid? captured db ::catalog normalized)))
    (is (some? narrow-token))
    (is (nil? (deps/token replaced ::catalog expected)))
    (is (false? (deps/valid? captured replaced ::catalog expected)))
    (is (false? (deps/valid? narrow-token replaced ::catalog expected)))
    (is (true? (deps/valid? narrow-token replaced ::catalog narrower)))
    ;; A consumer must not adopt the new ordinary token and then mistakenly
    ;; trust it after a change excluded by the replacement's narrower selector.
    (is (identical? narrow-token (deps/token after-omitted-change ::catalog)))
    (is (nil? (deps/token after-omitted-change ::catalog expected)))
    (is (false? (deps/valid? narrow-token after-omitted-change ::catalog expected)))
    (is (nil? (deps/token source ::catalog expected)))
    (is (false? (deps/valid? nil source ::catalog expected)))
    (doseq [view [(dc/filter db (fn [_ _] true)) (d/history db)
                  (d/as-of db (:max-tx db)) (d/since db (:max-tx db))]]
      (is (nil? (deps/token view ::catalog expected)))
      (is (false? (deps/valid? captured view ::catalog expected))))))

(deftest selector-aware-tokens-also-check-exclusions-and-selector-kind
  (let [db (enrolled (fixture-db))
        captured (deps/token db ::catalog selector)]
    (doseq [changed [(assoc selector :exclude-attributes #{:meta/flag})
                     (assoc selector :namespace-prefixes #{"app.other."})
                     (assoc selector :namespaces #{"other"})]]
      (let [after (apply-tx db [] {:track-dependencies {::catalog changed}})]
        (is (some? (deps/token after ::catalog)))
        (is (nil? (deps/token after ::catalog selector)))
        (is (false? (deps/valid? captured after ::catalog selector)))
        (is (true? (deps/valid? (deps/token after ::catalog) after ::catalog changed)))))))

(deftest selectors-cover-exact-attributes-namespaces-and-prefixes
  (doseq [refs? [false true]]
    (let [db (enrolled (fixture-db refs?))
          token (deps/token db ::catalog)]
      (doseq [attr [:meta/flag :catalog/value :app.catalog.types/value]]
        (testing (str "selected " attr ", refs=" refs?)
          (let [changed (apply-tx db [[:db/add 10000 attr 1]])]
            (is (false? (deps/valid? token changed ::catalog)))
            (is (true? (deps/valid? token db ::catalog))))))
      (doseq [attr [:row/value :app.catalogue/value]]
        (testing (str "unselected " attr ", refs=" refs?)
          (is (true? (deps/valid? token (apply-tx db [[:db/add 10000 attr 1]])
                                  ::catalog))))))))

(deftest independent-forks-never-share-a-change-count-token
  (let [db (enrolled (fixture-db))
        left (apply-tx db [[:db/add 10000 :meta/flag 1]])
        right (apply-tx db [[:db/add 10000 :meta/flag 2]])]
    (is (not (identical? (deps/token left ::catalog) (deps/token right ::catalog))))
    (is (false? (deps/valid? (deps/token left ::catalog) right ::catalog)))
    (is (true? (deps/valid? (deps/token left ::catalog) left ::catalog)))))

(deftest cas-and-retract-entity-use-the-same-mutation-tracking
  (doseq [refs? [false true]]
    (let [db (enrolled (apply-tx (fixture-db refs?) [{:db/id 10001 :row/value 5}]))
          captured (deps/token db ::catalog)
          ;; CAS currently expects resolved attributes on attribute-ref DBs.
          ;; Exercise that supported operation path independently of parsing.
          selected-attr (if refs? (get (:ident-ref-map db) :meta/flag) :meta/flag)
          row-attr (if refs? (get (:ident-ref-map db) :row/value) :row/value)]
      (doseq [operation [:db/cas :db.fn/cas]]
        (let [changed (apply-tx db [[operation 10000 selected-attr 0 1]])]
          (is (false? (deps/valid? captured changed ::catalog)))
          (is (= 1 (:meta/flag (d/entity changed 10000)))))
        (is (true? (deps/valid? captured
                                (apply-tx db [[operation 10000 row-attr 0 1]]) ::catalog)))
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (apply-tx db [[operation 10000 selected-attr 99 1]])))
        (is (true? (deps/valid? captured db ::catalog))))
      (is (true? (deps/valid? captured (apply-tx db [[:db/retractEntity 10001]]) ::catalog)))
      (let [removed (apply-tx db [[:db/retractEntity 10000]])]
        (is (false? (deps/valid? captured removed ::catalog)))
        (is (nil? (:meta/flag (d/entity removed 10000))))))))

(deftest derived-tuple-mutations-invalidate-a-tuple-only-selector
  (let [schema-db (apply-tx
                   (dc/empty-db {} {:schema-flexibility :write :attribute-refs? false})
                   [{:db/ident :tuple/left :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                    {:db/ident :tuple/right :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                    {:db/ident :tuple/pair :db/valueType :db.type/tuple
                     :db/cardinality :db.cardinality/one :db/tupleAttrs [:tuple/left :tuple/right]}])
        base (apply-tx schema-db [{:db/id 10000 :tuple/left 1 :tuple/right 2}])
        db (apply-tx base [] {:track-dependencies {::tuple {:attributes #{:tuple/pair}}}})
        captured (deps/token db ::tuple)
        after (apply-tx db [[:db/add 10000 :tuple/left 3]])]
    (is (= [1 2] (:tuple/pair (d/entity db 10000))))
    (is (= [3 2] (:tuple/pair (d/entity after 10000))))
    (is (false? (deps/valid? captured after ::tuple)))
    (is (true? (deps/valid? captured db ::tuple)))))

(deftest direct-import-emits-dependency-mutations
  (let [db (enrolled (fixture-db))
        captured (deps/token db ::catalog)
        import-row (fn [attribute]
                     (:db-after (dc/load-entities-with
                                 db [[20000 attribute 9 (inc (:max-tx db)) true]] nil)))
        unrelated (import-row :row/value)
        selected (import-row :meta/flag)]
    (is (true? (deps/valid? captured unrelated ::catalog)))
    (is (false? (deps/valid? captured selected ::catalog)))
    (is (= #{0 9} (set (d/q '[:find [?v ...] :where [_ :meta/flag ?v]] selected))))
    (is (true? (deps/valid? captured db ::catalog)))))

(deftest intermediate-transaction-prefixes-have-immutable-tokens
  (let [db (enrolled (fixture-db))
        seen (atom [])
        observe (fn [txdb]
                  (swap! seen conj (deps/token txdb ::catalog))
                  [])
        after (apply-tx db [[:db.fn/call observe]
                            [:db/add 10000 :meta/flag 1]
                            [:db.fn/call observe]
                            [:db/add 10000 :row/value 1]
                            [:db.fn/call observe]
                            [:db/add 10000 :meta/flag 0]
                            [:db.fn/call observe]])
        [initial changed irrelevant reverted] @seen]
    (is (= 4 (count @seen)))
    (is (identical? initial (deps/token db ::catalog)))
    (is (not (identical? initial changed)))
    (is (identical? changed irrelevant))
    (is (not (identical? changed reverted)))
    (is (not (identical? initial reverted)))
    (is (identical? reverted (deps/token after ::catalog)))
    (is (true? (deps/valid? initial db ::catalog)))))

(deftest enrollment-merges-removes-and-replaces-slots
  (let [db (enrolled (fixture-db))
        original (deps/token db ::catalog)
        both (apply-tx db [] {:track-dependencies {::rows {:attributes #{:row/value}}}})
        rows-token (deps/token both ::rows)
        replaced (apply-tx both [] {:track-dependencies {::catalog {:attributes #{:catalog/value}}}})
        removed (apply-tx both [] {:track-dependencies {::catalog nil}})]
    (is (identical? original (deps/token both ::catalog)))
    (is (some? rows-token))
    (is (not (identical? original (deps/token replaced ::catalog))))
    (is (identical? rows-token (deps/token replaced ::rows)))
    (is (nil? (deps/token removed ::catalog)))
    (is (false? (deps/valid? original removed ::catalog)))
    (is (identical? rows-token (deps/token removed ::rows)))
    (is (not (identical? original (deps/token (enrolled removed) ::catalog))))))

(deftest schema-and-ident-changes-conservatively-invalidate
  (let [db (enrolled (fixture-db true))
        token (deps/token db ::catalog)]
    (doseq [tx [[{:db/ident :unrelated/new :db/valueType :db.type/long
                  :db/cardinality :db.cardinality/one}]
                [[:db/add [:db/ident :row/value] :db/noHistory true]]
                [[:db/add [:db/ident :row/value] :db/ident :row/renamed]]]]
      (is (false? (deps/valid? token (apply-tx db tx) ::catalog))))))

(deftest read-flexibility-tracks-undeclared-selected-attributes
  (let [base (dc/empty-db {} {:schema-flexibility :read :attribute-refs? false})
        db (enrolled base)
        token (deps/token db ::catalog)]
    (is (true? (deps/valid? token (apply-tx db [{:row/undeclared 1}]) ::catalog)))
    (is (false? (deps/valid? token (apply-tx db [{:catalog/undeclared 1}]) ::catalog)))
    (is (false? (deps/valid? token (apply-tx db [{:app.catalog.new/value 1}]) ::catalog)))))

(deftest failure-does-not-mutate-source-tracking
  (let [db (enrolled (fixture-db))
        token (deps/token db ::catalog)]
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (apply-tx db [[:db/add 10000 :meta/flag 1]
                               [:db/add 10000 :row/value "not a long"]])))
    (is (true? (deps/valid? token db ::catalog)))
    (is (true? (deps/valid? token (apply-tx db [[:db/add 10000 :row/value 2]])
                            ::catalog)))))

(deftest malformed-and-over-limit-options-fail-before-source-changes
  (let [db (enrolled (fixture-db))
        token (deps/token db ::catalog)
        long-term (apply str (repeat 513 "x"))]
    (doseq [tracking [false []
                      {::bad {:unknown #{:meta/flag}}}
                      {::bad {:attributes #{"meta/flag"}}}
                      {::bad {:namespaces #{:catalog}}}
                      {::bad {:namespace-prefixes #{1}}}
                      {::bad {:namespaces #{long-term}}}
                      {::bad {:attributes (set (map #(keyword "attr" (str %)) (range 257)))}}
                      (into {} (map #(vector (keyword "group" (str %)) selector) (range 17)))]]
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (apply-tx db [[:db/add 10000 :meta/flag 1]]
                             {:track-dependencies tracking})))
      (is (true? (deps/valid? token db ::catalog))))))

(deftest group-limit-counts-inherited-slots
  (let [base (fixture-db)
        sixteen (into {} (map #(vector (keyword "group" (str %)) selector) (range 16)))
        db (apply-tx base [] {:track-dependencies sixteen})]
    (is (every? #(some? (deps/token db %)) (keys sixteen)))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (apply-tx db [] {:track-dependencies {::seventeenth selector}})))
    (is (some? (deps/token
                (apply-tx db [] {:track-dependencies {(keyword "group" "0") nil ::replacement selector}})
                ::replacement)))))

(deftest exclusions-win-over-selection-but-never-hide-schema-changes
  (let [db (apply-tx
            (fixture-db true) []
            {:track-dependencies
             {::catalog {:attributes #{:meta/flag :db/ident}
                         :namespaces #{"catalog" "db"}
                         :exclude-attributes #{:meta/flag :catalog/value :db/ident
                                               :db/noHistory :db/txInstant}}}})
        captured (deps/token db ::catalog)]
    (doseq [attr [:meta/flag :catalog/value]]
      (is (true? (deps/valid? captured (apply-tx db [[:db/add 10000 attr 1]])
                              ::catalog))))
    (doseq [op [[:db/add [:db/ident :row/value] :db/noHistory true]
                [:db/add [:db/ident :row/value] :db/ident :row/renamed]]]
      (is (false? (deps/valid? captured (apply-tx db [op]) ::catalog))))))

(deftest derived-views-do-not-inherit-ordinary-db-certificates
  (let [db (enrolled (fixture-db))
        captured (deps/token db ::catalog)
        tx (:max-tx db)]
    (doseq [view [(dc/filter db (fn [_ _] true))
                  (d/history db)
                  (d/as-of db tx)
                  (d/since db tx)]]
      (is (nil? (deps/token view ::catalog)))
      (is (false? (deps/valid? captured view ::catalog))))))

(deftest selected-purges-invalidate-current-and-historical-dependencies
  (doseq [refs? [false true]]
    (let [db (enrolled (fixture-db refs?))
          captured (deps/token db ::catalog)]
      (doseq [op [[:db/purge 10000 :meta/flag 0]
                  [:db.purge/attribute 10000 :meta/flag]
                  [:db.purge/entity 10000]]]
        (let [after (apply-tx db [op])]
          (is (false? (deps/valid? captured after ::catalog))
              (str "selected current purge " op ", refs=" refs?))
          (is (nil? (:meta/flag (d/entity after 10000))))
          (is (empty? (d/q '[:find [?v ...] :where [10000 :meta/flag ?v]]
                           (d/history after))))))
      (let [historical (apply-tx db [[:db/add 10000 :meta/flag 1]])
            before-purge (deps/token historical ::catalog)
            after-purge (apply-tx historical [[:db/purge 10000 :meta/flag 0]])]
        (is (false? (deps/valid? before-purge after-purge ::catalog))
            "Removing only an older value still invalidates conservatively")
        (is (= 1 (:meta/flag (d/entity after-purge 10000))))
        (is (= #{1} (set (d/q '[:find [?v ...] :where [10000 :meta/flag ?v]]
                              (d/history after-purge)))))))))

#?(:clj
   (deftest native-writer-publication-preserves-only-unaffected-tokens
     (let [config {:store {:backend :memory :id (random-uuid)}
                   :schema-flexibility :write :keep-history? true}]
       (d/create-database config)
       (let [conn (d/connect config)]
         (try
           (d/transact conn [{:db/ident :meta/flag :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :row/value :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}
                             {:db/id 10000 :meta/flag 0 :row/value 0}])
           (let [report (d/transact conn {:tx-data []
                                          :tx-options {:track-dependencies {::catalog selector}}})
                 captured (deps/token (:db-after report) ::catalog)]
             (is (some? captured))
             (is (true? (deps/valid? captured @conn ::catalog)))
             (d/transact conn [[:db/add 10000 :row/value 1]])
             (is (true? (deps/valid? captured @conn ::catalog)))
             (is (thrown? Exception
                          (d/transact conn [[:db/add 10000 :meta/flag 1]
                                            [:db/add 10000 :row/value "invalid"]])))
             (is (true? (deps/valid? captured @conn ::catalog)))
             (let [seen (atom [])
                   observe (fn [txdb] (swap! seen conj (deps/token txdb ::catalog)) [])]
               (d/transact conn [[:db.fn/call observe]
                                 [:db/add 10000 :meta/flag 2]
                                 [:db.fn/call observe]])
               (is (= 2 (count @seen)))
               (is (identical? captured (first @seen)))
               (is (not (identical? (first @seen) (second @seen))))
               (is (identical? (second @seen) (deps/token @conn ::catalog))))
             (is (false? (deps/valid? captured @conn ::catalog)))
             (is (some? (deps/token @conn ::catalog))))
           (finally (d/release conn) (d/delete-database config)))))))

#?(:clj
   (deftest reconnect-does-not-restore-runtime-tokens
     (let [^java.nio.file.Path directory
           (java.nio.file.Files/createTempDirectory
            "datahike-dependency-token-" (make-array java.nio.file.attribute.FileAttribute 0))
           config {:store {:backend :file :id (random-uuid) :path (str directory "/store")}
                   :schema-flexibility :write}
           open-conn (atom nil)]
       (try
         (d/create-database config)
         (let [conn (d/connect config)]
           (reset! open-conn conn)
           (d/transact conn [{:db/ident :meta/flag :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}
                             {:db/id 10000 :meta/flag 7}])
           (d/transact conn {:tx-data [] :tx-options {:track-dependencies {::catalog selector}}})
           (let [captured (deps/token @conn ::catalog)]
             (is (some? captured))
             (d/release conn)
             (reset! open-conn nil)
             (let [reopened (d/connect config)]
               (reset! open-conn reopened)
               (is (not (identical? conn reopened)))
               (is (= 7 (:meta/flag (d/entity @reopened 10000))))
               (is (nil? (deps/token @reopened ::catalog)))
               (is (false? (deps/valid? captured @reopened ::catalog))))))
         (finally
           (when-let [conn @open-conn] (d/release conn))
           (when (d/database-exists? config) (d/delete-database config))
           (java.nio.file.Files/deleteIfExists directory))))))
