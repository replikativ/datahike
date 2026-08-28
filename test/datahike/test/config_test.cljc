(ns datahike.test.config-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj [clojure.test :as t :refer [is are deftest testing]])
   [datahike.config :as c]
   [datahike.db :as db]
   [datahike.api :as d]
   [datahike.index :as di]
   [datahike.index.hitchhiker-tree :as dih]
   [datahike.migrate.fs :as fs]
   [datahike.connector :as conn]))

#?(:cljs (def Throwable js/Error))

(deftest int-from-env-test
  (is (= 1000
         (c/int-from-env :foo 1000))))

(deftest bool-from-env-test
  (is (c/bool-from-env :foo true)))

(deftest uri-test
  ;; The only /tmp literals in this namespace that stay: nothing here opens a
  ;; store, `uri->config` just parses. The path is the parser's INPUT and its
  ;; expected output, so it has to be spelled the same on both sides.
  (let [mem-uri "datahike:mem://config-test"
        file-uri "datahike:file:///tmp/config-test"]

    (are [x y] (= x (c/uri->config y))
      {:backend :memory :host "config-test" :uri mem-uri}
      mem-uri

      {:backend :file :path "/tmp/config-test" :uri file-uri}
      file-uri)))

(deftest deprecated-test
  (let [mem-cfg {:backend :memory
                 :host "deprecated-test"}
        file-cfg {:backend :file
                  :path "/deprecated/test"}
        default-new-cfg {:attribute-refs? false
                         :keep-history? true
                         :initial-tx nil
                         :index :datahike.index/persistent-set
                         :index-config {}
                         :schema-flexibility :write
                         :crypto-hash? false
                         :branch :db
                         :writer c/self-writer
                         :search-cache-size c/*default-search-cache-size*
                         :store-cache-size c/*default-store-cache-size*}]
    (is (= (merge default-new-cfg
                  {:store {:backend :memory :id #uuid "ec3537bd-3f0d-3719-acd5-40751bbb1012"}})
           (c/from-deprecated mem-cfg)))
    (is (= (merge default-new-cfg
                  {:store {:backend :file
                           :path "/deprecated/test"
                           :id #uuid "908d33ed-b562-3301-9a9f-94b961e56f05"}})
           (c/from-deprecated file-cfg)))))

(deftest load-config-test
  (testing "configuration defaults"
    (let [config (c/load-config)]
      (is (= (merge {:store {:backend :memory}
                     :attribute-refs? c/*default-attribute-refs?*
                     :keep-history? c/*default-keep-history?*
                     :schema-flexibility c/*default-schema-flexibility*
                     :index c/*default-index*
                     :crypto-hash? c/*default-crypto-hash?*
                     :branch c/*default-db-branch*
                     :writer c/self-writer
                     :search-cache-size c/*default-search-cache-size*
                     :store-cache-size c/*default-store-cache-size*}
                    (when (seq (di/default-index-config c/*default-index*))
                      {:index-config (di/default-index-config c/*default-index*)}))
             (update config :store dissoc :id :scope))))))

(deftest writer-ownership-defaults
  (testing "the local writer is explicitly normalized to safe shared ownership"
    (is (= {:backend :self :writer-ownership :shared}
           (:writer (c/load-config {:store {:backend :memory}})))))

  (testing "the self-only default does not leak through deep merge into remote writers"
    (doseq [writer [{:backend :datahike-server :url "http://localhost:3000"}
                    {:backend :kabel :peer-id :remote :local-peer :local}]]
      (is (= writer (:writer (c/load-config {:store {:backend :memory}
                                             :writer writer}))))))

  (testing "the experimental streaming option is translated before the default applies"
    (is (= {:backend :self :writer-ownership :shared}
           (:writer (c/load-config {:writer {:backend :self :streaming? false}}))))
    (is (= {:backend :self :writer-ownership :exclusive}
           (:writer (c/load-config {:writer {:backend :self :streaming? true}}))))))

(deftest core-config-test
  (testing "Schema on write in core empty database"
    (is (thrown-with-msg? Throwable
                          #"Bad entity attribute :name at \{:db/id 1, :name \"Ivan\"\}, not defined in current schema"
                          (d/db-with (db/empty-db nil {:schema-flexibility :write})
                                     [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                                      {:db/id 2 :name "Petr" :age 37 :huh? false}])))
    (is (thrown-with-msg? Throwable
                          #"Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
                          (db/empty-db {:name {:db/cardinality :db.cardinality/one}} {:schema-flexibility :write})))
    (is (= #{["Alice"]}
           (let [db (-> (db/empty-db {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}} {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))
    (is (= #{["Alice"]}
           (let [db (-> (db/empty-db [{:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}]
                                     {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))))

(deftest store-identity-config-test
  (testing "different configs with equal identities"
    ;; `file-path` is bound ONCE: file-start and file-index differ only in
    ;; :index, and that is the whole setup — two different paths would be two
    ;; different store identities and the test would assert the wrong error.
    (let [file-path  (fs/temp-dir! "dh-store-identity-test")
          mem-start  {:store {:backend :memory
                              :id #uuid "51de0715-1000-0000-0000-000000000001"}}
          mem-other  {:store {:backend :memory
                              :id #uuid "a107be12-1de0-7157-0000-000000000001"}}
          file-start {:store {:backend :file
                              :path file-path
                              :id #uuid "1de07157-0000-0000-0000-000000000001"}}
          file-index {:index :datahike.index/hitchhiker-tree
                      :store {:backend :file
                              :path file-path
                              :id #uuid "1de07157-0000-0000-0000-000000000001"}}
          mem-named  {:name "has-name"
                      :store {:backend :memory
                              :id #uuid "51de0715-1000-0000-0000-000000000001"}}
          mem-same   {:store {:backend :memory
                              :id #uuid "51de0715-1000-0000-0000-000000000001"
                              :irrelevant-property true}}]
      (is (thrown-with-msg? Throwable
                            #"Store identity mismatch"
                            (conn/ensure-stored-config-consistency mem-start mem-other)))
      (is (not= mem-start mem-other))
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match stored configuration."
                            (conn/ensure-stored-config-consistency file-start file-index)))
      (is (not= file-start file-index))
      (is (thrown-with-msg? Throwable
                            #"Store identity mismatch"
                            (conn/ensure-stored-config-consistency mem-start file-start)))
      (is (not= mem-start file-start))
      (is (nil? (conn/ensure-stored-config-consistency mem-start mem-named)))
      (is (not= mem-start mem-named))
      (is (nil? (conn/ensure-stored-config-consistency mem-start mem-same)))
      (is (not= mem-start mem-same))
      (is (nil? (conn/ensure-stored-config-consistency mem-named mem-same)))
      (is (not= mem-named mem-same)))))

(deftest store-identity-connection-test
  (testing "different connections with equal identities"
    ;; as above: one path, bound once — file-index has to reach the store
    ;; file-start is already connected to.
    (let [file-path  (fs/temp-dir! "dh-store-connection-test")
          mem-start  {:store {:backend :memory
                              :id #uuid "51dec000-ec71-0000-0000-000000000001"}}
          mem-other  {:store {:backend :memory
                              :id #uuid "a107be12-c000-ec71-0000-000000000001"}}
          file-start {:store {:backend :file
                              :path file-path
                              :id #uuid "c000ec71-0000-0000-0000-000000000001"}}
          file-index {:index :datahike.index/hitchhiker-tree
                      :store {:backend :file
                              :path file-path
                              :id #uuid "c000ec71-0000-0000-0000-000000000001"}}
          mem-named  {:name "has-name"
                      :store {:backend :memory
                              :id #uuid "51dec000-ec71-0000-0000-000000000001"}}
          mem-same   {:store {:backend :memory
                              :id #uuid "51dec000-ec71-0000-0000-000000000001"
                              :irrelevant-property true}}
          _ (doall (map d/delete-database [mem-start mem-other file-start file-index mem-named mem-same]))]
      (d/create-database mem-start)
      (is (= (d/connect mem-start) (d/connect mem-named) (d/connect mem-same)))
      (d/create-database file-start)
      (d/connect file-start)
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match existing connections."
                            (d/connect file-index))))))

(deftest self-only-writer-options-are-refused-on-remote-writers
  (testing "every self-writer option is refused, not ignored, on a remote backend.
            A remote writer transacts in another process, so none of these can be
            honoured from here — and :require-fencing in particular names a
            safety property, so a config that carries it and connects anyway
            LOOKS protected while nothing checks anything."
    (doseq [k [:writer-ownership :streaming? :require-fencing
               :max-batch :head-conflict-retries :head-conflict-backoff-ms]]
      (let [e (try (c/normalize-writer-config {:backend :datahike-server k :anything})
                   ::not-thrown
                   (catch #?(:clj Exception :cljs js/Error) ex (ex-data ex)))]
        (is (= :self-writer-options-on-remote-writer (:type e))
            (str k " must be refused on a remote writer"))
        (is (= [k] (:inert e))))))

  (testing "a remote writer's own keys pass untouched — the key set stays open
            (:kabel has :local-peer and friends), only the self-only keys are
            closed"
    (is (= {:backend :kabel :local-peer :a-peer}
           (c/normalize-writer-config {:backend :kabel :local-peer :a-peer}))))

  (testing "the same options on the :self backend keep their meaning"
    (is (= :exclusive (:writer-ownership
                       (c/normalize-writer-config
                        {:backend :self :writer-ownership :exclusive}))))
    (is (= :global (:require-fencing
                    (c/normalize-writer-config
                     {:backend :self :require-fencing :global}))))))

#?(:clj
   (deftest self-only-writer-options-are-refused-at-connect
     (testing "the guard fires through the public API, not only when the helper
               is called directly: connect normalizes every writer map before it
               touches a store or a writer. (create-database with a remote
               writer dispatches to the REMOTE endpoint first, so the config is
               that server's to validate — connect is the local entry point.)"
       (let [base {:store {:backend :memory :id #uuid "cf917000-0000-4000-8000-000000000001"}
                   :schema-flexibility :read
                   :keep-history? false}]
         (d/delete-database base)
         (d/create-database base)
         (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"inert on a remote writer"
              (d/connect (assoc base :writer {:backend :datahike-server
                                              :require-fencing :global}))))))))
