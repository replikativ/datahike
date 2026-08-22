(ns datahike.test.http.writer-test
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [datahike.http.server :refer [start-server stop-server]]
   [datahike.http.writer]
   [datahike.api :as d]))

(deftest test-http-writer
  (testing "Testing distributed datahike.http.writer implementation."
    (let [port  31283
          server (start-server {:port     port
                                :join?    false
                                :dev-mode false
                                :token    "securerandompassword"})]
      (try
        (let [cfg    {:store              {:backend :file
                                           :path  "/tmp/distributed_writer"
                                           :id #uuid "17100000-0000-0000-0000-000000000001"}
                      :keep-history?      true
                      :schema-flexibility :read
                      :writer             {:backend :datahike-server
                                           :url     (str "http://localhost:" port)
                                           :token   "securerandompassword"}}
              conn   (do
                       (when (d/database-exists? cfg)
                         (d/delete-database cfg))
                       (d/create-database cfg)
                       (d/connect cfg))]

          (d/transact conn [{:name "Alice"
                             :age  25}])
          (is (= #{[25 "Alice"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      25)))

          (d/transact conn [{:name "Peter"
                             :age  18}])
          (is (= #{[18 "Peter"]}
                 (d/q '[:find ?a ?v
                        :in $ ?a
                        :where
                        [?e :name ?v]
                        [?e :age ?a]]
                      @conn
                      18)))

          (d/delete-database cfg))
        (finally
          (stop-server server))))))

(deftest test-http-writer-failure-without-server
  (testing "Db creation fails without writer connection."
    (let [port   38217
          cfg    {:store              {:backend :memory :id #uuid "00170000-0000-0000-0000-000000000017"}
                  :keep-history?      true
                  :schema-flexibility :read
                  :writer             {:backend :datahike-server
                                       :url     (str "http://localhost:" port)
                                       :token   "securerandompassword"}}]
      (is (thrown? Exception
                   (do
                     (d/delete-database cfg)
                     (d/create-database cfg)
                     (d/connect cfg))))))
  (testing "Transact fails without writer connection."
    (let [port 38217
          cfg  {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                :keep-history?      true
                :schema-flexibility :read
                :writer             {:backend :datahike-server
                                     :url     (str "http://localhost:" port)
                                     :token   "securerandompassword"}}
          server-cfg {:store              {:backend :memory :id #uuid "00180000-0000-0000-0000-000000000018"}
                      :keep-history?      true
                      :schema-flexibility :read}]
        ;; make sure the database exists before testing transact
      (do (d/delete-database server-cfg)
          (d/create-database server-cfg))
      (is (thrown? Exception
                   (d/transact (d/connect cfg)
                               [{:name "Should fail."}]))))))

(deftest test-http-writer-integer-widths
  (testing "Integers survive the writer transport at every width."
    ;; datahike has a single integer value type, :db.type/long. A writer
    ;; transport that does not carry integer width therefore cannot be fixed up
    ;; downstream: a value arriving as a 32-bit Integer fails the Long schema
    ;; check rather than being coerced. The values that land in that range are
    ;; the ordinary ones — epoch seconds, ids, counts — so the symptom is
    ;; "most transactions fail", not an edge case.
    ;;
    ;; The existing tests above use 18 and 25, which are the same shape as each
    ;; other. This one pins the boundaries so a future transport change has to
    ;; keep carrying them.
    (let [port   31284
          server (start-server {:port  port
                                :join? false
                                :token "securerandompassword"})]
      (try
        (let [cfg    {:store              {:backend :file
                                           :path "/tmp/writer_integer_widths"
                                           :id #uuid "17100000-0000-0000-0000-000000000002"}
                      :keep-history?      true
                      :schema-flexibility :read
                      :writer             {:backend :datahike-server
                                           :url     (str "http://localhost:" port)
                                           :token   "securerandompassword"}}
              widths {:tiny          1
                      :zero          0
                      :small         42
                      :negative      -12345
                      :int-max       (long Integer/MAX_VALUE)
                      :int-min       (long Integer/MIN_VALUE)
                      :above-int-max (inc (long Integer/MAX_VALUE))
                      :below-int-min (dec (long Integer/MIN_VALUE))
                      :epoch-seconds 1755820800
                      :big           1099511627776}   ; 2^40
              conn   (do (when (d/database-exists? cfg) (d/delete-database cfg))
                         (d/create-database cfg)
                         (d/connect cfg))]
          (d/transact conn [(assoc widths :db/ident :widths)])
          (let [read-back (d/pull @conn '[*] [:db/ident :widths])]
            (doseq [[k expected] widths]
              (is (= expected (get read-back k))
                  (str k " round-trips through the writer"))))
          (d/release conn)
          (d/delete-database cfg))
        (finally
          (stop-server server))))))
