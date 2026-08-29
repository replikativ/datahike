(ns datahike.test.http.system-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.http.system :as system]))

(deftest catalog-lifecycle-is-durable-redacted-and-auditable
  (let [configured (system/configure
                    {:system-db {:store {:backend :memory}}})
        conn       (get configured system/conn-key)
        store-id   (random-uuid)
        database   {:name "accounts"
                    :store {:backend :jdbc
                            :id store-id
                            :password "database-password"
                            :client-secret "cloud-secret"}
                    :remote-peer {:token "remote-token"}
                    :writer {:token "writer-token"}}]
    (try
      (is (true? (get-in @conn [:config :keep-history?])))
      ((get configured system/register-key) database {:sub "alice"})
      (let [[entry] (system/entries conn)]
        (is (= {:store-id (str store-id)
                :name "accounts"
                :state :active
                :created-by "alice"}
               (select-keys entry [:store-id :name :state :created-by])))
        (is (inst? (:created-at entry)))
        (is (= "REDACTED" (get-in entry [:config :store :password])))
        (is (= "REDACTED" (get-in entry [:config :store :client-secret])))
        (is (not (contains? (:config entry) :remote-peer)))
        (is (not (contains? (:config entry) :writer))))

      ((get configured system/delete-key) database {:sub "bob"})
      (let [[entry] (system/entries conn)]
        (is (= :deleted (:state entry)))
        (is (= "bob" (:deleted-by entry)))
        (is (inst? (:deleted-at entry)))
        (is (= #{["bob"]}
               (set
                (d/q
                 '[:find ?principal
                   :where
                   [_ :datahike.system.database/state :deleted ?tx true]
                   [?tx :datahike.system/principal ?principal]]
                 (d/history @conn))))
            "the acting principal is attached to the lifecycle transaction"))

      (testing "recreating the same store reactivates its one catalog identity"
        ((get configured system/register-key) database {:sub "carol"})
        (let [[entry :as entries] (system/entries conn)]
          (is (= 1 (count entries)))
          (is (= :active (:state entry)))
          (is (= "alice" (:created-by entry))
              "reactivation preserves the original creator")
          (is (nil? (:deleted-at entry)))
          (is (nil? (:deleted-by entry)))))
      (finally
        (system/close! configured)))))

(deftest obsolete-auth-database-key-is-rejected
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"replaced by :system-db"
       (system/configure {:auth-db {:store {:backend :memory}}}))))
