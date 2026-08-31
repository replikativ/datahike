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

(deftest visible-catalog-pages-search-and-authorize-before-paging
  (let [configured (system/configure {:system-db {:store {:backend :memory}}})
        databases  (vec (for [n (range 30)]
                          {:name (format "db-%02d" n)
                           ;; Search covers both names and store IDs. Random
                           ;; UUIDs occasionally contain `db-2`, adding an
                           ;; unrelated row and making this assertion flaky.
                           :store {:backend :memory
                                   :id (java.util.UUID. 0 (long n))}}))]
    (try
      (doseq [database databases]
        ((get configured system/register-key) database {:sub "root"}))
      (let [page (system/visible-entry-page configured {:sub "root"}
                                            {:offset 10 :limit 7})]
        (is (= 30 (get-in page [:page :total])))
        (is (= 7 (count (:databases page))))
        (is (= "db-10" (get-in page [:databases 0 :name])))
        (is (true? (get-in page [:page :has-more?]))))
      (let [page (system/visible-entry-page configured {:sub "root"}
                                            {:q "DB-2" :offset 0 :limit 24})]
        (is (= 10 (get-in page [:page :total])))
        (is (= (mapv #(format "db-%02d" %) (range 20 30))
               (mapv :name (:databases page)))))
      (let [allowed (set (map (comp str :id :store) (take 3 databases)))
            restricted (assoc configured :authorize
                              (fn [{:keys [db]}]
                                (contains? allowed (str (:store-id db)))))
            page (system/visible-entry-page restricted {:sub "reader"}
                                            {:offset 0 :limit 2})]
        (is (= 3 (get-in page [:page :total])))
        (is (= 2 (count (:databases page))))
        (is (true? (get-in page [:page :has-more?]))))
      (finally
        (system/close! configured)))))

(deftest catalog-lifecycle-events-follow-durable-state
  (let [configured (system/configure
                    {:system-db {:store {:backend :memory}}})
        events     (atom [])
        database   {:name "events"
                    :store {:backend :memory :id (random-uuid)}}
        subscription (system/subscribe!
                      configured
                      #(swap! events conj (select-keys % [:event :config :principal])))]
    (try
      ((get configured system/prepare-register-key) database {:sub "alice"})
      ((get configured system/register-key) database {:sub "alice"})
      ((get configured system/prepare-delete-key) database {:sub "bob"})
      ((get configured system/cancel-delete-key) database {:sub "bob"})
      ((get configured system/prepare-delete-key) database {:sub "carol"})
      ((get configured system/delete-key) database {:sub "carol"})
      (is (= [:creating :created :deleting :delete-cancelled :deleting :deleted]
             (mapv :event @events)))
      (is (= ["alice" "alice" "bob" "bob" "carol" "carol"]
             (mapv (comp :sub :principal) @events)))
      (system/unsubscribe! configured subscription)
      ((get configured system/register-key) database {:sub "nobody"})
      (is (= 6 (count @events)))
      (finally
        (system/close! configured)))))

(deftest failed-delete-preparation-restores-state-and-listeners
  (let [configured (system/configure
                    {:system-db {:store {:backend :memory}}})
        events     (atom [])
        database   {:name "rollback"
                    :store {:backend :memory :id (random-uuid)}}]
    (try
      ((get configured system/register-key) database {:sub "root"})
      (system/subscribe!
       configured
       (fn [{:keys [event]}]
         (swap! events conj event)
         (when (= :deleting event)
           (throw (ex-info "listener could not release" {})))))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"could not release"
           ((get configured system/prepare-delete-key) database {:sub "root"})))
      (is (= [:deleting :delete-cancelled] @events))
      (is (= :active (:state (first (system/entries
                                     (get configured system/conn-key))))))
      (finally
        (system/close! configured)))))
