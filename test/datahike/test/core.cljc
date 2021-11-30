(ns datahike.test.core-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj [clojure.test :as t :refer [is deftest]])
   [clojure.walk]
   [clojure.data]
   #?(:clj [kaocha.stacktrace])
   [datahike.core :as dc]
   [datahike.constants :as const]
   [datahike.impl.entity :as de]
   #?(:cljs [datahike.test.cljs])))

#?(:cljs
   (enable-console-print!))

;; Added special case for printing ex-data of ExceptionInfo
#?(:cljs
   (defmethod t/report [::t/default :error] [m]
     (t/inc-report-counter! :error)
     (println "\nERROR in" (t/testing-vars-str m))
     (when (seq (:testing-contexts (t/get-current-env)))
       (println (t/testing-contexts-str)))
     (when-let [message (:message m)] (println message))
     (println "expected:" (pr-str (:expected m)))
     (print "  actual: ")
     (let [actual (:actual m)]
       (cond
         (instance? ExceptionInfo actual)
         (println (.-stack actual) "\n" (pr-str (ex-data actual)))
         (instance? js/Error actual)
         (println (.-stack actual))
         :else
         (prn actual)))))

#?(:cljs (def test-summary (atom nil)))
#?(:cljs (defmethod t/report [::t/default :end-run-tests] [m]
           (reset! test-summary (dissoc m :type))))

(defn wrap-res [f]
  #?(:cljs (do (f) (clj->js @test-summary))
     :clj (let [res (f)]
            (when (pos? (+ (:fail res) (:error res)))
              (System/exit 1)))))

;; utils
#?(:clj
   (defmethod t/assert-expr 'thrown-msg? [msg form]
     (let [[_ match & body] form]
       `(try ~@body
             (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
             (catch Throwable e#
               (let [m# (.getMessage e#)]
                 (if (= ~match m#)
                   (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                   (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
               e#)))))

(defn entity-map [db e]
  (when-let [entity (dc/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (dc/datoms db :eavt)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

;; Filter Kaocha frames from exceptions

#?(:clj
   (alter-var-root #'kaocha.stacktrace/*stacktrace-filters* (constantly ["java." "clojure." "kaocha." "orchestra."])))

;; Core tests

(deftest test-protocols
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        db (dc/db-with (dc/empty-db schema)
                       [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                        {:db/id 2 :name "Petr" :age 37 :huh? false}])]
    (is (= (dc/empty-db schema)
           (empty db)))
    (is (= 6 (count db)))
    (is (= (set (seq db))
           #{(dc/datom 1 :aka "IV")
             (dc/datom 1 :aka "Terrible")
             (dc/datom 1 :name "Ivan")
             (dc/datom 2 :age 37)
             (dc/datom 2 :name "Petr")
             (dc/datom 2 :huh? false)}))))

(defn- now []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(deftest test-uuid
  (let [now-ms (loop []
                 (let [ts (now)]
                   (if (> (mod ts 1000) 900) ;; sleeping over end of a second
                     (recur)
                     ts)))
        now    (int (/ now-ms 1000))]
    (is (= (* 1000 now) (dc/squuid-time-millis (dc/squuid))))
    (is (not= (dc/squuid) (dc/squuid)))
    (is (= (subs (str (dc/squuid)) 0 8)
           (subs (str (dc/squuid)) 0 8)))))

(deftest test-diff
  (is (= [[(dc/datom 1 :b 2) (dc/datom 1 :c 4) (dc/datom 2 :a 1)]
          [(dc/datom 1 :b 3) (dc/datom 1 :d 5)]
          [(dc/datom 1 :a 1)]]
         (clojure.data/diff
          (dc/db-with (dc/empty-db) [{:a 1 :b 2 :c 4} {:a 1}])
          (dc/db-with (dc/empty-db) [{:a 1 :b 3 :d 5}])))))

;; connection tests
(def user-schema {:aka {:db/cardinality :db.cardinality/many}})
(def user-schema' {:email {:db/unique :db.unique/identity}})

(def schema (merge const/non-ref-implicit-schema user-schema))
(def schema' (merge const/non-ref-implicit-schema user-schema'))

(def datoms #{(dc/datom 1 :age  17)
              (dc/datom 1 :name "Ivan")})

(deftest test-ways-to-create-conn
  (let [conn (dc/create-conn)]
    (is (= #{} (set (dc/datoms @conn :eavt))))
    (is (= const/non-ref-implicit-schema (:schema @conn))))

  (let [conn (dc/create-conn user-schema)]
    (is (= #{} (set (dc/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))

  (let [conn (dc/conn-from-datoms datoms)]
    (is (= datoms (set (dc/datoms @conn :eavt))))
    (is (= const/non-ref-implicit-schema (:schema @conn))))

  (let [conn (dc/conn-from-datoms datoms user-schema)]
    (is (= datoms (set (dc/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))

  (let [conn (dc/conn-from-db (dc/init-db datoms))]
    (is (= datoms (set (dc/datoms @conn :eavt))))
    (is (= const/non-ref-implicit-schema (:schema @conn))))

  (let [conn (dc/conn-from-db (dc/init-db datoms user-schema))]
    (is (= datoms (set (dc/datoms @conn :eavt))))
    (is (= schema (:schema @conn)))))

(deftest test-reset-conn!
  (let [conn    (dc/conn-from-datoms datoms user-schema)
        report  (atom nil)
        _       (dc/listen! conn #(reset! report %))
        datoms' #{(dc/datom 1 :age 20)
                  (dc/datom 1 :sex :male)}
        db'     (dc/init-db datoms' user-schema')]
    (dc/reset-conn! conn db' :meta)
    (is (= datoms' (set (dc/datoms @conn :eavt))))
    (is (= schema' (:schema @conn)))

    (let [{:keys [db-before db-after tx-data tx-meta]} @report]
      (is (= datoms  (set (dc/datoms db-before :eavt))))
      (is (= schema  (:schema db-before)))
      (is (= datoms' (set (dc/datoms db-after :eavt))))
      (is (= schema' (:schema db-after)))
      (is (= :meta   tx-meta))
      (is (= [[1 :age  17     false]
              [1 :name "Ivan" false]
              [1 :age  20     true]
              [1 :sex  :male  true]]
             (map (juxt :e :a :v :added) tx-data))))))
