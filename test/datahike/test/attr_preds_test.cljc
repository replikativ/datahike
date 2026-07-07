(ns datahike.test.attr-preds-test
  "Attribute-value constraints: :db/maxLength (declarative string bound) and
   :db.attr/preds (symbol-referenced value predicates)."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.string :as str]
   [datahike.attr-preds :as ap]
   #?(:clj [datahike.api :as d])))

;; ---------------------------------------------------------------------------
;; Registry / resolution — pure, cross-platform
;; ---------------------------------------------------------------------------

(deftest resolve-pred-sources
  (ap/register-attr-pred! 'attr-preds-test/kw-reg (fn [s] (= s "ok")))
  (testing "registered symbol resolves to its fn"
    (is (fn? (ap/resolve-pred 'attr-preds-test/kw-reg)))
    (is (true? ((ap/resolve-pred 'attr-preds-test/kw-reg) "ok"))))
  (testing "a bare fn resolves to itself"
    (let [f (fn [_] true)] (is (identical? f (ap/resolve-pred f)))))
  (testing "unregistered / unresolvable symbol -> nil"
    (is (nil? (ap/resolve-pred 'attr-preds-test/never-registered-xyz))))
  (ap/unregister-attr-pred! 'attr-preds-test/kw-reg)
  (is (nil? (ap/resolve-pred 'attr-preds-test/kw-reg))))

;; ---------------------------------------------------------------------------
;; Enforcement — clj (synchronous transact)
;; ---------------------------------------------------------------------------

#?(:clj
   (do
     (defn- tx-error
       "Root :error keyword of a (possibly writer-wrapped) transact failure,
        or :ok if the thunk succeeds."
       [f]
       (try (f) :ok
            (catch Throwable e
              (loop [x e]
                (let [err (:error (ex-data x))
                      c   (.getCause x)]
                  (cond
                    err  err
                    c    (recur c)
                    :else :no-error-key))))))

     (defn- with-conn* [flex f]
       (let [cfg {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility flex :keep-history? true}]
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (try (f conn) (finally (d/release conn) (d/delete-database cfg))))))

     (defn- decl [flex m]
       (if (= flex :write)
         (merge {:db/valueType :db.type/string :db/cardinality :db.cardinality/one} m)
         m))

     ;; a var predicate resolved by fully-qualified symbol (clj only)
     (defn upper-only? [s] (boolean (re-matches #"[A-Z]+" s)))

     (deftest maxlength-both-flexibilities
       (doseq [flex [:write :read]]
         (with-conn* flex
           (fn [conn]
             (d/transact conn [(decl flex {:db/ident :nm :db/maxLength 5})])
             (testing (str flex " within bound passes")
               (is (= :ok (tx-error #(d/transact conn [{:nm "abcde"}])))))
             (testing (str flex " over bound raises :transact/attr-pred")
               (is (= :transact/attr-pred (tx-error #(d/transact conn [{:nm "abcdef"}])))))))))

     (deftest maxlength-skips-non-string
       ;; :read lets a non-string value through the type gate; maxLength must
       ;; only apply to strings (never throw a spurious count error).
       (with-conn* :read
         (fn [conn]
           (d/transact conn [{:db/ident :nm :db/maxLength 5}])
           (is (= :ok (tx-error #(d/transact conn [{:nm 123456789}])))))))

     (deftest preds-registry-and-var
       (ap/register-attr-pred! 'attr-preds-test/nonblank
                               (fn [s] (boolean (seq (str/trim s)))))
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :code :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db.attr/preds ['attr-preds-test/nonblank
                                              'datahike.test.attr-preds-test/upper-only?]}])
           (testing "passes both preds"
             (is (= :ok (tx-error #(d/transact conn [{:code "ABC"}])))))
           (testing "fails the registry pred"
             (is (= :transact/attr-pred (tx-error #(d/transact conn [{:code "   "}])))))
           (testing "fails the var pred"
             (is (= :transact/attr-pred (tx-error #(d/transact conn [{:code "abc"}]))))))))

     (deftest single-pred-stored-as-vector
       ;; Regression: a lone :db.attr/preds must accumulate to a vector
       ;; regardless of datom order within the schema tx (a bare value broke
       ;; enforcement's doseq — surfaced first on cljs, where map order differs).
       (ap/register-attr-pred! 'attr-preds-test/nb1
                               (fn [s] (boolean (seq (str/trim s)))))
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :code :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db.attr/preds ['attr-preds-test/nb1]}])
           (is (vector? (get-in (:schema @conn) [:code :db.attr/preds])))
           (is (= :ok (tx-error #(d/transact conn [{:code "x"}]))))
           (is (= :transact/attr-pred (tx-error #(d/transact conn [{:code "  "}])))))))

     (deftest preds-unresolved
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :x :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db.attr/preds ['attr-preds-test/definitely-not-registered]}])
           (is (= :transact/attr-pred-unresolved
                  (tx-error #(d/transact conn [{:x "v"}])))))))

     (deftest retract-not-blocked
       ;; A constraint must never block retracting an existing value.
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :nm :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one :db/maxLength 5}])
           (d/transact conn [{:db/id 100 :nm "ok"}])
           (let [eid (ffirst (d/q '[:find ?e :where [?e :nm "ok"]] @conn))]
             (is (= :ok (tx-error #(d/transact conn [[:db/retract eid :nm "ok"]]))))))))

     (deftest post-hoc-add-constraint
       ;; :db/maxLength / :db.attr/preds can be added to an already-defined attr.
       (ap/register-attr-pred! 'attr-preds-test/nonblank2
                               (fn [s] (boolean (seq (str/trim s)))))
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :nm :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}])
           (is (= :ok (tx-error #(d/transact conn [{:nm "anything-long-ok"}]))))
           (d/transact conn [{:db/ident :nm :db/maxLength 5
                              :db.attr/preds ['attr-preds-test/nonblank2]}])
           (is (= :transact/attr-pred (tx-error #(d/transact conn [{:nm "toolong"}]))))
           (is (= :ok (tx-error #(d/transact conn [{:nm "ok"}])))))))

     (deftest unconstrained-attr-unaffected
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :free :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}])
           (is (= :ok (tx-error #(d/transact conn [{:free (apply str (repeat 10000 \x))}])))))))))
