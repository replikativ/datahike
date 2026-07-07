(ns datahike.test.attr-preds-test
  "Attribute-value constraints: :db/maxLength (declarative string bound),
   :db.attr/preds (symbol-referenced value predicates), and the default
   value-size resource model. Enforcement tests are `deftest-async` so they run
   on both the JVM and Node from one source; a few genuinely platform-specific
   cases (var-symbol predicate resolution, JVM byte arrays, `with-redefs`) stay
   clj-only."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.core.async :as async :refer [<!]]
   [clojure.string :as str]
   [datahike.api :as d]
   [datahike.attr-preds :as ap]
   [superv.async :as sa]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]
   #?(:clj [datahike.config :as dc])))

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
;; Portable enforcement helpers
;; ---------------------------------------------------------------------------

(defn- tx-err
  "Classify the result of `(<! (d/transact! …))`: `:ok` on success, else the
   failure's `:error` keyword. `d/transact!` delivers a failure as an error
   *value* on the channel (superv.async), so we inspect it rather than
   `<?-`-rethrow — a `<?-` inside a `try` isn't transformable by the CLJS `go`
   state machine, whereas taking the value with `<!` in the body is."
  [r]
  (if-not (instance? #?(:clj Throwable :cljs js/Error) r)
    :ok
    (loop [x r]
      (let [e (:error (ex-data x))
            c #?(:clj (.getCause x) :cljs (.-cause x))]
        (cond e e
              (some? c) (recur c)
              :else :no-error)))))

(defn- fresh-conn
  "Channel yielding a fresh in-memory keep-history? connection."
  [flex]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility flex :keep-history? true}]
    (async/go
      #?(:clj  (do (d/create-database cfg) (d/connect cfg))
         :cljs (do (<! (d/create-database cfg)) (<! (d/connect cfg {:sync? false})))))))

(defn- decl
  "Under :write, complete a schema entity with string type + cardinality; under
   :read schema is optional so pass it through."
  [flex m]
  (if (= flex :write)
    (merge {:db/valueType :db.type/string :db/cardinality :db.cardinality/one} m)
    m))

(defn- rep [n c] (apply str (repeat n c)))

;; ---------------------------------------------------------------------------
;; Enforcement — portable (JVM + Node). `<?-` (bare, in the go body) rethrows a
;; setup failure → the test fails loudly; assertions inspect the taken value.
;; ---------------------------------------------------------------------------

(deftest-async explicit-maxlength-both-flexibilities
  (let [wconn (<! (fresh-conn :write))
        rconn (<! (fresh-conn :read))]
    (sa/<?- (d/transact! wconn [(decl :write {:db/ident :nm :db/maxLength 5})]))
    (sa/<?- (d/transact! rconn [(decl :read {:db/ident :nm :db/maxLength 5})]))
    (testing "within bound passes (both flexibilities)"
      (is (= :ok (tx-err (<! (d/transact! wconn [{:nm (rep 5 \a)}])))))
      (is (= :ok (tx-err (<! (d/transact! rconn [{:nm (rep 5 \a)}]))))))
    (testing "over bound raises :transact/max-length (both flexibilities)"
      (is (= :transact/max-length (tx-err (<! (d/transact! wconn [{:nm (rep 6 \a)}])))))
      (is (= :transact/max-length (tx-err (<! (d/transact! rconn [{:nm (rep 6 \a)}]))))))))

(deftest-async maxlength-skips-non-string
  ;; :read lets a non-string value through the type gate; maxLength must only
  ;; apply to strings (never throw a spurious count error).
  (let [conn (<! (fresh-conn :read))]
    (sa/<?- (d/transact! conn [{:db/ident :nm :db/maxLength 5}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:nm 123456789}])))))))

(deftest-async preds-registry
  (ap/register-attr-pred! 'attr-preds-test/nonblank (fn [s] (boolean (seq (str/trim s)))))
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :code :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one
                                :db.attr/preds ['attr-preds-test/nonblank]}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:code "ABC"}])))) "passes the registry pred")
    (is (= :transact/attr-pred (tx-err (<! (d/transact! conn [{:code "   "}])))) "fails the registry pred")))

(deftest-async single-pred-stored-as-vector
  ;; Regression: a lone :db.attr/preds must accumulate to a vector regardless of
  ;; datom order within the schema tx (a bare value broke enforcement's doseq —
  ;; surfaced first on cljs, where map order differs).
  (ap/register-attr-pred! 'attr-preds-test/nb1 (fn [s] (boolean (seq (str/trim s)))))
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :code :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one
                                :db.attr/preds ['attr-preds-test/nb1]}]))
    (is (vector? (get-in (:schema @conn) [:code :db.attr/preds])))
    (is (= :ok (tx-err (<! (d/transact! conn [{:code "x"}])))))
    (is (= :transact/attr-pred (tx-err (<! (d/transact! conn [{:code "  "}])))))))

(deftest-async preds-unresolved
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :x :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one
                                :db.attr/preds ['attr-preds-test/definitely-not-registered]}]))
    (is (= :transact/attr-pred-unresolved (tx-err (<! (d/transact! conn [{:x "v"}])))))))

(deftest-async retract-not-blocked
  ;; A constraint must never block retracting an existing value.
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :nm :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/maxLength 5}]))
    (sa/<?- (d/transact! conn [{:db/id 100 :nm "ok"}]))
    (let [eid (ffirst (d/q '[:find ?e :where [?e :nm "ok"]] @conn))]
      (is (= :ok (tx-err (<! (d/transact! conn [[:db/retract eid :nm "ok"]]))))))))

(deftest-async post-hoc-add-constraint
  ;; :db/maxLength / :db.attr/preds can be added to an already-defined attr.
  (ap/register-attr-pred! 'attr-preds-test/nonblank2 (fn [s] (boolean (seq (str/trim s)))))
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :nm :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:nm "anything-long-ok"}])))))
    (sa/<?- (d/transact! conn [{:db/ident :nm :db/maxLength 5
                                :db.attr/preds ['attr-preds-test/nonblank2]}]))
    (is (= :transact/max-length (tx-err (<! (d/transact! conn [{:nm "toolong"}])))))
    (is (= :ok (tx-err (<! (d/transact! conn [{:nm "ok"}])))))))

(deftest-async unconstrained-attr-unaffected
  ;; A non-string/bytes attribute is never touched by the value-size model.
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :n :db/valueType :db.type/long
                                :db/cardinality :db.cardinality/one}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:n 123456789}])))))))

(deftest-async default-string-cap-write-only
  ;; New :write DBs get the 4096 default; :read (no declared types) does not.
  (let [wconn (<! (fresh-conn :write))
        rconn (<! (fresh-conn :read))]
    (sa/<?- (d/transact! wconn [{:db/ident :s :db/valueType :db.type/string
                                 :db/cardinality :db.cardinality/one}]))
    (is (= :transact/max-length (tx-err (<! (d/transact! wconn [{:s (rep 5000 \x)}])))))
    (is (= :ok (tx-err (<! (d/transact! wconn [{:s (rep 4096 \x)}])))))
    (is (= :ok (tx-err (<! (d/transact! rconn [{:s (rep 5000 \x)}])))) ":read has no default")))

(deftest-async explicit-maxlength-overrides-default
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :bigcap :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/maxLength 8192}
                               {:db/ident :nocap :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one :db/maxLength 0}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:bigcap (rep 5000 \x)}])))) "raised above default")
    (is (= :ok (tx-err (<! (d/transact! conn [{:nocap (rep 9000 \x)}])))) "disabled")))

(deftest-async tuple-string-slot-cap
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :tup :db/valueType :db.type/tuple
                                :db/tupleTypes [:db.type/string :db.type/long]
                                :db/cardinality :db.cardinality/one}]))
    (is (= :transact/max-length (tx-err (<! (d/transact! conn [{:tup [(rep 300 \x) 1]}])))))
    (is (= :ok (tx-err (<! (d/transact! conn [{:tup [(rep 256 \x) 1]}])))))))

(deftest-async system-string-attr-exempt-from-default
  ;; :db/doc is a system string attribute — the default must not cap it.
  (let [conn (<! (fresh-conn :write))]
    (sa/<?- (d/transact! conn [{:db/ident :s :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}]))
    (is (= :ok (tx-err (<! (d/transact! conn [{:db/ident :s :db/doc (rep 9000 \x)}])))))))

;; ---------------------------------------------------------------------------
;; Enforcement — clj-only (platform-specific mechanisms)
;; ---------------------------------------------------------------------------

#?(:clj
   (do
     (defn- tx-error [f]
       (try (f) :ok
            (catch Throwable e
              (loop [x e]
                (let [err (:error (ex-data x)) c (.getCause x)]
                  (cond err err c (recur c) :else :no-error-key))))))

     (defn- with-conn* [flex f]
       (let [cfg {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility flex :keep-history? true}]
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (try (f conn) (finally (d/release conn) (d/delete-database cfg))))))

     ;; a var predicate resolved by fully-qualified symbol — clj-only, since
     ;; cljs has no `requiring-resolve`.
     (defn upper-only? [s] (boolean (re-matches #"[A-Z]+" s)))

     (deftest preds-var-symbol
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :code :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db.attr/preds ['datahike.test.attr-preds-test/upper-only?]}])
           (is (= :ok (tx-error #(d/transact conn [{:code "ABC"}]))))
           (is (= :transact/attr-pred (tx-error #(d/transact conn [{:code "abc"}])))))))

     (deftest default-bytes-cap
       (with-conn* :write
         (fn [conn]
           (d/transact conn [{:db/ident :b :db/valueType :db.type/bytes
                              :db/cardinality :db.cardinality/one}])
           (is (= :transact/max-length (tx-error #(d/transact conn [{:b (byte-array 5000)}]))))
           (is (= :ok (tx-error #(d/transact conn [{:b (byte-array 4096)}])))))))

     (deftest existing-db-unbounded
       ;; A DB created without the caps (predating the feature) stays unbounded.
       (let [cfg {:store {:backend :memory :id (random-uuid)}
                  :schema-flexibility :write :keep-history? false}]
         (with-redefs [dc/apply-default-value-caps identity]
           (d/create-database cfg))
         (let [conn (d/connect cfg)]
           (try
             (d/transact conn [{:db/ident :s :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}])
             (is (= :ok (tx-error #(d/transact conn [{:s (apply str (repeat 9000 \x))}]))))
             (finally (d/release conn) (d/delete-database cfg))))))))
