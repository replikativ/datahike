(ns datahike.test.reference-test
  "Cross-database references (datahike.reference): URI round-trips,
   reified-reference conversion, and resolution semantics — strict by
   default (selector attr must be :db/unique), explicit opt-in for
   non-unique selectors, as-of record references, dangling references.
   Runs on CLJ and CLJS via the deftest-async pattern."
  (:require
   #?(:clj  [clojure.test :as t :refer [deftest is testing]]
      :cljs [cljs.test :as t :refer-macros [deftest is testing]])
   [clojure.core.async :as a :refer [<!]]
   [datahike.api :as d]
   [datahike.reference :as ref]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

(defn- rand-uuid []
  #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

;; ============================================================================
;; Pure round-trips — no database needed
;; ============================================================================

(deftest uri-round-trips
  (let [db-id (rand-uuid)
        u     (rand-uuid)]
    (testing "living reference, uuid selector (untagged value form)"
      (let [r (ref/reference db-id [:entity/uuid u])]
        (is (= r (ref/parse (ref/render r))))))
    (testing "record reference, string selector, tx temporal"
      (let [r (ref/reference db-id [:page/title "Road map / Q3"] {:tx 536871113})]
        (is (= r (ref/parse (ref/render r))))))
    (testing "branch temporal"
      (let [r (ref/reference db-id [:entity/uuid u] {:branch "exploration 2"})]
        (is (= r (ref/parse (ref/render r))))))
    (testing "long and keyword selector values"
      (let [rl (ref/reference db-id [:item/code 42])
            rk (ref/reference db-id [:item/kind :alpha/beta])]
        (is (= rl (ref/parse (ref/render rl))))
        (is (= rk (ref/parse (ref/render rk))))))
    (testing "malformed URIs throw"
      (is (thrown? #?(:clj Exception :cljs js/Error) (ref/parse "dh://nope")))
      (is (thrown? #?(:clj Exception :cljs js/Error) (ref/parse "http://x/y/z"))))))

(deftest reified-reference-round-trip
  (let [r (ref/reference (rand-uuid) [:page/title "Roadmap"] {:tx 42})
        m (ref/reference->tx-map r :derived-from)]
    (is (= :derived-from (:dh.ref/type m)))
    (is (= r (ref/tx-map->reference m)))))

;; ============================================================================
;; Resolution — strict / opt-in / fiber / as-of / dangling
;; ============================================================================

(def ^:private schema
  [{:db/ident :thing/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :thing/title             ; deliberately NOT unique
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn- cfg []
  {:store {:backend :memory :id (rand-uuid)}
   :keep-history? true
   :schema-flexibility :write})

(defn- setup [cfg]
  (a/go
    #?(:clj  (do (d/create-database cfg)
                 (d/connect cfg))
       :cljs (do (<! (d/create-database cfg))
                 (<! (d/connect cfg {:sync? false}))))))

(defn- teardown [conn cfg]
  (a/go
    #?(:clj  (do (d/release conn)
                 (d/delete-database cfg))
       :cljs (do (d/release conn)
                 (<! (d/delete-database cfg))))))

(deftest-async resolution-semantics
  (let [c     (cfg)
        db-id (get-in c [:store :id])
        conn  (<! (setup c))
        _     (<! (d/transact! conn schema))
        tid   (rand-uuid)
        _     (<! (d/transact! conn [{:thing/id tid :thing/title "One"}
                                     {:thing/id (rand-uuid) :thing/title "Copy"}
                                     {:thing/id (rand-uuid) :thing/title "Copy"}]))
        connect-fn (fn [id _] (when (= id db-id) conn))]

    (testing "strict resolution on a unique selector (AVET fast path)"
      (let [r (ref/reference db-id [:thing/id tid])]
        (is (some? (:eid (ref/resolve-reference r connect-fn))))))

    (testing "strict resolution on a NON-unique selector throws with a hint"
      (let [r (ref/reference db-id [:thing/title "One"])]
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (ref/resolve-reference r connect-fn)))))

    (testing "explicit opt-in resolves non-unique selectors (first match)"
      (let [r (ref/reference db-id [:thing/title "One"])]
        (is (some? (:eid (ref/resolve-reference r connect-fn {:ambiguous :first}))))))

    (testing "resolve-all returns the whole fiber"
      (let [r (ref/reference db-id [:thing/title "Copy"])]
        (is (= 2 (count (:eids (ref/resolve-all r connect-fn)))))))

    (testing "record reference: as-of a tx before the entity existed → dangling"
      (let [tx0 (:max-tx @conn)          ; head BEFORE "Late" exists
            _   (<! (d/transact! conn [{:thing/id (rand-uuid)
                                        :thing/title "Late"}]))
            late-uuid (d/q '[:find ?u . :where [?e :thing/title "Late"] [?e :thing/id ?u]] @conn)
            live   (ref/reference db-id [:thing/id late-uuid])
            record (ref/reference db-id [:thing/id late-uuid] {:tx tx0})]
        (is (some? (:eid (ref/resolve-reference live connect-fn)))
            "live reference resolves at head")
        (is (nil? (:eid (ref/resolve-reference record connect-fn)))
            "record reference pinned before creation is dangling")))

    (testing "unknown / ungranted database → nil (dangling, not an error)"
      (let [r (ref/reference (rand-uuid) [:thing/id tid])]
        (is (nil? (ref/resolve-reference r connect-fn)))))

    (<! (teardown conn c))))
