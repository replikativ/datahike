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
    (testing "commit temporal — exact content-addressed pin"
      (let [c (rand-uuid)
            r (ref/reference db-id [:entity/uuid u] {:commit c})]
        (is (= r (ref/parse (ref/render r))))
        (is (= (str "dh://" db-id "/entity/uuid/" u "?commit=" c)
               (ref/render r)))))
    (testing "temporal serializes as a URL query string (not @-syntax)"
      (let [r (ref/reference db-id [:entity/uuid u] {:tx 536871113 :branch "exp"})
            s (ref/render r)]
        (is (re-find #"\?tx=536871113&branch=exp$" s))
        (is (nil? (re-find #"@" s)))))
    (testing "unknown temporal qualifier throws"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (ref/parse (str "dh://" db-id "/entity/uuid/" u "?bogus=1")))))
    (testing "namespaced attr keeps its slash readable (not %2F)"
      (let [r (ref/reference db-id [:S.Page/title "Roadmap / Q3"])]
        (is (= (str "dh://" db-id "/S.Page/title/str:Roadmap%20%2F%20Q3") (ref/render r)))
        (is (= r (ref/parse (ref/render r))))))
    (testing "non-namespaced attr and a value that contains slashes"
      (let [r (ref/reference db-id [:title "a/b/c"])]
        (is (= (str "dh://" db-id "/title/str:a%2Fb%2Fc") (ref/render r)))
        (is (= r (ref/parse (ref/render r))))))
    (testing "long and keyword selector values"
      (let [rl (ref/reference db-id [:item/code 42])
            rk (ref/reference db-id [:item/kind :alpha/beta])]
        (is (= rl (ref/parse (ref/render rl))))
        (is (= rk (ref/parse (ref/render rk))))))
    (testing "valid-time temporal, alone and combined with tx (bitemporal pin)"
      (let [rv (ref/reference db-id [:entity/uuid u] {:valid #inst "2026-06-01T00:00:00.000-00:00"})
            rb (ref/reference db-id [:entity/uuid u] {:tx 536871113
                                                      :valid #inst "2026-06-01T00:00:00.000-00:00"})]
        (is (= rv (ref/parse (ref/render rv))))
        (is (= rb (ref/parse (ref/render rb))))))
    (testing "bare entity-id selector — single numeric segment"
      (let [re (ref/reference db-id 387)
            rt (ref/reference db-id 387 {:tx 536871113})]
        (is (= [:db/id 387] (ref/lookup-ref re)))
        (is (= (str "dh://" db-id "/387") (ref/render re)))
        (is (= re (ref/parse (ref/render re))))
        (is (= rt (ref/parse (ref/render rt))))))
    (testing "malformed URIs throw"
      (is (thrown? #?(:clj Exception :cljs js/Error) (ref/parse "dh://nope")))
      (is (thrown? #?(:clj Exception :cljs js/Error) (ref/parse "http://x/y/z"))))))

(deftest value-type-codec
  (testing "every datahike value type survives the URL value codec"
    (let [db-id (rand-uuid)
          rt    (fn [v] (ref/parse (ref/render (ref/reference db-id [:some/attr v]))))
          ;; full-record equality also asserts type fidelity: (= (float 1.5) 1.5)
          ;; and (= 1.5M 1.50M) are both false, so a widened float or a dropped
          ;; bigdec scale would fail this, not silently pass.
          same? (fn [v] (= (ref/reference db-id [:some/attr v]) (rt v)))]
      (is (same? true))                                    ; boolean → bool:
      (is (same? "road / map?x=1&y"))                      ; string (URL-special chars)
      (is (same? 42))                                      ; long → long:
      (is (same? :alpha/beta))                             ; keyword → kw:
      (is (same? (rand-uuid)))                             ; uuid → untagged
      (is (same? 3.14159))                                 ; double → edn:
      (is (same? 'my.ns/sym))                              ; symbol → edn:
      (is (same? [1 "a" :k]))                              ; tuple → edn:
      (is (same? #inst "2026-01-01T12:30:00.000-00:00"))   ; instant → inst:
      (testing "bytes → b64: (base64url; java.util.Base64 / goog.crypt.base64)"
        (let [b   #?(:clj (byte-array [1 2 3 4]) :cljs (js/Uint8Array. #js [1 2 3 4]))
              out (:value (rt b))]
          (is #?(:clj  (java.util.Arrays/equals ^bytes b ^bytes out)
                 :cljs (= (vec (js/Array.from b)) (vec (js/Array.from out)))))))
      #?(:clj
         (testing "clj type fidelity: float≠double, bigdec scale, bigint"
           (is (same? (float 1.5)))                        ; flt: keeps Float
           (is (same? 1.50M))                              ; edn: keeps bigdec scale
           (is (same? 123456789012345678901234567890N)))))))  ; bigint

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

    ;; `:commit` resolution loads a different db value via `commit-as-db`,
    ;; which is synchronous only on CLJ (async on CLJS). resolve-reference is a
    ;; sync API, so commit-pinned resolution is CLJ-only.
    #?(:clj
       (testing "commit reference: exact content-addressed pin resolves via commit-as-db"
         (let [cid (d/commit-id @conn)
               r   (ref/reference db-id [:thing/id tid] {:commit cid})]
           (is (some? cid) "head db carries a commit-id")
           (is (= (:eid (ref/resolve-reference (ref/reference db-id [:thing/id tid]) connect-fn))
                  (:eid (ref/resolve-reference r connect-fn)))
               "commit pin resolves the same entity as the live head"))))

    #?(:clj
       (testing "commit reference to an unknown commit → dangling"
         (let [r (ref/reference db-id [:thing/id tid] {:commit (rand-uuid)})]
           (is (nil? (ref/resolve-reference r connect-fn))))))

    (testing "unknown / ungranted database → nil (dangling, not an error)"
      (let [r (ref/reference (rand-uuid) [:thing/id tid])]
        (is (nil? (ref/resolve-reference r connect-fn)))))

    (testing "bare eid pointer: strict, no AVET, no unique attr required"
      (let [eid (d/q '[:find ?e . :in $ ?u :where [?e :thing/id ?u]] @conn tid)
            live (ref/reference db-id eid)]
        (is (= eid (:eid (ref/resolve-reference live connect-fn)))
            "existing eid resolves strictly (unique by construction)")
        (is (nil? (ref/resolve-reference (ref/reference db-id 999999) connect-fn))
            "never-allocated eid dangles")
        (is (nil? (:eid (ref/resolve-reference
                         (ref/reference db-id eid {:tx (dec 536870913)}) connect-fn)))
            "eid pinned before its creation dangles")))

    (<! (teardown conn c))))

(deftest-async valid-time-references
  ;; Bitemporal record references: an entity whose vt-window starts
  ;; 2026-06-01 resolves for {:valid ≥ start}, dangles for {:valid < start}.
  (let [c     (cfg)
        db-id (get-in c [:store :id])
        conn  (<! (setup c))
        _     (<! (d/transact! conn schema))
        vid   (rand-uuid)
        _     (<! (d/transact! conn {:tx-data [{:thing/id vid :thing/title "Windowed"}]
                                     :tx-meta {:db.valid/from #inst "2026-06-01T00:00:00.000-00:00"}}))
        connect-fn (fn [id _] (when (= id db-id) conn))
        inside  (ref/reference db-id [:thing/id vid] {:valid #inst "2026-07-01T00:00:00.000-00:00"})
        before  (ref/reference db-id [:thing/id vid] {:valid #inst "2025-01-01T00:00:00.000-00:00"})]
    (is (some? (:eid (ref/resolve-reference inside connect-fn)))
        "valid-time point inside the window resolves")
    (is (nil? (:eid (ref/resolve-reference before connect-fn)))
        "valid-time point before the window dangles")
    (<! (teardown conn c))))

(deftest-async system-schema-graduation
  ;; :dh.ref/* are system-installed: a schema-on-write database accepts
  ;; reified references WITHOUT any user schema declaration, and they are
  ;; datalog-queryable by predicate.
  (let [c    (cfg)
        conn (<! (setup c))
        r    (ref/reference (rand-uuid) [:entity/uuid (rand-uuid)] {:tx 42})
        _    (<! (d/transact! conn [(ref/reference->tx-map r :derived-from)]))
        types (d/q '[:find [?t ...] :where [?e :dh.ref/type ?t]] @conn)]
    (is (= [:derived-from] types)
        "reified reference transacted with no user schema, queryable by type")
    (is (= r (let [eid (d/q '[:find ?e . :where [?e :dh.ref/type :derived-from]] @conn)]
               (ref/tx-map->reference (d/pull @conn '[*] eid))))
        "pull round-trips back to the original Reference")
    (<! (teardown conn c))))
