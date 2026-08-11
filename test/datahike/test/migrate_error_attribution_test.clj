(ns datahike.test.migrate-error-attribution-test
  "An import must not blame the data for the store.

   `:on-error :collect`'s contract is to survive a bad RECORD and name it. It
   was applied to every failure, and because the label was a FALLBACK
   (`(or (:error (ex-data ex)) :import/corrupt-datom)`) rather than a judgement,
   everything came back as a corrupt datom.

   Two independent bugs made that unavoidable:

   1. **The error key never survived the writer boundary.** A failure reaching
      the importer is wrapped twice — `throwable-promise`'s deref calls `.get`
      on a CompletableFuture, raising `ExecutionException` (ex-data nil), and
      `superv.async/throw-if-exception-` wraps THAT, reading its ex-data. So
      `(ex-data ex)` was empty even for a `:transact/unique` conflict that said
      precisely what was wrong, and the fallback was always what got recorded.
   2. **Nothing asked whether a record could be responsible.** Measured: an
      `IOException` out of `writing/commit!` killed the writer, the narrowing
      retry then attempted every remaining datom individually against a dead
      one, and all 74 were filed as `:import/corrupt-datom` — the first of them
      a `:db/txInstant` the EXPORTER wrote. `import-db` returned normally with
      10 of 84 datoms restored; the only signal was `:verified? false`.

   So: `datahike.tools/ex-error` looks through the wrappers, and a failure
   outside the record-fault namespaces aborts instead of being filed against
   datoms that did nothing wrong."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.tools :as dt]
            [datahike.writing :as w]
            [datahike.writer :as dwriter]
            [datahike.migrate.store :as mstore]
            [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- dump-of!
  "Export `conn` to a fresh in-memory store; returns the target."
  [conn]
  (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                               {:sync? true})
        target {:store store :prefix "attrib"}]
    (m/export-db @conn target {:history? true :sort? false :chunk-size 200})
    target))

(defn- plain-db []
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (doseq [i (range 40)] (d/transact conn [{:n i}]))
    conn))

(defn- unique-db []
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn [{:db/ident :email :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}])
    (doseq [i (range 5)] (d/transact conn [{:email (str "u" i "@x")}]))
    conn))

(defn- import-catching [conn target opts]
  (try {:report (m/import-db conn target opts)}
       (catch Exception e {:error (:error (ex-data e))
                           :data (ex-data e)
                           :chain (mapv #(.getMessage ^Throwable %) (dt/cause-chain e))})))

;; ---------------------------------------------------------------------------

(deftest the-error-key-survives-the-writer-boundary
  (testing "`dt/ex-error` looks through the two wrappers a writer failure picks
            up. Without it `(:error (ex-data e))` is nil and the caller has to
            guess — which is what produced the fallback label every time."
    (let [inner (ex-info "unique constraint" {:error :transact/unique :attribute :email})
          wrapped (ex-info "boom" {} (java.util.concurrent.ExecutionException. inner))]
      (is (nil? (:error (ex-data wrapped))) "the naive read finds nothing")
      (is (= :transact/unique (dt/ex-error wrapped)) "this one finds it")
      (is (= 3 (count (dt/cause-chain wrapped)))))
    (testing "and falls back to :type, which is how the writer marks itself dead"
      (is (= :writer-shut-down
             (dt/ex-error (ex-info "outer" {} (ex-info "w" {:type :writer-shut-down}))))))
    (testing "nil when nothing in the chain says anything"
      (is (nil? (dt/ex-error (java.io.IOException. "connection reset")))))))

(deftest a-store-failure-is-not-a-corrupt-datom
  (testing "THE case. An IOException inside commit! is not the dump's fault, and
            filing it against 74 datoms sends an operator to look at a backup
            that is perfectly intact."
    (doseq [mode [:collect :abort]]
      (let [src    (plain-db)
            target (dump-of! src)
            orig   w/commit!
            n      (atom 0)
            applies (atom 0)
            orig-load dwriter/load-entities]
        (with-redefs [w/commit! (fn [& a]
                                  (if (> (swap! n inc) 1)
                                    (throw (java.io.IOException. "connection reset by peer"))
                                    (apply orig a)))
                      dwriter/load-entities (fn [& a] (swap! applies inc) (apply orig-load a))]
          (let [tgt (utils/setup-db (mem-cfg))
                r   (import-catching tgt target {:on-error mode :batch-size 10})]
            (is (= :import/apply-failed (:error r))
                (str "the import aborts and names the failure, :on-error " mode))
            (is (nil? (:report r)) "rather than returning a report")
            (is (some #(re-find #"connection reset" (or % "")) (:chain r))
                "with the original IOException still reachable in the cause chain")
            (testing "and does not retry every datom against a dead writer"
              ;; 4 batches of ~10 + narrowing would be ~74 extra single-datom
              ;; applies; aborting at the failing batch keeps it to a handful.
              (is (< @applies 15)
                  (str "load-entities called " @applies " times")))
            (teardown tgt)))
        (teardown src)))))

(deftest a-real-record-fault-is-still-collected-and-named
  (testing "the whole point of `:collect` still works — and now reports what
            datahike actually said, `:transact/unique`, instead of the generic
            fallback that used to swallow it"
    (let [src    (unique-db)
          target (dump-of! src)
          ;; force every entity onto one unique value: legitimately the records'
          ;; fault, and exactly what `:collect` exists for
          clash  (map (fn [r] (if (= :email (nth r 1)) (assoc (vec r) 2 "same@x") r)))
          tgt    (utils/setup-db (mem-cfg))
          rep    (m/import-db tgt target {:on-error :collect :xform clash :verify? false})]
      (is (pos? (count (:errors rep))) "the offending records are collected")
      (is (= #{:transact/unique} (set (map :error (:errors rep))))
          "under datahike's own error key, not :import/corrupt-datom")
      (is (every? :datom (:errors rep)) "each naming the record it blames")
      (is (pos? (count (d/datoms @tgt :eavt))) "and the rest still imported")
      (teardown tgt)
      (teardown src))))

(deftest a-record-fault-under-abort-still-says-corrupt-datom
  (testing "the abort path keeps its established label for genuine record
            faults — the change is that it stops applying it to everything else"
    (let [src    (unique-db)
          target (dump-of! src)
          clash  (map (fn [r] (if (= :email (nth r 1)) (assoc (vec r) 2 "same@x") r)))
          tgt    (utils/setup-db (mem-cfg))
          r      (import-catching tgt target {:on-error :abort :xform clash :verify? false})]
      (is (= :import/corrupt-datom (:error r)))
      (is (= :transact/unique (:cause-error (:data r)))
          "and now also carries what datahike said, which used to be lost")
      (teardown tgt)
      (teardown src))))

;; ---------------------------------------------------------------------------
;; A-F2 — the verification result must say WHICH of its outcomes happened
;;
;; `:verified?` is true / false / nil, and `nil` covered three unrelated
;; situations: verification switched off, nothing to compare against, and
;; `:collect` swallowing the result. In a report otherwise full of successes all
;; three read as "fine". Worse, the two import paths DISAGREED on one of them:
;; given a source declaring no record count, the streaming path threw
;; `:import/verify-failed` ("datom count mismatch" — comparing the live count
;; against `(- 0 dropped)`, a statement about nothing) while the index-build
;; path returned **`:verified? true`**. Both measured.

(defn- counted-db []
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (doseq [i (range 10)] (d/transact conn [{:n i}]))
    conn))

(defn- strip-source-count!
  "Remove the manifest's declared record count, leaving the dump otherwise
   intact — the shape a non-dump record source has by nature."
  [target]
  (let [med (mstore/open target {:sync? true})
        man (mstore/read-manifest med)]
    (k/assoc (:store med) (#'mstore/ckey (:prefix med) "manifest")
             (-> man
                 (update :semantic-digest dissoc :count)
                 ;; so the Phase-2 completeness gate, a different check on a
                 ;; different key, is not what fires
                 (assoc-in [:stats :transformed?] true))
             {:sync? true})
    target))

(deftest verification-says-which-outcome-it-was
  (testing "four distinguishable states, none of which reads as another"
    (let [src    (counted-db)
          target (dump-of! src)]
      (testing ":ok — checked and it matched"
        (let [tgt (utils/setup-db (mem-cfg))
              r   (m/import-db tgt target {})]
          (is (true? (:verified? r)))
          (is (= :ok (get-in r [:verification :status])))
          (is (= (:expected (:verification r)) (:actual (:verification r))))
          (teardown tgt)))
      (testing ":skipped — the caller switched it off, so nothing is claimed"
        (let [tgt (utils/setup-db (mem-cfg))
              r   (m/import-db tgt target {:verify? false})]
          (is (nil? (:verified? r)))
          (is (= :skipped (get-in r [:verification :status]))
              "which used to be indistinguishable from 'checked, inconclusive'")
          (teardown tgt)))
      (teardown src))))

(deftest a-source-with-no-declared-count-is-refused-on-both-paths
  (testing "the divergence. Same input, and the index-build path answered
            `:verified? true` while the streaming path threw a mismatch it had
            invented. Neither was right: nothing is known to be wrong, and
            nothing is verified either."
    (doseq [opts [{} {:build-indexes? true}]]
      (let [src    (counted-db)
            target (strip-source-count! (dump-of! src))
            tgt    (utils/setup-db (mem-cfg))
            r      (import-catching tgt target opts)]
        (is (= :import/verify-unavailable (:error r))
            (str "refused, and named accurately, under " opts))
        (is (nil? (:report r)) "no path certifies it")
        (teardown tgt)
        (teardown src)))))

(deftest an-uncheckable-import-is-never-reported-as-verified
  (testing "`:collect` reports instead of throwing — but it must report
            :unavailable, not silence"
    (let [src    (counted-db)
          target (strip-source-count! (dump-of! src))
          tgt    (utils/setup-db (mem-cfg))
          r      (m/import-db tgt target {:on-error :collect})]
      (is (nil? (:verified? r)) "not true")
      (is (= :unavailable (get-in r [:verification :status])))
      (is (= :no-source-count (get-in r [:verification :reason]))
          "and says why it could not be checked")
      (teardown tgt)
      (teardown src))))

(deftest a-failed-verification-reconciles-with-the-errors
  (testing "`{:verified? false, :errors []}` was the old shape and said nothing.
            A shortfall now states its size, and it matches what was collected —
            so the two halves of the report can be checked against each other."
    (let [src    (unique-db)
          target (dump-of! src)
          clash  (map (fn [r] (if (= :email (nth r 1)) (assoc (vec r) 2 "same@x") r)))
          tgt    (utils/setup-db (mem-cfg))
          r      (m/import-db tgt target {:on-error :collect :xform clash})]
      (is (false? (:verified? r)))
      (is (= :failed (get-in r [:verification :status])))
      (is (pos? (get-in r [:verification :missing])))
      (is (= (count (:errors r)) (get-in r [:verification :missing]))
          "every missing datom is accounted for by a collected error")
      (teardown tgt)
      (teardown src))))

;; ---------------------------------------------------------------------------
;; #70/#79 — a function must describe its own limits accurately
;;
;; `verify` and `estimate-import-memory` are synchronous and JVM-only, which is
;; documented. What was wrong is what they SAID: both called
;; `assert-sync-supported!` with a hardcoded `{:sync? true}`, so a ClojureScript
;; caller was told to "omit :sync? (the default here is false) and take from the
;; returned channel" — there is no :sync? to omit, and no channel. And on the
;; JVM an explicit `{:sync? false}` was silently ignored, so portable code
;; passing it worked on one runtime and threw a misleading message on the other.

(deftest a-synchronous-function-refuses-sync-false-rather-than-ignoring-it
  (testing "on the JVM too — otherwise the same call passes here and throws on
            ClojureScript, which is the shape that makes portable code hard to
            write against this namespace"
    (let [src    (counted-db)
          target (dump-of! src)]
      (is (map? (m/estimate-import-memory target)) "the ordinary call still works")
      (is (map? (m/estimate-import-memory target {:batch-size 50}))
          "and still reads the options it does honour")
      (let [e (try (m/estimate-import-memory target {:sync? false}) nil
                   (catch Exception e e))]
        (is (some? e) "an option it cannot honour is refused")
        (is (= :migrate/sync-only (:error (ex-data e))))
        (is (re-find #"no asynchronous form" (ex-message e))
            "saying what is actually true about it"))
      (teardown src))))
