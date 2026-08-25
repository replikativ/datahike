(ns datahike.test.migrate-fault-node-test
  "Fault injection on the import path, on Node — the coverage gap the audit
   named last and the one that hid the whole A-cluster.

   Everything the JVM suite proves about failure handling
   (`migrate_silent_failure_test`, `migrate_error_attribution_test`) it proves
   with blocking takes, which do not exist here. That would be a reason to skip
   the runtime if the failure MODES were the same. They are not:

   `go-try-` expands to `(try … (catch js/Error e e))` on ClojureScript and
   `(catch Exception e e)` on the JVM. ClojureScript can throw anything —
   `(throw #js {:msg \"…\"})` is legal, and a foreign library or a JS callback
   can produce one — and such a throw is NOT a `js/Error`, so it escapes the
   catch, the go block's channel CLOSES, and `<?-` yields `nil`. That is the
   A-F1 shape (\"a closed channel is not an empty result\") arriving through a
   door that exists only here. The JVM's counterpart is `Error`, which we
   deliberately do not catch.

   So these tests are not a port of the JVM ones for symmetry. They cover a
   distinct way of failing, plus the Phase 2/3 refusals running for the first
   time on the runtime the browser and Node deployments actually use.

   Every injection asserts a PRECONDITION that it took effect. `with-redefs` in
   an `:advanced` build only reaches calls that go through the var, and a
   redefinition that silently did nothing would leave every assertion below
   passing against an unmodified import."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.core.async :refer [go <! chan close!]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.store :as mstore]
            [konserve.core :as k]
            [konserve.store :as ks]))

(defn- cfg [] {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :write})

(defn- err-of
  "The `:error` an import reported, however it reported it. On ClojureScript
   `go-try-` delivers a thrown `ExceptionInfo` as the channel's VALUE, so a
   failure and a result arrive the same way and the caller must look."
  [v]
  (when (instance? js/Error v) (:error (ex-data v))))

(defn- filled!
  "A database with a handful of transactions, so a dump has several chunks."
  []
  (go (let [c (cfg)]
        (<! (d/create-database c))
        (let [conn (d/connect c)]
          (<! (d/transact! conn [{:db/ident :n :db/valueType :db.type/long
                                  :db/cardinality :db.cardinality/one}]))
          (doseq [i (range 8)]
            (<! (d/transact! conn [{:n i}])))
          {:conn conn :cfg c}))))

(defn- unique-filled! []
  (go (let [c (cfg)]
        (<! (d/create-database c))
        (let [conn (d/connect c)]
          (<! (d/transact! conn [{:db/ident :email :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one
                                  :db/unique :db.unique/identity}]))
          (doseq [i (range 5)]
            (<! (d/transact! conn [{:email (str "u" i "@x")}])))
          {:conn conn :cfg c}))))

(defn- dump!
  "Export `conn` to a fresh in-memory store; returns {:target .. :chunks n}.

   The chunk count is returned because the fault-injection tests fail a
   PARTICULAR chunk — with one chunk there is no \"second\" one to fail and they
   would pass without exercising anything."
  [conn opts]
  (go (let [store (<! (ks/create-store {:backend :memory :id (random-uuid)}
                                       {:sync? false}))
            target {:store store :prefix "fault"}
            ;; `:xform` goes to `export-transformed`, which is where a transform
            ;; lives now — positional there so it cannot be forgotten. This
            ;; helper still takes it in a map because these tests vary it as data.
            opts* (merge {:history? true :sort? false :chunk-size 4} opts)
            man (<! (if-let [xf (:xform opts*)]
                      (m/export-transformed @conn target xf (dissoc opts* :xform))
                      (m/export-db @conn target opts*)))]
        {:target target :chunks (count (:chunks man))})))

(defn- import-into!
  "Import into a fresh database; returns {:value .. :error ..} and cleans up."
  [target opts]
  (go (let [c (cfg)]
        (<! (d/create-database c))
        (let [tgt (d/connect c)
              v (<! (m/import-db tgt target opts))
              n (try (count (d/datoms @tgt :eavt)) (catch :default _ :unreadable))]
          (<! (d/delete-database c))
          {:value v :error (err-of v) :in-db n}))))

;; ---------------------------------------------------------------------------

(deftest a-foreign-throw-at-the-read-seam-fails-the-import-instead-of-the-process
  (testing "THE ClojureScript-only case, and it is worse than the silent nil the
            rest of this file is about.

            `go-try-` expands to `(catch js/Error e e)` here, so a value that is
            not an Error escapes it. core.async then catches `js/Object`, closes
            the go block's channel, and RETHROWS onto the microtask queue where
            nothing is listening. Measured before the fix: the whole Node
            process exited mid-import —

              cljs.core.async.impl.ioc_helpers.js:99
              throw ex;
              { msg: 'a foreign object, not a js/Error' }

            — taking the rest of the test run with it. The JVM has no analogue:
            a non-Exception throwable there is an `Error`, which we deliberately
            let propagate.

            The `:read` seam is where a CALLER-SUPPLIED function will run once
            the record source is public, so it is exactly where a foreign throw
            is likely. It is normalised into an ex-info the machinery can carry."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target chunks]} (<! (dump! conn {}))
                   orig mstore/read-chunk
                   n (atom 0)
                   ;; EXPLICIT ARITIES, not `[& a]`. ClojureScript compiles a
                   ;; fixed-arity call to `…$arity$4`, which a variadic
                   ;; replacement does not define — the import then dies with
                   ;; "read_chunk…arity$4 is not a function" and `n` stays 0. The
                   ;; precondition below is what caught that; without it this
                   ;; test would have "passed" having injected nothing.
                   r (with-redefs [mstore/read-chunk
                                   (fn
                                     ([med man c] (orig med man c))
                                     ([med man c opts]
                                      (if (= 2 (swap! n inc))
                                        (throw #js {:msg "a foreign object, not a js/Error"})
                                        (orig med man c opts))))]
                       (<! (import-into! target {})))]
               (is (> chunks 1) "precondition: more than one chunk, so one can be failed")
               (is (>= @n 2) "precondition: the injection ran and reached chunk 2")
               (is (= :async/foreign-throw (:error r))
                   (str "the import must fail, naming it — got " (pr-str (:value r))))
               (let [thrown (some-> (ex-data (:value r)) :thrown)]
                 (is (= "a foreign object, not a js/Error"
                        (aget thrown "msg"))
                     "with the thrown value preserved rather than discarded"))
               (<! (d/delete-database cfg))
               (done))))))

;; KNOWN LIMITATION, deliberately not tested: a foreign value thrown inside the
;; CALLEE's own go block escapes into core.async before any caller can see it,
;; and terminates the process just the same. Covering that means widening
;; `go-try-`'s ClojureScript catch from `js/Error` to `:default`, which belongs
;; to superv.async. A test for it could not run — it would kill the suite.

(deftest a-chunk-read-that-closes-its-channel-does-not-shorten-the-import
  (testing "the same guard reached the ordinary way. On Node every chunk read
            IS a channel, so this is the shape a backend error takes here —
            where on the JVM the sync path returns a bare nil instead."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target chunks]} (<! (dump! conn {}))
                   orig mstore/read-chunk
                   n (atom 0)
                   r (with-redefs [mstore/read-chunk
                                   (fn
                                     ([med man c] (orig med man c))
                                     ([med man c opts]
                                      (if (= 2 (swap! n inc))
                                        (doto (chan) (close!))
                                        (orig med man c opts))))]
                       (<! (import-into! target {})))]
               (is (> chunks 1) "precondition: more than one chunk")
               (is (>= @n 2) "precondition: the injection ran")
               (is (= :async/no-result (:error r)))
               (<! (d/delete-database cfg))
               (done))))))

(deftest an-unexplained-shortfall-is-refused-on-node
  (testing "the completeness gate, running for the first time off the JVM. A
            dump holding fewer records than its source with nothing to explain
            the gap is what a truncated export looks like."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   src (count (d/datoms @conn :eavt))
                   {:keys [target]} (<! (dump! conn {:xform (take 6)}))
                   med (<! (mstore/open target {:sync? false}))
                   man (<! (mstore/read-manifest med {:sync? false}))]
               (is (= 6 (get-in man [:stats :datom-count])))
               (is (= src (get-in man [:stats :source-datom-count]))
                   "the source count is recorded on Node too")
               (is (true? (get-in man [:stats :transformed?]))
                   "and an :xform dump explains its own shortfall, so it imports")
               (is (nil? (:error (<! (import-into! target {})))))
               (testing "strip the explanation and it is refused"
                 ;; The manifest's key, spelled out rather than reached through
                 ;; `mstore/ckey` — which is private, and which ClojureScript
                 ;; would rename under :advanced. Duplicating it here is
                 ;; deliberate: this IS the wire location, and a test that
                 ;; derived it from the code under test could not notice the
                 ;; code moving it.
                 (<! (k/assoc (:store med)
                              ["datahike.migrate" (:prefix med) "manifest"]
                              (assoc-in man [:stats :transformed?] false)
                              {:sync? false}))
                 (let [r (<! (import-into! target {}))]
                   (is (= :import/incomplete-dump (:error r)))
                   (testing "unless the caller opts in"
                     (is (nil? (:error (<! (import-into! target {:allow-partial? true}))))))))
               (<! (d/delete-database cfg))
               (done))))))

(deftest verification-says-which-outcome-it-was-on-node
  (testing "`:verified? nil` alone cannot distinguish 'switched off' from
            'nothing to compare against'. Both reach Node."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target]} (<! (dump! conn {}))
                   ok (<! (import-into! target {}))
                   skipped (<! (import-into! target {:verify? false}))]
               (is (true? (:verified? (:value ok))))
               (is (= :ok (get-in (:value ok) [:verification :status])))
               (is (nil? (:verified? (:value skipped))))
               (is (= :skipped (get-in (:value skipped) [:verification :status]))
                   "and says so, rather than leaving nil to be read as fine")
               (<! (d/delete-database cfg))
               (done))))))

(deftest a-record-fault-is-collected-under-its-own-key-on-node
  (testing "`:on-error :collect` must name what datahike said. The label used to
            be a fallback because the error key did not survive the writer
            boundary — a different boundary here (a promise-chan, not a
            CompletableFuture), so it is worth proving on this runtime too."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (unique-filled!))
                   {:keys [target]} (<! (dump! conn {}))
                   clash (map (fn [r] (if (= :email (nth r 1))
                                        (assoc (vec r) 2 "same@x") r)))
                   r (<! (import-into! target {:on-error :collect :xform clash
                                               :verify? false}))
                   errs (:errors (:value r))]
               (is (pos? (count errs)) "the offending records are collected")
               (is (= #{:transact/unique} (set (map :error errs)))
                   "under datahike's own key, not the :import/corrupt-datom fallback")
               (is (every? :datom errs) "each naming the record it blames")
               (<! (d/delete-database cfg))
               (done))))))

(deftest the-jvm-only-entry-points-refuse-by-name-on-node
  (testing "`verify` and `estimate-import-memory` have no ClojureScript form.
            They used to say so by claiming `:sync? true` was unsupported and
            advising the caller to omit an argument that is a literal and take
            from a channel that does not exist."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target]} (<! (dump! conn {}))]
               (doseq [f [#(m/verify target)
                          #(m/estimate-import-memory target)]]
                 (let [e (try (f) nil (catch :default e e))]
                   (is (some? e) "refused")
                   (is (= :migrate/jvm-only (:error (ex-data e)))
                       "by name, and for the true reason")
                   (is (re-find #"JVM only" (ex-message e)))))
               (<! (d/delete-database cfg))
               (done))))))

;; ---------------------------------------------------------------------------
;; Integrity refuses to fail open — on the store medium, which is the ONLY
;; medium a browser has.
;;
;; These three guards existed on the filesystem path first and were not carried
;; across; measured on a store dump before `assert-dump-manifest!` unified them,
;; a chunk entry with `:sha256` deleted imported tampered bytes and reported
;; `{:verified? true}`, and a MISSING manifest — the normal shape of an export
;; that died midway, since the manifest is the commit marker — compared 0
;; against 0 and reported success. So they were absent for exactly the
;; deployment that most needs them, and this runtime is that deployment.

(defn- manifest-key [prefix] ["datahike.migrate" prefix "manifest"])

(defn- rewrite-manifest! [med f]
  (go (let [man (<! (mstore/read-manifest med {:sync? false}))]
        (<! (k/assoc (:store med) (manifest-key (:prefix med)) (f man) {:sync? false}))
        man)))

(deftest a-chunk-whose-hash-does-not-match-is-refused-on-node
  (testing "the per-chunk SHA-256 is checked as the chunk is read, on this
            medium too — a declared hash that does not match the bytes means
            the dump and the manifest disagree, whichever of them moved"
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target]} (<! (dump! conn {}))
                   med (<! (mstore/open target {:sync? false}))]
               (<! (rewrite-manifest! med #(assoc-in % [:chunks 0 :sha256]
                                                     (apply str (repeat 64 "0")))))
               (let [r (<! (import-into! target {}))]
                 (is (= :import/checksum-failed (:error r))
                     (str "refused — got " (pr-str (:value r)))))
               (testing "and `:checksums :skip` is the one way past it"
                 (is (nil? (:error (<! (import-into! target {:checksums :skip}))))))
               (<! (d/delete-database cfg))
               (done))))))

(deftest a-chunk-with-no-declared-hash-is-refused-on-node
  (testing "fails CLOSED: a chunk with no declared hash is refused rather than
            treated as unhashed, so a manifest edited to remove integrity cannot
            buy itself a clean import"
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target]} (<! (dump! conn {}))
                   med (<! (mstore/open target {:sync? false}))]
               (<! (rewrite-manifest! med #(update-in % [:chunks 0] dissoc :sha256)))
               (is (= :import/missing-checksum (:error (<! (import-into! target {})))))
               (is (nil? (:error (<! (import-into! target {:checksums :skip}))))
                   "explicitly opting out still works, and warns")
               (<! (d/delete-database cfg))
               (done))))))

(deftest a-source-with-no-manifest-is-not-a-dump-on-node
  (testing "the manifest is written LAST as the commit marker, so a source
            without one is an export that did not finish. It used to be read as
            an empty dump: every guard no-ops on nil, expected and live were
            both 0, and an import of nothing reported success."
    (async done
           (go
             (let [{:keys [conn cfg]} (<! (filled!))
                   {:keys [target]} (<! (dump! conn {}))
                   med (<! (mstore/open target {:sync? false}))]
               (<! (k/dissoc (:store med) (manifest-key (:prefix med)) {:sync? false}))
               (let [r (<! (import-into! target {}))]
                 (is (= :import/not-a-dump (:error r))
                     (str "refused — got " (pr-str (:value r)))))
               (<! (d/delete-database cfg))
               (done))))))
