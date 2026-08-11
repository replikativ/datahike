(ns datahike.test.migrate-completeness-test
  "A dump must be able to say that it holds less than its source did.

   Every integrity signal a dump carries — `:datom-count`, the semantic digest,
   the per-chunk SHA-256 — is derived from the WRITE PATH. So a dump that lost
   records agrees with itself perfectly, and checking it against itself proves
   nothing. Measured before this existed: a 205-datom database exported short to
   120 produced a manifest saying 120, a matching digest, and `verify` returned
   `:ok? true`. An operator restoring that backup would have been told everything
   was fine.

   The fix is one INDEPENDENT witness — `:source-datom-count`, taken from the
   database rather than from the stream — plus `:transformed?` to say when a
   shortfall is EXPLAINED. Filtering one tenant out with an `:xform` is a
   legitimately smaller dump; the same shortfall with no xform is loss."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.store :as mstore]
            [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- filled []
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn (vec (for [i (range 200)] {:n i})))
    conn))

(defn- dump! [conn opts]
  (let [dir (str (System/getProperty "java.io.tmpdir") "/dh-complete-" (utils/get-time))]
    (m/export-db @conn dir (merge {:history? true} opts))
    dir))

(defn- stats [dir] (:stats (edn/read-string (slurp (str dir "/manifest.edn")))))

(defn- import-into [dir opts]
  (let [c (utils/setup-db (mem-cfg))]
    (try {:ok (m/import-db c dir opts)}
         (catch Exception e {:error (:error (ex-data e)) :missing (:missing (ex-data e))})
         (finally (teardown c)))))

(deftest a-dump-records-what-its-source-held
  (testing "not only what was written — the two together are what make a short
            dump detectable"
    (let [conn (filled)
          src  (count (d/datoms @conn :eavt))
          st   (stats (dump! conn {}))]
      (is (= src (:datom-count st)) "a complete dump wrote everything")
      (is (= src (:source-datom-count st)) "and records the source's own count")
      (is (false? (:transformed? st)))
      (teardown conn))))

(deftest an-xform-dump-is-smaller-and-says-why
  (testing "filtering is legitimate, so it must import — but the artefact still
            has to admit it holds less than the source"
    (let [conn (filled)
          dir  (dump! conn {:xform (take 120)})
          st   (stats dir)]
      (is (= 120 (:datom-count st)))
      (is (< (:datom-count st) (:source-datom-count st)) "smaller than its source")
      (is (true? (:transformed? st)) "and the shortfall is explained")
      (is (:ok (import-into dir {})) "so it imports")
      (teardown conn))))

(deftest an-unexplained-shortfall-is-refused
  (testing "THE case. A dump missing records with nothing to explain them is what
            a partially-written export looks like, and importing it would restore
            a silently incomplete database.

            Simulated by clearing `:transformed?` on an xform dump, which is
            exactly the state a truncated export leaves: fewer records than the
            source, no explanation."
    (let [conn (filled)
          dir  (dump! conn {:xform (take 120)})
          f    (str dir "/manifest.edn")
          man  (edn/read-string (slurp f))]
      (spit f (pr-str (assoc-in man [:stats :transformed?] false)))
      (let [r (import-into dir {})]
        (is (= :import/incomplete-dump (:error r)))
        (is (= 85 (:missing r)) "and says how many are unaccounted for"))
      (testing "with an explicit opt-in it is still importable"
        (is (:ok (import-into dir {:allow-partial? true}))))
      (teardown conn))))

(deftest the-refusal-covers-both-media
  (testing "a konserve-store dump gets the same gate as a filesystem one.

            This is not hypothetical symmetry: the check was written for the
            store branch first and the filesystem dump sailed straight through —
            the same shape as the `:sha256` fail-closed rule, which was written
            for the filesystem and not carried across to the store."
    (let [conn   (filled)
          src    (count (d/datoms @conn :eavt))
          store  (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)} {:sync? true})
          target {:store store :prefix "backup-1"}
          _      (m/export-db @conn target {:history? true :xform (take 120)})
          medium (mstore/open target {:sync? true})
          man    (mstore/read-manifest medium)]
      (is (= 120 (get-in man [:stats :datom-count])) "the store medium records both counts too")
      (is (= src (get-in man [:stats :source-datom-count])))
      ;; strip the explanation, exactly as the filesystem case does
      (k/assoc (:store medium) (#'mstore/ckey (:prefix medium) "manifest")
               (assoc-in man [:stats :transformed?] false) {:sync? true})
      (let [c (utils/setup-db (mem-cfg))
            r (try {:ok (m/import-db c target {})}
                   (catch Exception e {:error (:error (ex-data e))})
                   (finally (teardown c)))]
        (is (= :import/incomplete-dump (:error r))
            "the store branch refuses it too"))
      (mstore/close medium {:sync? true})
      (teardown conn))))

;; ---------------------------------------------------------------------------
;; The `store-target?` fork, pinned
;;
;; Three times in this branch a rule was written for one medium and not carried
;; to the other, each time silently: the `:sha256` fail-closed rule (filesystem
;; first), the incomplete-dump refusal above (store first), and `:checksums
;; :skip` (filesystem only — measured, a store dump refused it outright while
;; the same dump on disk imported). Nothing failed any of the three times,
;; because every test used one medium.
;;
;; So the assertion is PARITY itself, over the matrix, rather than each medium's
;; behaviour separately. A rule added to one branch and not the other fails here
;; whichever branch it lands in.

(defn- store-dump! [conn opts]
  (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                               {:sync? true})
        target {:store store :prefix "parity"}]
    (m/export-db @conn target (merge {:history? true :sort? false :chunk-size 4} opts))
    target))

(defn- outcome
  "`:imported` or the `:error` keyword it was refused with."
  [target opts]
  (let [c (utils/setup-db (mem-cfg))]
    (try (do (m/import-db c target opts) :imported)
         (catch Exception e (:error (ex-data e)))
         (finally (teardown c)))))

(defn- damage-fs! [dir f]
  (let [p (str dir "/manifest.edn")]
    (spit p (pr-str (f (edn/read-string (slurp p)))))
    dir))

(defn- damage-store! [target f]
  (let [med (mstore/open target {:sync? true})]
    (k/assoc (:store med) (#'mstore/ckey (:prefix med) "manifest")
             (f (mstore/read-manifest med)) {:sync? true})
    target))

(deftest the-two-media-agree-on-every-integrity-outcome
  (testing "same damage, same options, same answer — whichever medium holds it"
    (let [conn (filled)]
      (doseq [[label damage] [[:bad-hash  #(assoc-in % [:chunks 0 :sha256]
                                                     (apply str (repeat 64 "0")))]
                              [:no-hash   #(update-in % [:chunks 0] dissoc :sha256)]]
              [opt-label opts] [[:default {}]
                                [:skip    {:checksums :skip}]]]
        (let [dir (str (System/getProperty "java.io.tmpdir") "/dh-parity-" (utils/get-time))
              _ (m/export-db @conn dir {:history? true :sort? false :chunk-size 4})
              fs-out (outcome (damage-fs! dir damage) opts)
              st-out (outcome (damage-store! (store-dump! conn {}) damage) opts)]
          (is (= fs-out st-out)
              (str label " + " opt-label ": filesystem said " fs-out
                   ", store said " st-out))
          (when (= :default opt-label)
            (is (not= :imported fs-out)
                (str label " must fail closed by default — otherwise the parity "
                     "above is satisfied by both media being broken")))
          (when (= :skip opt-label)
            (is (= :imported fs-out)
                ":checksums :skip must actually be an escape hatch, on both"))))
      (teardown conn))))
