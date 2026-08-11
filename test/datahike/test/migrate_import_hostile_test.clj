(ns datahike.test.migrate-import-hostile-test
  "What `import-db` does with input `export-db` would never produce.

   ## Why this exists, and how to read it

   A dump is normally written by this same code, so the realistic threats are
   narrow: bit-rot, truncation, a hand-edited dump, a foreign producer using the
   documented `{:chunks .. :read ..}` seam, and a buggy `:xform`. This namespace
   feeds exactly those into a real import and records what comes out.

   **These are CHARACTERISATION tests, not specifications.** Most of them assert
   behaviour that is wrong — a record silently corrupted, an import reporting
   `:verified? true` over a database that does not match its dump. Each such
   test says so in a `testing` string beginning with `TODAY:`. When a check is
   added that makes one of them fail, the failure is the point: update the
   assertion to the new, better behaviour rather than deleting the test.

   The dumps are built by rewriting a real dump's chunks and recomputing the
   per-chunk SHA-256, the counts and the semantic digest, so every dump here is
   INTERNALLY CONSISTENT. Nothing below is caught by tier-0 integrity, because
   there is nothing wrong with the bytes — only with what they say.

   Everything pins `:index :datahike.index/persistent-set` explicitly: the suite
   runs each namespace a second time under `:clj-hht` with `*default-index*`
   rebound, and `:build-indexes?` is refused for anything else."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.constants :as c]
            [datahike.db.transaction :as dbt]
            [datahike.migrate :as m]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.digest :as dig]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.manifest :as mman]))

;; ---------------------------------------------------------------------------
;; fixture: a small database, its dump, and a way to rewrite that dump

(def ^:private base-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])

(defn- cfg [history?]
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :index :datahike.index/persistent-set
   :keep-history? history? :schema-flexibility :write})

(defn- conn! [c] (d/delete-database c) (d/create-database c) (d/connect c))

(defn- tmp-dir! [] (str (fs/temp-dir! "dh-hostile")))

(defn- small-db! []
  (let [conn (conn! (cfg true))]
    (d/transact conn base-schema)
    (d/transact conn [{:db/id -1 :name "a" :score 1 :tag :x}
                      {:db/id -2 :name "b" :score 2}])
    (d/transact conn [{:db/id [:name "a"] :score 10}])
    (d/transact conn [{:db/id [:name "a"] :pal [:name "b"]}])
    conn))

(defn- card-many-db!
  "A database whose current state depends on a RETRACTION surviving the round
   trip — the shape a card-one upsert would otherwise mask."
  []
  (let [conn (conn! (cfg true))]
    (d/transact conn base-schema)
    (d/transact conn [{:db/id -1 :name "a" :tag :x}])
    (d/transact conn [{:db/id [:name "a"] :tag :y}])
    (d/transact conn [[:db/retract [:name "a"] :tag :x]])
    conn))

(defn- manifest-of [dir]
  (edn/read-string {:readers *data-readers*} (slurp (str dir "/manifest.edn"))))

(defn- dump-records [dir]
  (let [man (manifest-of dir)
        codec (mman/codec-of man)]
    (vec (mapcat (fn [{:keys [file]}]
                   (let [bs (fs/read-bytes (str dir "/" file))]
                     (vec (mcbor/decode-records-from
                           (if (= :none codec) bs (mz/decompress-bytes codec bs {}))))))
                 (:chunks man)))))

(defn- rewrite-dump!
  "Replace `dir`'s chunks with `records`, recomputing every integrity field the
   manifest carries. The result passes tier-0 by construction, which is the
   whole point: these are dumps that are intact and wrong."
  [dir records]
  (let [man (manifest-of dir)
        codec (mman/codec-of man)]
    (doseq [{:keys [file]} (:chunks man)] (.delete (io/file (str dir "/" file))))
    (let [encs (mapv mcbor/encode-record records)
          raw  (mcbor/concat-records encs)
          fname (str "datoms-000001.cbor" (mz/extension codec))
          bs   (if (= :none codec) raw (mz/compress-bytes codec raw))
          _    (when (seq records)
                 (with-open [o (io/output-stream (str dir "/" fname))] (.write o ^bytes bs)))
          digest (dig/finalize (reduce dig/add-record (dig/accumulator) encs))]
      (spit (str dir "/manifest.edn")
            (pr-str (assoc man
                           :chunks (if (seq records)
                                     [{:file fname :count (count records) :bytes (count bs)
                                       :raw-bytes (count raw) :sha256 (dig/sha256-hex raw)}]
                                     [])
                           :semantic-digest digest
                           ;; `:transformed? true` because that is what this
                           ;; helper DOES: `f` rewrote the records, so a record
                           ;; count below `:source-datom-count` is explained.
                           ;; Without it every shrinking rewrite here — the
                           ;; zero-record dump most obviously — is
                           ;; indistinguishable from a truncated export and is
                           ;; refused as `:import/incomplete-dump` before any of
                           ;; these tests' actual subjects are reached. That
                           ;; check is about UNexplained shortfalls; these dumps
                           ;; are intact and wrong, which is a different thing.
                           :stats (assoc (:stats man)
                                         :datom-count (:count digest)
                                         :transformed? true))))
      dir)))

(defn- dump-of
  "Export `conn`, then optionally rewrite the records through `f`."
  ([conn] (dump-of conn identity))
  ([conn f]
   (let [dir (tmp-dir!)]
     (m/export-db conn dir {:history? true})
     (rewrite-dump! dir (vec (f (dump-records dir))))
     dir)))

(defn- import-into!
  "Import and return `{:report r}` or `{:threw msg :data d}` — never both."
  [conn dir opts]
  (try {:report (m/import-db conn dir opts)}
       (catch Exception e {:threw (or (ex-message e) (str (class e))) :data (ex-data e)})))

(defn- current [conn]
  (mapv (fn [x] [(nth x 0) (nth x 1) (nth x 2) (nth x 3) (nth x 4)]) (d/datoms @conn :eavt)))

(defn- history-count [conn] (count (d/datoms (d/history @conn) :eavt)))

(defn- release! [& conns]
  (doseq [conn conns] (when conn (try (d/release conn) (catch Exception _ nil)))))

;; ---------------------------------------------------------------------------
;; 1. `op` is used as a truthiness test, never checked to be a boolean

(deftest non-boolean-op-is-refused
  (testing "a producer writing 0/1 for `op` USED TO have every retraction silently
            asserted, with :verified? true over a database whose current state
            differed from the dump's. `op` is read by truthiness and 0 is truthy
            in Clojure, so nothing downstream could notice.

            Now refused by name. This is the check that matters most for the
            documented foreign-producer seam: 0/1 is the natural encoding from
            any other language."
    (let [src (card-many-db!)
          bool-dir (dump-of src)
          int-dir  (dump-of src (fn [rs] (mapv (fn [[e a v t op]] [e a v t (if op 1 0)]) rs)))
          ok   (conn! (cfg true))
          bad  (conn! (cfg true))]
      (try
        ;; the honest dump restores the source's current state exactly
        (import-into! ok bool-dir {})
        (is (= [:y] (mapv #(nth % 2) (filter #(= :tag (nth % 1)) (current ok))))
            "control: the retraction of :tag :x survives a normal round trip")

        ;; 0/1 instead of false/true, through the index-build path
        (let [res (import-into! bad int-dir {:build-indexes? true})]
          (is (= :import/malformed-record (:error (:data res)))
              "refused by name rather than imported")
          (is (re-find #"op is not a boolean" (str (:threw res)))
              "and the message says which field and why"))
        (finally (release! src ok bad))))))

(deftest non-boolean-op-streaming-is-refused-too
  (testing "the streaming path corrupted the same way, and was caught only by
            luck: the datom count happened to disagree, so `:verify? true` threw
            a count mismatch that said nothing about `op` — and `:verify? false`
            said nothing at all. Both paths now refuse the record itself."
    (let [src (card-many-db!)
          dir (dump-of src (fn [rs] (mapv (fn [[e a v t op]] [e a v t (if op 1 0)]) rs)))
          conn (conn! (cfg true))]
      (try
        (let [res (import-into! conn dir {})]
          (is (= :import/malformed-record (:error (:data res)))
              "refused on `op`, not on a downstream count mismatch")
          (is (empty? (filter #(= :tag (nth % 1)) (current conn)))
              "and nothing was written — the refusal is before the first flush"))
        (finally (release! src conn))))))

;; ---------------------------------------------------------------------------
;; 2. structural garbage a foreign producer or a buggy :xform can emit

(deftest nil-entity-id-is-refused
  (testing "`e` nil used to pass: `(or (migrated-eid m nil) max-eid)` allocates
            ONE id for the nil key and remembers it, so every nil-`e` record
            became an attribute of the SAME entity, with :verified? true."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (into (vec rs) [[nil :score 42 (+ c/tx0 4) true]
                                                    [nil :name "zz" (+ c/tx0 4) true]])))
          conn (conn! (cfg true))]
      (try
        (let [res (import-into! conn dir {})]
          (is (= :import/malformed-record (:error (:data res))))
          (is (re-find #"e is not an integer" (str (:threw res)))))
        (finally (release! src conn))))))

(deftest nil-value-is-refused
  (testing "`validate-val` refuses a nil value on the transact path (\"Cannot
            store nil as a value\") but the import path never calls it, so a nil
            value reached the index trees on BOTH import paths. Now refused on
            both, at the same seam."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) [7 :score nil (+ c/tx0 4) true])))
          a (conn! (cfg true))
          b (conn! (cfg true))]
      (try
        (is (= :import/malformed-record (:error (:data (import-into! a dir {}))))
            "streaming")
        (is (= :import/malformed-record
               (:error (:data (import-into! b dir {:build-indexes? true}))))
            "index build")
        (finally (release! src a b))))))

(deftest malformed-records-are-refused-by-name
  (testing "a short record and a non-keyword attribute always failed, but as a
            bare IndexOutOfBoundsException / ClassCastException from inside a
            comparator — no :error key, and for the short record no message at
            all, so nothing told the operator which record was bad."
    (let [src (small-db!)
          short-dir (dump-of src (fn [rs] (conj (vec rs) [7 :score 42])))
          strattr-dir (dump-of src (fn [rs] (conj (vec rs) [7 "score" 42 (+ c/tx0 4) true])))
          a (conn! (cfg true))
          b (conn! (cfg true))]
      (try
        (let [res (import-into! a short-dir {})]
          (is (= :import/malformed-record (:error (:data res))))
          (is (re-find #"not a 5-element vector" (str (:threw res)))))
        (let [res (import-into! b strattr-dir {})]
          (is (= :import/malformed-record (:error (:data res))))
          (is (re-find #"a is not a keyword" (str (:threw res)))))
        (finally (release! src a b))))))

;; ---------------------------------------------------------------------------
;; 3. the schema is never consulted about the data

(deftest value-not-conforming-to-its-declared-type-is-stored-and-verified
  (testing "TODAY: a string under a :db.type/long attribute is stored verbatim,
            import reports :verified? true, and `verify` reports :ok? true at
            every tier — because the dump and the database agree with each other
            and neither is compared against the schema."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) [7 :score "not-a-long" (+ c/tx0 4) true])))
          conn (conn! (cfg true))]
      (try
        (let [res (import-into! conn dir {})]
          (is (true? (:verified? (:report res))))
          (is (some #(= "not-a-long" (nth % 2)) (current conn)))
          (is (true? (:ok? (m/verify dir))) "verify 1-arity: intact")
          (is (true? (:ok? (m/verify conn dir))) "verify 2-arity: equivalent — and both are right"))
        (finally (release! src conn))))))

(deftest attribute-absent-from-the-schema-is-stored-under-write-flexibility
  (testing "TODAY: `:schema-flexibility :write` refuses an undeclared attribute
            on the transact path; the import path stores it. Both paths."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) [7 :totally/undeclared 42 (+ c/tx0 4) true])))
          a (conn! (cfg true))
          b (conn! (cfg true))]
      (try
        (is (true? (:verified? (:report (import-into! a dir {})))))
        (is (some #(= :totally/undeclared (nth % 1)) (current a)))
        (is (nil? (get-in @a [:schema :totally/undeclared])) "and it is in no schema")
        (is (true? (:verified? (:report (import-into! b dir {:build-indexes? true})))))
        (is (some #(= :totally/undeclared (nth % 1)) (current b)))
        (finally (release! src a b))))))

(deftest an-xform-that-drops-schema-datoms-produces-a-schemaless-database
  (testing "TODAY: the most realistic buggy-:xform shape. Dropping the schema
            datoms leaves the data datoms behind: no attribute is declared, so
            `dbu/ref?` is false for every one of them and REF VALUES ARE NEVER
            REMAPPED — the surviving refs point at source ids that no longer
            exist. :verified? true throughout."
    (let [src (small-db!)
          dir (dump-of src)
          conn (conn! (cfg true))
          res (import-into! conn dir
                            {:xform (remove #(#{:db/ident :db/valueType :db/cardinality :db/unique}
                                              (nth % 1)))})]
      (try
        (is (true? (:verified? (:report res)))
            "the drop is subtracted from the expectation, so the count check cannot see it")
        (is (not-any? #(contains? (:schema @conn) %) [:name :score :tag :pal])
            "no user schema survives")
        (is (seq (filter #(= :name (nth % 1)) (current conn)))
            "but the data datoms did")
        (let [pal (first (filter #(= :pal (nth % 1)) (current conn)))
              eids (set (map first (current conn)))]
          (is (some? pal))
          (is (not (contains? eids (nth pal 2)))
              "TODAY: the ref value was not remapped and now dangles"))
        (finally (release! src conn))))))

;; ---------------------------------------------------------------------------
;; 4. `t` outside the transaction id space

(deftest t-below-tx0-is-refused-on-both-paths
  (testing "`dd/datom`'s 5-arity encodes `added` in the SIGN of tx, so t=0 cannot
            represent an assertion. The index build passed the raw t straight
            through `record->datom` and produced a RETRACTION datom sitting in
            the current index; the streaming path happened to be immune because
            it remaps `t` through :tids first.

            Both now refuse it. The divergence was the argument for refusing
            rather than for leaving the tolerant path alone: one path silently
            coping while the other silently corrupted is worse than either, and
            export never emits t <= tx0 in the first place."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) [7 :score 42 0 true])))
          a (conn! (cfg true))
          b (conn! (cfg true))
          c2 (conn! (cfg true))]
      (try
        (is (= :import/malformed-record (:error (:data (import-into! a dir {}))))
            "streaming")

        (is (= :import/malformed-record
               (:error (:data (import-into! b dir {:build-indexes? true}))))
            "index build: refused on the record, not on a downstream count mismatch")

        (let [res (import-into! c2 dir {:build-indexes? true :verify? false})]
          (is (= :import/malformed-record (:error (:data res)))
              "and with :verify? false too — this used to be the silent case"))
        (finally (release! src a b c2))))))

;; ---------------------------------------------------------------------------
;; 5. duplicate records

(deftest duplicate-record-index-build-history-throws-unattributed
  (testing "`from-sorted-seq` refuses non-distinct input, which is the right
            answer — but the message comes from persistent-sorted-set and carries
            no :error key, so nothing says `this dump has a duplicate record`."
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) (first (filter #(= :score (nth % 1)) rs)))))
          conn (conn! (cfg true))
          res (import-into! conn dir {:build-indexes? true})]
      (try
        (is (some? (:threw res)))
        (is (re-find #"strictly ascending" (:threw res)))
        (is (nil? (:error (:data res))) "TODAY: unattributed")
        (finally (release! src conn))))))

(deftest duplicate-record-no-history-index-build-changes-the-hash
  (testing "TODAY: without temporal trees the currentness fold DEDUPES the run,
            so nothing refuses the duplicate — but `:hash` is summed over the
            RECORDS, so it counts the duplicate twice. Same datoms, different
            `:hash`. The default count check catches it; `:verify? false` does not."
    (let [src (let [conn (conn! (assoc (cfg false) :keep-history? false))]
                (d/transact conn base-schema)
                (d/transact conn [{:db/id -1 :name "a" :score 1}])
                conn)
          dir (tmp-dir!)
          _ (m/export-db src dir {:history? false})
          rs (dump-records dir)
          dup (first (filter #(= :score (nth % 1)) rs))
          dup-dir (tmp-dir!)]
      (try
        ;; both dumps carry the same manifest shape; only the record list differs
        (doseq [f (.listFiles (io/file dir))]
          (io/copy f (io/file (str dup-dir "/" (.getName f)))))
        (rewrite-dump! dup-dir (conj (vec rs) dup))
        (let [a (conn! (assoc (cfg false) :keep-history? false))
              b (conn! (assoc (cfg false) :keep-history? false))]
          (try
            (is (nil? (:threw (import-into! a dir {:build-indexes? true}))))
            (is (nil? (:threw (import-into! b dup-dir {:build-indexes? true :verify? false}))))
            (is (= (count (current a)) (count (current b)))
                "the duplicate produces no extra datom")
            (is (not= (:hash @a) (:hash @b))
                "TODAY: but :hash differs — two databases with identical content
                 that will not compare equal")
            (finally (release! a b))))
        (finally (release! src))))))

;; ---------------------------------------------------------------------------
;; 6. the manifest's :schema is trusted without being checked against the datoms

(deftest manifest-schema-that-disagrees-with-the-datoms-corrupts-an-index-build
  (testing "TODAY: `:build-indexes?` refuses a manifest with NO :schema, because
            `ids/build-mapping` needs it to identify ref values. It does not check
            that the schema it was given is the one the datoms were written under.
            A schema missing a ref attribute leaves that attribute's values
            unmapped; a schema calling a non-ref attribute a ref rewrites its
            values into the entity id space. Both report :verified? true."
    (let [src (small-db!)]
      (try
        ;; (a) :pal missing from the manifest schema, under a NON-identity mapping
        (let [dir (tmp-dir!)
              _ (m/export-db src dir {:history? true})
              shift (fn [x] (if (and (number? x) (< (long x) (long c/tx0))) (+ (long x) 1000) x))
              rs (mapv (fn [[e a v t op]] [(shift e) a (if (= a :pal) (shift v) v) t op])
                       (dump-records dir))
              _ (rewrite-dump! dir rs)
              _ (spit (str dir "/manifest.edn")
                      (pr-str (update (manifest-of dir) :schema dissoc :pal)))
              conn (conn! (cfg true))
              res (import-into! conn dir {:build-indexes? true :eids :allocate :check-refs? true})]
          (try
            (is (true? (:verified? (:report res))))
            (is (pos? (:count (:dangling-refs (:report res))))
                "TODAY: the ref value kept its SOURCE id — only the opt-in
                 :check-refs? pass notices")
            (finally (release! conn))))

        ;; (b) the manifest calls :score a ref; the dump's values are longs
        (let [dir (tmp-dir!)
              _ (m/export-db src dir {:history? true})
              _ (rewrite-dump! dir (dump-records dir))
              _ (spit (str dir "/manifest.edn")
                      (pr-str (assoc-in (manifest-of dir) [:schema :score :db/valueType] :db.type/ref)))
              conn (conn! (cfg true))
              res (import-into! conn dir {:build-indexes? true :eids :allocate})]
          (try
            (is (true? (:verified? (:report res))))
            (is (not (contains? (set (map #(nth % 2) (filter #(= :score (nth % 1)) (current conn)))) 10))
                "TODAY: the long value 10 was rewritten into the entity id space")
            (finally (release! conn))))
        (finally (release! src))))))

;; ---------------------------------------------------------------------------
;; 7. :db.secondary/only — the primary holds a content hash, and import re-hashes it

(deftest secondary-only-values-round-trip
  (testing "a :db.secondary/only attribute's value survives export/import.

            It did not, and the reason was subtler than a hashing bug. The
            primary indexes hold `hasch(v)` — `project-primary` substitutes it —
            and the real value goes ONLY to the secondary index. Export read the
            primary indexes, so the dump carried the hash and the value was
            ABSENT FROM THE BACKUP; the visible symptom was a restored primary
            holding `hasch(hasch(v))`, because import re-projected the hash it
            found.

            Export now reads the value from the secondary index
            (`ISecondaryScannable`), so the dump carries `v`. Import needs no
            change: re-projection then lands the primary on `hasch(v)` (correct)
            and feeds the secondary `v` (correct). The double-hash was a symptom
            of the missing value, not a separate defect."
    (require 'datahike.index.secondary.scriptum)
    (let [p1 (str "/tmp/dh-hostile-so-" (java.util.UUID/randomUUID))
          p2 (str "/tmp/dh-hostile-so-" (java.util.UUID/randomUUID))
          src (conn! (cfg true))]
      (d/transact src [{:db/ident :doc/body :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one :db.secondary/only true}])
      (d/transact src [{:db/ident :idx/ft :db.secondary/type :scriptum
                        :db.secondary/attrs [:doc/body]
                        :db.secondary/config {:path p1}}])
      (Thread/sleep 600)
      (d/transact src [{:db/id -1 :doc/body "hello datalog"}])
      (Thread/sleep 400)
      (let [stored (d/q '[:find ?v . :where [?e :doc/body ?v]] @src)
            dir (tmp-dir!)]
        (m/export-db src dir {:history? true})
        (release! src)
        (let [conn (conn! (cfg true))
              ;; rewrite the machine-local index path, which `check-target!` warns
              ;; about and which would otherwise collide with the source's lock
              res (import-into! conn dir
                                {:xform (map (fn [r] (if (= :db.secondary/config (nth r 1))
                                                       (assoc r 2 {:path p2}) r)))})]
          (try
            (Thread/sleep 500)
            (is (true? (:verified? (:report res))) "the import reports success")
            (let [restored (d/q '[:find ?v . :where [?e :doc/body ?v]] @conn)]
              (is (= stored restored)
                  "the primary holds the same content hash it did at the source")
              (is (not= (dbt/secondary-only-hash stored) restored)
                  "and specifically NOT the hash of the hash, which is what a dump
                   carrying the projected value used to produce"))
            ;; NOT `:ok? true`, and not because of the value: this fixture
            ;; rewrites `:db.secondary/config` with an `:xform` on import (to
            ;; avoid colliding with the source's Lucene write lock on the same
            ;; host), so the dump and the live database genuinely disagree about
            ;; that one value. Tiers 2 and 3 both report it, and tier 2 is
            ;; documented as meaningless after an `:xform` for exactly this
            ;; reason. What matters here is that the mismatch is attributable to
            ;; the rewritten index path and NOT to :doc/body.
            (let [rep (m/verify conn dir)]
              (is (false? (:ok? rep)))
              (is (= [[:db/ident :idx/ft]] (mapv :unique (:diffs (:tier3 rep))))
                  "the only entity that differs is the index whose path we rewrote"))
            (finally (release! conn))))))))

;; ---------------------------------------------------------------------------
;; 8. things that are FINE — worth pinning so a future check does not break them

(deftest degenerate-and-reordered-dumps-are-handled
  (let [src (small-db!)]
    (try
      (testing "a dump with zero records imports to an empty database on both paths"
        (let [dir (dump-of src (constantly []))
              a (conn! (cfg true)) b (conn! (cfg true))]
          (try
            (is (zero? (:datom-count (:report (import-into! a dir {})))))
            (is (zero? (:datom-count (:report (import-into! b dir {:build-indexes? true})))))
            (finally (release! a b)))))

      (testing "record order is not load-bearing: a fully reversed dump restores
                the same content, and the index build reproduces the same :hash"
        (let [fwd (dump-of src)
              rev (dump-of src (comp vec reverse))
              a (conn! (cfg true)) b (conn! (cfg true))]
          (try
            (is (nil? (:threw (import-into! a fwd {:build-indexes? true}))))
            (is (nil? (:threw (import-into! b rev {:build-indexes? true}))))
            (is (= (:hash @a) (:hash @b)))
            (is (= (count (current a)) (count (current b))))
            (finally (release! a b)))))

      (testing ":chunk-size 1 — a transaction split across every possible
                boundary — round-trips on both paths"
        (let [dir (tmp-dir!)]
          (m/export-db src dir {:history? true :chunk-size 1})
          (is (< 1 (count (:chunks (manifest-of dir)))))
          (let [a (conn! (cfg true)) b (conn! (cfg true))]
            (try
              (is (true? (:verified? (:report (import-into! a dir {:batch-size 1})))))
              (is (true? (:verified? (:report (import-into! b dir {:build-indexes? true})))))
              (finally (release! a b))))))
      (finally (release! src)))))

(deftest no-history-and-cardinality-many-round-trip-exactly
  (testing ":db/noHistory (never reaches temporal) and cardinality-many (its live
            datom never reaches temporal either) are the two classes the temporal
            split has to put back. Both paths reproduce the source exactly,
            including :hash."
    (let [src (conn! (cfg true))]
      (d/transact src (conj (vec base-schema)
                            {:db/ident :note :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one :db/noHistory true}))
      (d/transact src [{:db/id -1 :name "a" :note "one" :tag :x}])
      (d/transact src [{:db/id [:name "a"] :note "two"}])
      (d/transact src [{:db/id [:name "a"] :tag :y}])
      (d/transact src [[:db/retract [:name "a"] :tag :x]])
      (d/transact src [{:db/id [:name "a"] :tag :x}])
      (let [dir (tmp-dir!)
            _ (m/export-db src dir {:history? true})
            a (conn! (cfg true)) b (conn! (cfg true))]
        (try
          (import-into! a dir {})
          (import-into! b dir {:build-indexes? true})
          (is (= (count (d/datoms @src :eavt)) (count (current a)) (count (current b))))
          (is (= (count (d/datoms (d/history @src) :eavt)) (history-count a) (history-count b)))
          (is (= (:hash @src) (:hash @a) (:hash @b)))
          (finally (release! src a b)))))))

(deftest on-error-collect-names-the-offending-record
  (testing ":on-error :collect narrows to the datom, reports :verified? false and
            does not throw — the one path where a bad record is both survivable
            and identified"
    (let [src (small-db!)
          dir (dump-of src (fn [rs] (conj (vec rs) [7 "score" 42 (+ c/tx0 4) true])))
          conn (conn! (cfg true))
          res (import-into! conn dir {:on-error :collect})]
      (try
        (is (nil? (:threw res)))
        (is (false? (:verified? (:report res))))
        (is (= [[7 "score" 42 (+ c/tx0 4) true]] (mapv :datom (:errors (:report res)))))
        (finally (release! src conn))))))
