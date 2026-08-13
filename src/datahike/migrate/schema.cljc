(ns ^:no-doc datahike.migrate.schema
  "Malli schemas for the migration entry points, and the one check that uses
   them.

   ## Why schemas rather than hand-written checks

   `datahike.api/register-api-schemas!` states the house position: schemas are
   REGISTERED so `malli.instrument/instrument!` can find them, and datahike does
   not instrument itself — a user who never calls the instrumenter pays nothing
   and sees exactly the behaviour they always did. These schemas follow that,
   which is also what makes them cheap to move into `api-specification` if
   `export-db`/`import-db` ever gain language bindings.

   ## Why a runtime check as well, and why it is not the same thing

   Registration only helps a caller who instruments. An operator running a
   nightly backup does not, and for one option that difference is a data
   incident rather than a wrong result: `export-db`'s `:xform` docstring names
   per-tenant dump splitting as the motivating use, and `{:xfrom …}` was
   silently dropped — the export SUCCEEDED, the manifest said
   `:transformed? false`, `verify` said `:ok? true`, and the dump held every
   tenant. Absent and misspelled were indistinguishable, and absent means
   \"export everything\".

   So `validate-opts!` runs on every call. It is affordable precisely here: an
   export or import runs for seconds to minutes, so one schema check at the
   entry is unmeasurable — this is not a hot path, and the same check on one
   would be indefensible.

   ## Open maps, not closed ones

   A wrong VALUE under a known key is refused: that is unambiguous misuse.
   An UNKNOWN key is reported, not refused — closing the map would break a
   caller forwarding opts from a newer datahike, and rigidity there buys
   nothing. The near-miss suggestion is what makes the report actionable, and
   it is computed from the schema's own entries rather than a second list that
   could drift from it."
  (:require [clojure.string :as str]
            [malli.core :as m]
            [replikativ.logging :as log]))

;; ---------------------------------------------------------------------------
;; Shared option fragments
;;
;; Spelled once and merged, so `:chunk-size` cannot come to mean one thing on
;; export and another on import — the failure `datahike.migrate.store`'s own
;; comments record for chunk NAMES, one level up.
;; ---------------------------------------------------------------------------

(def ^:private common-opts
  [[:sync?       {:optional true} :boolean]
   [:progress-fn {:optional true} fn?]])

(def ^:private stream-opts
  ;; `:xform` is here because the shared implementation receives it — but
  ;; `export-db` refuses it at its own entry and `export-transformed` supplies
  ;; it positionally, so no CALLER reaches this key through an opts map. See
  ;; `export-transformed`'s docstring for why the transform is not an option.
  [[:chunk-size  {:optional true} pos-int?]
   [:sort-buffer {:optional true} pos-int?]
   [:xform       {:optional true} fn?]
   [:history?    {:optional true} :boolean]])

(def ExportOpts
  (into [:map] (concat common-opts stream-opts
                       ;; `:compression` is declared but NOT typed. It has a
                       ;; purpose-built guard — `assert-codec-supported!` — that
                       ;; says more than a schema can: it names the offending
                       ;; value, and it refuses an explicit `nil` on the grounds
                       ;; that the defaults are merged BEFORE it runs, so nil is
                       ;; a caller overriding `:gzip` rather than an absent key.
                       ;; Typing it `:keyword` here would pre-empt that with a
                       ;; worse message. The key is still LISTED so it is not
                       ;; reported as unknown.
                       [[:compression {:optional true} :any]
                        [:sort?       {:optional true} :boolean]])))

(def SinkOpts
  ;; No `:compression` and no `:sort?`: a sink writes no bytes, and `:sort?
  ;; false` is refused outright. Declaring them absent is what makes passing
  ;; them a reported near-miss rather than a silent no-op.
  (into [:map] (concat common-opts stream-opts)))

(def ImportOpts
  (into [:map]
        (concat common-opts
                [[:batch-size       {:optional true} pos-int?]
                 [:xform            {:optional true} fn?]
                 [:verify?          {:optional true} :boolean]
                 [:on-error         {:optional true} [:enum :abort :collect]]
                 [:checksums        {:optional true} [:enum :require :skip]]
                 [:merge?           {:optional true} :boolean]
                 [:eids             {:optional true} [:or [:enum :allocate :offset :preserve]
                                                      [:map-of :any :any] fn?]]
                 [:build-indexes?   {:optional true} :boolean]
                 [:schema           {:optional true} [:maybe [:map-of :any :any]]]
                 [:source-meta      {:optional true} [:maybe [:map-of :any :any]]]
                 [:allow-partial?   {:optional true} :boolean]
                 [:check-refs?      {:optional true} :boolean]
                 [:dangling-sample  {:optional true} nat-int?]
                 [:validate-records? {:optional true} :boolean]
                 [:count-source?    {:optional true} :boolean]
                 [:max-pending      {:optional true} pos-int?]
                 [:spool-chunk-size {:optional true} pos-int?]
                 [:spool-codec      {:optional true} :keyword]])))

(def EstimateOpts
  (into [:map] [[:batch-size {:optional true} pos-int?]]))

;; ---------------------------------------------------------------------------
;; The check
;; ---------------------------------------------------------------------------

(defn- edit-distance
  "Small Levenshtein, only ever run on a caller's mistake — so its cost is
   bounded by the number of unknown keys, which is normally zero."
  [a b]
  (let [m (count a) n (count b)]
    (loop [i 0 prev (vec (range (inc n)))]
      (if (= i m)
        (peek prev)
        (recur (inc i)
               (loop [j 0 row [(inc i)]]
                 (if (= j n)
                   row
                   (recur (inc j)
                          (conj row (min (inc (peek row))
                                         (inc (nth prev (inc j)))
                                         (+ (nth prev j)
                                            (if (= (nth a i) (nth b j)) 0 1))))))))))))

(defn- near-miss [k known]
  (let [ks (name k)]
    (->> known
         (map (fn [c] [c (edit-distance ks (name c))]))
         (filter (fn [[_ d]] (<= d 2)))
         (sort-by second)
         ffirst)))

(defn validate-opts!
  "Refuse a bad VALUE under a known key; report an UNKNOWN key.

   Returns `opts` so it can sit in a threading position."
  [schema opts who]
  (when (some? opts)
    (when-not (map? opts)
      (throw (ex-info (str who " takes an options MAP; got " (pr-str (type opts)) ".")
                      {:error :migrate/invalid-opts :fn who})))
    (when-let [err (m/explain schema opts)]
      (throw (ex-info (str who ": invalid option value — "
                           (str/join "; "
                                     (for [{:keys [in value]} (:errors err)]
                                       (str (pr-str (first in)) " = " (pr-str value)))))
                      {:error :migrate/invalid-opts :fn who
                       :explain (m/form schema) :errors (:errors err)})))
    (let [known (set (map first (m/children schema)))]
      (doseq [k (remove known (keys opts))]
        (log/warn :datahike/unknown-migrate-option
                  (cond-> {:fn who :option k}
                    (near-miss k known) (assoc :did-you-mean (near-miss k known)))))))
  opts)
