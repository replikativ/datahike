(ns datahike.test.query-binding-seam-test
  "ONE law, asserted at every site that binds a variable:

     Binding a variable that is already bound is UNIFICATION, not assignment.

   An occurrence of a bound variable is an equality obligation. This is
   Datomic's semantics, and datahike already implements it for a variable
   repeated *inside* one clause (#912/#913) and for ordinary data patterns —
   `[?e :name ?v] [?e :nick ?v]` correctly selects the entities whose nick
   equals their name. It was the clause forms that BIND a value which each
   invented their own rule:

     get-else          planner ignored the obligation, base engine overwrote
     tuple binding     both overwrote
     repeated head var both produced nil
     :in constant      planner ignored the obligation

   Four bugs, one law. The tests are grouped by site rather than by engine so
   that a future site is obviously missing from the list, and each asserts an
   ABSOLUTE expected value as well as engine agreement — two engines agreeing
   on a wrong answer is exactly how these survived.

   ONE site is deliberately NOT covered, so this file is not read as an
   exhaustive enumeration: a variable repeated inside a SINGLE binding form
   (`[(vector 1 2) [?x ?x]]`) still overwrites on both engines, because the
   #912/#913 normalisation rewrites data patterns only. Both engines agree
   there, so no differential test can reach it either.

   Written with `deftest-async` and listed in `nodejs_test.cljs` so it runs on
   ClojureScript too. Being `.cljc` is NOT enough: a `.cljc` test only runs on
   cljs if the Node runner names it, and that gap is precisely why a CLJS merge
   kernel kept the pre-law behaviour while every JVM run was green — the same
   shape as #917, the twin nobody executed."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is testing]]
      :clj  [clojure.test :as t :refer [is testing]])
   [clojure.core.async :refer [<!]]
   [datahike.api :as d]
   [datahike.query :as q]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

(defn- connect!
  "Create + connect. Returns a channel so the body works on the JVM (sync) and
   on cljs (async writer) alike."
  [cfg]
  (clojure.core.async/go
    #?(:clj  (do (d/create-database cfg) (d/connect cfg))
       :cljs (do (<! (d/create-database cfg)) (<! (d/connect cfg {:sync? false}))))))

(defn- planner [query db & args]
  (binding [q/*disable-planner* false q/*query-result-cache?* false]
    (set (apply d/q query db args))))

(defn- base [query db & args]
  (binding [q/*disable-planner* true q/*query-result-cache?* false]
    (set (apply d/q query db args))))

(defn- outcome
  "The engine's answer, or ::raised — so a test can assert that BOTH engines
   decline a query rather than one of them quietly inventing a row."
  [f query db args]
  (try (apply f query db args)
       (catch #?(:clj Exception :cljs js/Error) _ ::raised)))

(defn- both [expected query db & args]
  (is (= expected (apply base query db args)) "base engine")
  (is (= expected (apply planner query db args)) "planner"))

(deftest-async binding-seam-law
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :nick :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :score :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :tag :db/valueType :db.type/keyword
                            :db/cardinality :db.cardinality/many}
                           {:db/ident :flag :db/valueType :db.type/boolean
                            :db/cardinality :db.cardinality/one}]))
    ;; carol's nick EQUALS her name: the one row a unifying engine keeps.
    ;; :tag is card-many so the group routes through the card-many merge kernel,
    ;; which is a DIFFERENT code path from the card-one one and was the last to
    ;; be fixed — on cljs it was the last of all.
    (<! (d/transact! conn [{:db/id 1 :name "alice" :nick "al"    :score 20 :tag [:x :y] :flag true}
                           {:db/id 2 :name "carol" :nick "carol" :score 30 :tag [:x]}
                           {:db/id 3 :name "dave"                :score 10 :tag [:y]}]))
    (let [db (d/db conn)]

      (testing "pattern occurrence — the case that was ALREADY correct"
        (both #{[2 "carol"]}
              '[:find ?e ?v :where [?e :name ?v] [?e :nick ?v]] db))

      (testing "a get-else writing into a bound var constrains it"
        (both #{[2 "carol"]}
              '[:find ?e ?v :where [?e :name ?v] [(get-else $ ?e :nick "zzz") ?v]] db)
        ;; the default DOES satisfy the obligation when it matches: dave has no
        ;; nick so "dave" is compared, and carol still qualifies on her real one
        (both #{[2 "carol"] [3 "dave"]}
              '[:find ?e ?v :where [?e :name ?v] [(get-else $ ?e :nick "dave") ?v]] db))

      (testing "a real value sorting BEFORE the obligated one is still present"
        ;; the presence probe used to seek at the obligated value, so alice's
        ;; "al" was skipped, the attribute read as absent, and the default was
        ;; planted — keeping a row whose real nick disagrees
        (both #{[3 "zzz"]}
              '[:find ?e ?v :in $ ?v :where [?e :name _] [(get-else $ ?e :nick "zzz") ?v]]
              db "zzz"))

      (testing "the same, through the card-many merge kernel"
        (both #{[3 "zzz"]}
              '[:find ?e ?v :in $ ?v :where [?e :name _] [?e :tag _] [(get-else $ ?e :nick "zzz") ?v]]
              db "zzz"))

      (testing "a get-else on a card-many attribute is still single-valued"
        ;; `get-else` returns ONE value per entity — the entity's first datom
        ;; for the attribute — whatever the attribute's cardinality. Judging the
        ;; obligation by seeking AT the obligated value instead asks "does SOME
        ;; datom equal it", so alice, whose tags are #{:x :y}, was admitted for
        ;; :y as well as :x — a row her own `get-else` would never produce.
        (both #{[1 :x] [2 :x]}
              '[:find ?e ?v :in $ ?v :where [?e :name _] [(get-else $ ?e :tag :zz) ?v]]
              db :x)
        (both #{[3 :y]}
              '[:find ?e ?v :in $ ?v :where [?e :name _] [(get-else $ ?e :tag :zz) ?v]]
              db :y))

      (testing "the bound value may come from :in rather than a pattern"
        (both #{[2 "carol"]}
              '[:find ?e ?v :in $ ?v :where [?e :name _] [(get-else $ ?e :nick "zzz") ?v]]
              db "carol"))

      (testing "an obligation whose value is FALSE is still an obligation"
        ;; `false` was read as "no constant" wherever presence was tested by
        ;; truthiness, and the obligation vanished: the folded-constant filter
        ;; admitted the entities whose flag is TRUE, and a false scalar `:in`
        ;; reached a function as nil.
        (both #{[2] [3]}
              '[:find ?e :in $ ?v :where [?e :name _] [(get-else $ ?e :flag false) ?v]]
              db false)
        (both #{[1]}
              '[:find ?e :in $ ?f :where [?e :flag ?v] [(= ?v ?f)]] db true))

      (testing "a plain function output writing into a bound var constrains it"
        (both #{[3 10]}
              '[:find ?e ?s :where [?e :score ?s] [(+ 5 5) ?s]] db))

      (testing "each slot of a tuple binding is its own obligation"
        (both #{[3 "dave" 10]}
              '[:find ?e ?n ?s :where
                [?e :name ?n] [?e :score ?s] [(vector ?n 10) [?n ?s]]] db))

      (testing "a collection binding filters rather than overwrites"
        (both #{[1 20] [3 10]}
              '[:find ?e ?s :where
                [?e :score ?s] [(identity [10 20]) [?s ...]]] db))

      (testing "a variable repeated in a rule HEAD binds one value, not nil"
        (both #{[20 20] [30 30] [10 10]}
              '[:find ?x ?y :in $ % :where (same ?x ?y)]
              db '[[(same ?a ?a) [?e :score ?a]]]))

      (testing "a rule head obligation the caller collapses is not an error"
        ;; substitution turns the appended [(identity ?a) ?a__1] into
        ;; [(identity ?e) ?e], which the planner cannot resolve inside not-join
        ;; — it used to raise "Cannot resolve any more clauses" here. No entity
        ;; id equals a score, so `(same ?e ?e)` matches nothing, the negation
        ;; excludes nothing, and every entity is returned. ANSWERING is the
        ;; property under test.
        (both #{[1] [2] [3]}
              '[:find ?e :in $ % :where [?e :score _] (not-join [?e] (same ?e ?e))]
              db '[[(same ?a ?a) [?e :score ?a]]]))

      (testing "a tautology the USER wrote is not covered by that drop"
        ;; the clause above is dropped because SUBSTITUTION collapsed it. This
        ;; one is already `[(identity ?x) ?x]` as written, and dropping it would
        ;; leave ?x unbound and answer #{[nil]} — inventing a row out of a query
        ;; neither engine can resolve. Declining is the agreed answer.
        (let [query '[:find ?x :in $ % :where (t ?x)]
              rules '[[(t ?x) [(identity ?x) ?x]]]]
          (is (= ::raised (outcome base query db [rules])) "base engine")
          (is (= ::raised (outcome planner query db [rules])) "planner"))))))

(deftest-async binding-seam-law-temporal
  ;; The temporal kernel is a SEPARATE merge kernel from the current-db ones,
  ;; and each temporal view (`history`, `as-of`, `since`) takes a different
  ;; branch through it. The law has to hold on all of them: `get-else` is total
  ;; over entities on every view, so "present but disagreeing" must exclude the
  ;; row there too. Two ways it failed here: the presence probe was asked over a
  ;; slice already bounded by the obligated value — which can only ever answer
  ;; about that ONE value, making the check a no-op — and the whole `:cljs` half
  ;; of the kernel kept planting defaults unconditionally while every JVM run
  ;; was green. That twin-nobody-executes shape is #917 exactly, which is why
  ;; these run on both platforms.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :nick :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}]))
    (<! (d/transact! conn [{:db/id 1 :name "a" :nick "aaa"}
                           {:db/id 2 :name "b" :nick "bbb"}
                           {:db/id 3 :name "c" :nick "ccc"}
                           {:db/id 4 :name "d"}]))
    (let [db (d/db conn)
          ;; only 4 has no nick, so only 4 may take the default — the other
          ;; three are PRESENT with a value that disagrees with "zzz"
          query '[:find ?e ?v :in $ ?v :where
                  [?e :name _] [(get-else $ ?e :nick "zzz") ?v]]]
      (testing "current"   (both #{[4 "zzz"]} query db "zzz"))
      (testing "history"   (both #{[4 "zzz"]} query (d/history db) "zzz"))
      (testing "as-of"     (both #{[4 "zzz"]} query (d/as-of db (:max-tx db)) "zzz"))
      (testing "since"     (both #{[4 "zzz"]} query (d/since db 0) "zzz")))))

(deftest-async binding-seam-law-history-versions
  ;; `history` holds EVERY version of a card-one attribute, so it is the view
  ;; where "the value `get-else` returns" and "some value the attribute ever
  ;; had" come apart. The obligation is about the first — asking it of every
  ;; version admits a row the query's own `get-else` would never produce.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :nick :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}]))
    (<! (d/transact! conn [{:db/id 1 :name "n1b" :nick "n1"}
                           {:db/id 2 :name "n1"  :nick "n1"}
                           {:db/id 3 :name "zzz"}]))
    ;; second versions: entity 1's nick BECOMES its name, entity 2's stops
    ;; being it. Only the first version of each counts.
    (<! (d/transact! conn [{:db/id 1 :nick "n1b"} {:db/id 2 :nick "n1b"}]))
    (let [db (d/db conn)
          query '[:find ?e ?v :where [?e :name ?v] [(get-else $ ?e :nick "zzz") ?v]]]
      ;; current: entity 1's nick IS "n1b" now, and 3 takes the default
      (testing "current" (both #{[1 "n1b"] [3 "zzz"]} query db))
      ;; history: entity 1's first nick is "n1", which is not its name — so it
      ;; is excluded even though a LATER version would have matched. Entity 2's
      ;; first nick "n1" is its name, so it stays.
      (testing "history" (both #{[2 "n1"] [3 "zzz"]} query (d/history db)))
      (testing "as-of"   (both #{[1 "n1b"] [3 "zzz"]} query (d/as-of db (:max-tx db))))
      (testing "since"   (both #{[1 "n1b"] [3 "zzz"]} query (d/since db 0))))))

(defn- bytes-of [xs]
  #?(:clj (byte-array xs) :cljs (js/Int8Array. (clj->js xs))))

(defn- floats-of [xs]
  #?(:clj (float-array xs) :cljs (js/Float32Array. (clj->js xs))))

(defn- doubles-of [xs]
  #?(:clj (double-array xs) :cljs (js/Float64Array. (clj->js xs))))

(deftest-async binding-seam-law-value-arrays
  ;; Byte, float and double arrays are VALUES in datahike — `datahike.array/a=`
  ;; is what decides that — but Clojure's `=` compares them by identity. Every
  ;; equality the engine makes about values therefore has to be array-aware,
  ;; and unification is one of them. Four seams, because each had its own
  ;; comparison and they disagreed in BOTH directions: the base engine dropped
  ;; every byte-valued unification while the planner kept it, and the planner
  ;; dropped every float-valued one while the base engine kept it.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :anchor :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :blob :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :blob2 :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :fa :db/valueType :db.type/float-array
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :fa2 :db/valueType :db.type/float-array
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :hi :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :hi2 :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :nan :db/valueType :db.type/double-array
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :nan2 :db/valueType :db.type/double-array
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :blob3 :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}]))
    ;; entity 1 holds equal CONTENT in both attributes of each pair, in
    ;; distinct array objects; entity 2 does not
    (<! (d/transact! conn [{:db/id 1 :anchor 1
                            :blob (bytes-of [1 2 3]) :blob2 (bytes-of [1 2 3])
                            :fa (floats-of [1.5 2.5]) :fa2 (floats-of [1.5 2.5])}
                           {:db/id 2 :anchor 2
                            :blob3 (bytes-of [1 2 3])
                            :blob (bytes-of [1 2 3]) :blob2 (bytes-of [9 9 9])
                            :fa (floats-of [1.5 2.5]) :fa2 (floats-of [9.5 9.5])}
                           ;; bytes above 0x7f, and a NaN
                           {:db/id 3 :anchor 3
                            :hi (bytes-of [-1 -128 5]) :hi2 (bytes-of [-1 -128 5])
                            :nan (doubles-of [##NaN 1.5]) :nan2 (doubles-of [##NaN 1.5])}]))
    (let [db (d/db conn)]

      (testing "an optional merge obligation — byte arrays"
        (both #{[1]}
              '[:find ?e :where [?e :blob ?v] [(get-else $ ?e :blob2 "x") ?v]] db))

      (testing "an optional merge obligation — float arrays"
        (both #{[1]}
              '[:find ?e :where [?e :fa ?v] [(get-else $ ?e :fa2 "x") ?v]] db))

      (testing "the obligated value may be an :in constant"
        (both #{[1] [2]}
              '[:find ?e :in $ ?v :where [?e :anchor _] [(get-else $ ?e :blob "x") ?v]]
              db (bytes-of [1 2 3])))

      (testing "a function output unifying with a bound array"
        (both #{[1]}
              '[:find ?e :where [?e :blob ?v] [?e :blob2 ?w] [(identity ?w) ?v]] db))

      (testing "an ordinary pattern occurrence, for comparison"
        (both #{[1]}
              '[:find ?e :where [?e :blob ?v] [?e :blob2 ?v]] db))

      (testing "a join ACROSS entity groups, which the planner hashes separately"
        ;; the same-entity case above fuses into ONE group and never reaches a
        ;; hash-probe join. This one does — and the planner joins through
        ;; `relation.cljc`'s key fn while the base engine went through
        ;; `query.cljc`'s, so fixing one copy left the DEFAULT engine returning
        ;; nothing for any array-valued join.
        ;; entities 1 AND 2 both hold :blob [1 2 3]; only 2 holds :blob3
        (both #{[1 2] [2 2]}
              '[:find ?e ?f :where [?e :blob ?v] [?f :blob3 ?v]] db)
        ;; and the same value arriving as a COLLECTION binding
        (both #{[1] [2]}
              '[:find ?e :in $ [?v ...] :where [?e :blob ?v]] db [(bytes-of [1 2 3])])
        ;; A join key may not depend on PRINT settings. The ClojureScript key
        ;; has to be a primitive — `js/Set`/`js/Map` compare objects by
        ;; identity — and building it with `pr-str` made it honour
        ;; `*print-length*`, so under a caller's binding every array printed
        ;; the same prefix and unequal values collided.
        (binding [*print-length* 1 *print-level* 1]
          (both #{[1 2] [2 2]}
                '[:find ?e ?f :where [?e :blob ?v] [?f :blob3 ?v]] db)
          (both #{}
                '[:find ?e ?f :where [?e :blob ?v] [?f :blob2 ?v]
                  [(= ?e 999)]] db)))

      (testing "the values where the key representation disagreed with a="
        ;; A join key has to hash BY VALUE over the whole domain. It did not:
        ;; a byte from 0x80 up threw building the key, two NaNs hashed apart
        ;; though `a=` calls them equal, and -0.0 hashed together with 0.0
        ;; though `a=` separates them. Entity 3 carries all three.
        (both #{[3]}
              '[:find ?e :where [?e :hi ?v] [?e :hi2 ?v]] db)
        (both #{[3]}
              '[:find ?e :where [?e :nan ?v] [?e :nan2 ?v]] db)
        (both #{[3]}
              '[:find ?e :where [?e :hi ?v] [(get-else $ ?e :hi2 "x") ?v]] db)))))

(deftest-async binding-seam-law-history-value-order
  ;; `get-else` returns the FIRST datom `search` yields, and on `history` that
  ;; is first in EAVT order — by VALUE, not the earliest transaction. The two
  ;; coincide whenever the older value also sorts first, so this fixture makes
  ;; them disagree: the newer value sorts BEFORE the older one. An
  ;; "earliest version" implementation would answer "old" here.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :nick :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :anchor :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}]))
    (<! (d/transact! conn [{:db/id 1 :anchor 1 :nick "old"}]))
    (<! (d/transact! conn [{:db/id 1 :nick "new"}]))
    (let [db (d/db conn)
          ;; the `[?e :anchor _]` pattern is what makes this reach the temporal
          ;; fused merge kernel. With the get-else alone and `?e` bound through
          ;; `:in`, the planner treats it as a standalone optional scan and
          ;; routes it through `bind-by-fn`/legacy `-get-else` — so the test
          ;; would pass on a merge kernel that took the earliest version.
          query '[:find ?v :where [?e :anchor _] [(get-else $ ?e :nick "D") ?v]]]
      (testing "history takes the EAVT-first version, not the earliest"
        (both #{["new"]} query (d/history db)))
      (testing "current is unambiguous" (both #{["new"]} query db)))))
