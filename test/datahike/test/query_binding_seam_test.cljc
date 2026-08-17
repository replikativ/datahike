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

(deftest-async binding-seam-law-byte-arrays
  ;; A byte array is a VALUE in datahike, and Clojure's `=` compares byte
  ;; arrays by identity — so an obligation between two equal-content arrays
  ;; failed and the row was dropped. Every equality the engine makes about
  ;; values has to be array-aware, unification included.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}
        conn (<! (connect! cfg))]
    (<! (d/transact! conn [{:db/ident :blob :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :blob2 :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}]))
    (<! (d/transact! conn [{:db/id 1 :blob #?(:clj (byte-array [1 2 3]) :cljs (js/Int8Array. #js [1 2 3]))
                            :blob2 #?(:clj (byte-array [1 2 3]) :cljs (js/Int8Array. #js [1 2 3]))}
                           {:db/id 2 :blob #?(:clj (byte-array [1 2 3]) :cljs (js/Int8Array. #js [1 2 3]))
                            :blob2 #?(:clj (byte-array [9 9 9]) :cljs (js/Int8Array. #js [9 9 9]))}]))
    (let [db (d/db conn)]
      ;; only entity 1 holds the same bytes in both attributes
      (both #{[1]}
            '[:find ?e :where [?e :blob ?v] [(get-else $ ?e :blob2 "x") ?v]] db))))
