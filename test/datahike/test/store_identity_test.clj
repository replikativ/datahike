(ns datahike.test.store-identity-test
  "A store `:id` is not a label, it is a process-wide key. The connection
   registry is `[store-id branch]`, the GC guard's safe point is keyed by it, and
   so is the persistent-sorted-set storage registry. Two stores sharing one id
   therefore resolve to each other, and `d/connect` on the second config returns
   the FIRST connection — the caller reads and writes a different database than
   the one it named, silently.

   Datahike already has two consistency checks here and NEITHER can catch it,
   for the same structural reason: both reduce a store to its id.

     - `:store-identity-mismatch` compares the id a store RECORDS against the id
       you connect with. Two stores that each self-consistently hold the same id
       both pass.
     - the connection-cache comparison runs configs through `normalize-config`,
       which drops `:store` entirely, on the assumption that the id in the
       connection key already accounts for it.

   So nothing looked at WHERE THE BYTES ARE, which is the only thing that
   distinguishes the two stores."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.store :as ds]))

(def ^:private tmp (System/getProperty "java.io.tmpdir"))

(defn- cfg [name id]
  {:store {:backend :file :path (str tmp "/" name) :id id}
   :schema-flexibility :read})

(defn- collision-type [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))

(defn- clean! [& configs]
  (doseq [c configs]
    (try (when (d/database-exists? c) (d/delete-database c))
         (catch Throwable _))
    (ds/release-store-identity! (get-in c [:store :id]))))

(deftest a-second-store-cannot-claim-a-live-id
  (let [id #uuid "5c4e3a00-0000-0000-0000-000000000201"
        a (cfg "dh-sid-a" id)
        b (cfg "dh-sid-b" id)]
    (clean! a b)
    (try
      (d/create-database a)
      (testing "creating a second store under a live id is refused"
        (is (= :datahike/store-identity-collision
               (collision-type #(d/create-database b)))))

      (let [conn (d/connect a)]
        (testing "and so is connecting one — the case that used to hand back the
                  WRONG database instead of failing"
          (is (= :datahike/store-identity-collision
                 (collision-type #(d/connect b)))))

        (testing "including on a branch with no cached connection, which takes
                  the full connect path rather than the cache short-circuit"
          (is (= :datahike/store-identity-collision
                 (collision-type #(d/connect (assoc b :branch :other))))))

        (testing "the legitimate case still works: the SAME config reconnects"
          (let [again (d/connect a)]
            (is (some? again))
            (d/release again)))
        (d/release conn))
      (finally (clean! a b)))))

(deftest a-composite-store-may-share-its-id-with-its-own-layers
  (testing "An `:id` is konserve's LOGICAL identity, deliberately the same across
            the backends holding one store. A `:tiered` store shares its id with
            its `:frontend-config` and `:backend-config` by construction, and
            opening it through its durable layer — to read what actually landed
            there — presents a second config for the same store under the same
            id, differing exactly in `:backend`. That is a supported composition,
            not a collision, and the cljs tiered-storage test does it.

            So a differing `:backend` is never a conflict; only two configs
            naming the SAME backend in different places are."
    (let [id #uuid "5c4e3a00-0000-0000-0000-000000000204"
          path (str tmp "/dh-sid-tiered")
          tiered {:backend :tiered
                  :id id
                  :frontend-config {:backend :memory :id id}
                  :backend-config {:backend :file :path path :id id}}
          layer {:backend :file :path path :id id}
          conflicting {:backend :file :path (str path "-elsewhere") :id id}]
      (ds/release-store-identity! id)
      (try
        (is (nil? (collision-type #(ds/claim-store-identity! tiered))))
        (is (nil? (collision-type #(ds/claim-store-identity! layer)))
            "the durable layer is the same store, reached one level down")
        (is (= :datahike/store-identity-collision
               (collision-type #(ds/claim-store-identity! conflicting)))
            "but the same backend in a different place still collides")
        (finally (ds/release-store-identity! id))))))

(deftest deleting-a-database-frees-its-id
  (testing "an id names a store, so once the store is gone the id may name
            another. Only a deletion releases it — releasing a CONNECTION must
            not, since the store still exists."
    (let [id #uuid "5c4e3a00-0000-0000-0000-000000000202"
          a (cfg "dh-sid-c" id)
          b (cfg "dh-sid-d" id)]
      (clean! a b)
      (try
        (d/create-database a)
        (testing "releasing the connection does not free the id"
          (d/release (d/connect a))
          (is (= :datahike/store-identity-collision
                 (collision-type #(d/create-database b)))))

        (d/delete-database a)
        (testing "deleting the database does"
          (is (nil? (collision-type #(d/create-database b))))
          (let [conn (d/connect b)]
            (is (= (get-in b [:store :path])
                   (get-in @conn [:config :store :path]))
                "and the connection really is to the new store")
            (d/release conn)))
        (finally (clean! a b))))))

(deftest a-sparser-config-does-not-mask-a-later-collision
  (testing "Configs sharing a backend are MERGED, not kept first-wins. A caller
            may hold a fuller or sparser config than the one that claimed the id
            first; if they do not disagree, their union describes the store more
            completely than either.

            It matters: under first-claim-wins a sparse `{:backend :file :id X}`
            has no `:path` to disagree with, so it would let ANY later file
            config through — including one naming a different store. Merging the
            path in from the second claim is what catches the third."
    (let [id #uuid "5c4e3a00-0000-0000-0000-000000000205"
          sparse {:backend :file :id id}
          full   {:backend :file :path (str tmp "/dh-sid-merge") :id id}
          other  {:backend :file :path (str tmp "/dh-sid-merge-elsewhere") :id id}]
      (ds/release-store-identity! id)
      (try
        (is (nil? (collision-type #(ds/claim-store-identity! sparse)))
            "nothing to disagree with yet")
        (is (nil? (collision-type #(ds/claim-store-identity! full)))
            "same store, described more fully — absence is not disagreement")
        (is (= :datahike/store-identity-collision
               (collision-type #(ds/claim-store-identity! other)))
            "and the merged claim now carries the :path that catches this")
        (finally (ds/release-store-identity! id))))))

(deftest the-error-names-both-configurations
  (testing "an operator has to see WHICH two configs collided; the id alone does
            not say which one is the mistake"
    (let [id #uuid "5c4e3a00-0000-0000-0000-000000000203"
          a (cfg "dh-sid-e" id)
          b (cfg "dh-sid-f" id)]
      (clean! a b)
      (try
        (d/create-database a)
        (let [e (try (d/create-database b) nil
                     (catch clojure.lang.ExceptionInfo e e))
              {:keys [store-id claimed-by requested]} (ex-data e)]
          (is (= id store-id))
          (is (= (get-in a [:store :path]) (:path claimed-by)))
          (is (= (get-in b [:store :path]) (:path requested))))
        (finally (clean! a b))))))
