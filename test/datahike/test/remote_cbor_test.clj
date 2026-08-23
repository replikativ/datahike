(ns datahike.test.remote-cbor-test
  "The CBOR codec for the remote protocol.

  Codec-level on purpose, like `datahike.test.cbor-test`: the end-to-end
  behaviour is `datahike.test.http.server-test`'s CBOR binding, which runs the
  whole API suite over the wire. What is pinned here is what that suite cannot
  see — what the bytes contain, and what happens to a type nobody registered."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [boring.core :as boring]
            [boring.data :as bdata]
            [cognitect.transit]
            [jsonista.core]
            [datahike.datom :as dd]
            [datahike.remote :as remote]
            [datahike.remote.cbor :as rcbor]))

(def ^:private peer
  {:url "https://db.example.com" :token "SECRET-TOKEN" :backend :datahike-server})

(defn- with-peer [f] (binding [remote/*remote-peer* peer] (f)))

(defn- enc ^bytes [x reg] (boring/encode x {:registry reg}))

(defn- wire-text [x reg] (String. (enc x reg) "ISO-8859-1"))

;; ---------------------------------------------------------------------------
;; Registration completeness
;; ---------------------------------------------------------------------------

(defn- record-constructors
  "The `map->X` constructors a namespace publishes, i.e. its defrecords. Derived
  rather than listed: a record added to `datahike.remote` later is a wire type
  whether or not anyone remembers this test exists."
  [ns-sym]
  (->> (ns-publics ns-sym)
       (keep (fn [[sym v]]
               (when (str/starts-with? (name sym) "map->")
                 [(symbol (subs (name sym) 5)) v])))
       (into (sorted-map))))

(deftest every-remote-record-is-registered
  (testing "boring emits an UNREGISTERED defrecord natively, under its derived
            `ns/Record` name and with every field — including :remote-peer,
            which holds the peer's url and bearer token. So an omission here is
            not a missing feature, it is a credential in the request body. This
            enumerates the namespace instead of repeating a list, because the
            list is exactly what would go stale."
    (let [reg (rcbor/client-registry)
          ctors (record-constructors 'datahike.remote)]
      (is (seq ctors) "the probe itself must not silently find nothing")
      (with-peer
        (fn []
          (doseq [[rec-sym ctor] ctors]
            (testing (str rec-sym)
              (let [inst (ctor {:remote-peer peer})
                    name (bdata/frame-name (boring/decode (enc inst reg) {}))]
                (is (contains? rcbor/wire-names name)
                    (str rec-sym " went out as " name
                         ", which this codec does not speak — it fell through to"
                         " boring's native record emission"))
                (is (not (str/includes? (wire-text inst reg) "SECRET-TOKEN"))
                    (str rec-sym " put the peer's token on the wire"))))))))))

(deftest the-natively-derived-names-are-not-what-goes-out
  (testing "the counterpart to the check above, stated positively: the wire
            carries the LOGICAL name shared with transit, not the client's
            implementation class. A server reading `datahike/DB` has no idea
            the sender's class was `RemoteDB`, which is the whole point of one
            protocol with two ends."
    (let [reg (rcbor/client-registry)]
      (with-peer
        (fn []
          (let [db (remote/remote-db {:store-id ["sid" :db] :max-tx 536870913
                                      :max-eid 42 :commit-id nil})]
            (is (= rcbor/db-name (bdata/frame-name (boring/decode (enc db reg) {}))))
            (is (not (str/includes? (wire-text db reg) "RemoteDB")))))))))

;; ---------------------------------------------------------------------------
;; Round trips
;; ---------------------------------------------------------------------------

(deftest handles-round-trip-and-reattach-the-peer
  (testing ":remote-peer is stripped on write and restored from *remote-peer*
            on read, so equality holds without the token ever being sent."
    (let [reg (rcbor/client-registry)]
      (with-peer
        (fn []
          (let [db  (remote/remote-db {:store-id ["sid" :db] :max-tx 1 :max-eid 2 :commit-id nil})
                xs  {:connection (remote/remote-connection ["sid" :db])
                     :db         db
                     :historical (remote/remote-historical-db {:origin db})
                     :since      (remote/remote-since-db {:origin db :time-point 5})
                     :as-of      (remote/remote-as-of-db {:origin db :time-point 5})
                     ;; nested: a RemoteDB inside a RemoteEntity
                     :entity     (remote/remote-entity {:db db :eid 42})}]
            (doseq [[k x] xs]
              (testing (str k)
                (is (= x (boring/decode (enc x reg) (rcbor/decode-opts reg))))
                (is (= peer (:remote-peer (boring/decode (enc x reg) (rcbor/decode-opts reg))))
                    "the peer is reattached from the dynamic binding, not the wire")))))))))

(deftest a-datom-keeps-the-name-it-has-on-every-other-datahike-wire
  (testing "`datahike.datom/Datom`, from `datahike.cbor` — NOT transit's
            `datahike/Datom`. A Datom is the same type with the same payload at
            both ends and in konserve blobs, so it gets one name everywhere;
            the handles differ per end and so keep transit's logical names."
    (let [reg (rcbor/client-registry)
          d   (dd/datom 1 :name "Alice" 536870913 true)
          raw (boring/decode (enc d reg) {})]
      (is (= "datahike.datom/Datom" (bdata/frame-name raw)))
      (is (= [1 :name "Alice" 536870913 true] (vec (bdata/frame-payload raw))))
      (is (= d (boring/decode (enc d reg) (rcbor/decode-opts reg)))))))

;; ---------------------------------------------------------------------------
;; Size
;; ---------------------------------------------------------------------------

(deftest cbor-is-materially-smaller-than-the-formats-it-joins
  (testing "the reason the http-writer defaults to CBOR, asserted rather than
            recorded. Measured at the time of writing: 500 datoms cost 14 756 B
            against transit's 22 354 and JSON's 37 296; a 200-entity transact
            argument costs 5 496 against 7 342 and 13 909. The bounds below are
            slack enough to survive an encoder tweak and tight enough that a
            regression to transit's shape fails.

            Compared at the same VALUES rather than the same framing, which is
            the only honest comparison between formats."
    (let [reg (rcbor/client-registry)
          datoms (vec (for [i (range 500)]
                        (dd/datom (+ 100000 i)
                                  (nth [:person/name :person/age :person/email :person/city] (mod i 4))
                                  (if (even? i) (str "value-" i) (long (* i 37)))
                                  (+ 536870912 (quot i 20))
                                  true)))
          cbor-size (count (enc datoms reg))
          transit-size (let [o (java.io.ByteArrayOutputStream.)]
                         (cognitect.transit/write
                          (cognitect.transit/writer o :json {:handlers remote/transit-write-handlers})
                          datoms)
                         (count (.toByteArray o)))
          json-size (count (jsonista.core/write-value-as-bytes datoms remote/json-mapper))]
      (is (< cbor-size (* 0.8 transit-size))
          (format "cbor %d B vs transit %d B" cbor-size transit-size))
      (is (< cbor-size (* 0.55 json-size))
          (format "cbor %d B vs json %d B" cbor-size json-size)))))

;; ---------------------------------------------------------------------------
;; Failure mode
;; ---------------------------------------------------------------------------

(deftest an-unknown-record-name-is-an-error-not-a-carrier
  (testing "boring's default is to degrade an unregistered tag-27 name to a
            carrier, which is right for a dump a stranger reads partially and
            wrong for an RPC: there it means the peers disagree about the
            protocol, and a carrier defers that into an unrelated failure
            later. This is also the property that keeps a native image honest,
            since boring's reflective fallbacks fail silently under a closed
            world."
    (let [reg (rcbor/client-registry)
          alien (bdata/tagged-value 27 ["some.other/Thing" {:a 1}])
          bs (boring/encode alien {})]
      (is (thrown? Exception (boring/decode bs (rcbor/decode-opts reg))))
      (testing "and boring's own default still degrades, so this is our choice
                rather than something boring imposes"
        (is (some? (boring/decode bs {:registry reg})))))))
