(ns datahike.test.json-list-tag-test
  "Datahike's tagged JSON keeps lists (#1145). JSON has no list type, so the
   call `(count ?v)` in a query used to reach the server as the vector
   `[count ?v]`. Lists now travel as `[\"!list\", [...]]`."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.json :as djson]
   [datahike.remote :as remote]
   [jsonista.core :as j]
   [jsonista.tagged :as jt]))

(defn- client->server
  "Encode as the JVM client does and decode as the server does."
  [x]
  (j/read-value (j/write-value-as-string x remote/json-mapper) djson/mapper))

(defn- server->client [x]
  (j/read-value (j/write-value-as-string x djson/mapper) remote/json-mapper))

(deftest lists-survive-the-round-trip
  (doseq [[label x] [["quoted list" '(count ?v)]
                     ["list fn" (list 'some? '?n)]
                     ["empty list" ()]
                     ["cons" (cons 'get '(?v 0))]
                     ["list*" (list* 'get '?v [0])]
                     ["syntax-quote" `(count ~'?v)]]]
    (testing label
      (let [out (client->server x)]
        (is (seq? out))
        (is (= x out)))
      (let [out (server->client x)]
        (is (seq? out))
        (is (= x out)))))
  (testing "a query's calls stay lists inside vectors"
    (let [q '[:find ?c . :in $ ?v :where [(count ?v) ?c] (not [(zero? ?c)])]
          out (client->server q)]
      (is (= q out))
      (is (seq? (first (nth out 7))))
      (is (seq? (nth out 8)))
      (is (seq? (first (second (nth out 8))))))))

(deftest lazy-seqs-stay-plain-arrays
  ;; Results are mostly lazy seqs, and JSON consumers other than the JVM
  ;; client (the TypeScript client, pydatahike, curl) read them as plain
  ;; arrays. They are left untagged and still decode to an equal value.
  (let [xs (map inc [1 2 3])]
    (is (= "[2,3,4]" (j/write-value-as-string xs djson/mapper)))
    (is (= xs (server->client xs)))
    (is (= [[1 2] [3 4]] (server->client (map vec (partition 2 [1 2 3 4])))))))

(deftest an-older-decoder-sees-a-two-element-vector
  ;; What a server from before the tag reads: jsonista turns an unknown tag
  ;; into a plain vector, so the call reaches the engine as ["!list" [...]].
  (let [old-mapper (j/object-mapper
                    {:decode-key-fn true
                     :modules [(jt/module {:handlers (dissoc djson/json-base-handlers
                                                             clojure.lang.PersistentList)})]})]
    (is (= ["!list" ['count '?v]]
           (j/read-value (j/write-value-as-string '(count ?v) remote/json-mapper)
                         old-mapper)))))
