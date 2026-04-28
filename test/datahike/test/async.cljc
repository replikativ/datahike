(ns datahike.test.async
  "Cross-platform `deftest-async`.

   The body is wrapped in a `clojure.core.async/go` block on both
   platforms, so `<!` works inside the body uniformly. The outer
   wait-for-completion mechanism is platform-specific:

     - CLJ:  `<!!` on the go-block's result channel.
     - CLJS: `cljs.test/async done#` + `(done#)` after the body.

   Test author writes a single body using `<!` to take from
   channel-returning APIs (`d/transact!`, `d/connect` on CLJS, the
   `setup-db` helpers in `datahike.test.utils`, etc.). Works on CLJ
   even though some of those APIs are nominally synchronous, because
   wrapping a synchronous value in `(go ...)` produces a channel that
   yields the value, and `<!` of that channel returns the value.

   Usage:

       (require '[datahike.test.async :refer [deftest-async]
                  :refer-macros [deftest-async]])
       (require '[clojure.core.async :as a :refer [go <!]
                  :refer-macros [go]])

       (deftest-async basic-roundtrip
         (let [conn (<! (utils/setup-db {:store {:backend :memory}}))]
           (<! (d/transact! conn [{:db/id -1 :name \"Ivan\"}]))
           (is (= 1 (count (d/q '[:find ?e :where [?e :name _]] @conn))))
           (<! (utils/teardown-db conn))))

   This is the test-suite analogue of konserve.utils/async+sync, but
   simpler: tests don't need a runtime sync/async toggle, only a
   compile-time platform dispatch."
  #?(:clj  (:require [clojure.test]
                     [clojure.core.async])
     :cljs (:require [cljs.test]
                     [clojure.core.async]))
  #?(:cljs (:require-macros [datahike.test.async])))

(defn- cljs-env? [env] (some? (:ns env)))

(defmacro deftest-async
  "Like `clojure.test/deftest` but wraps the body in a `go` block.
   On CLJ blocks the test thread until the go-block completes via
   `<!!`; on CLJS uses `cljs.test/async` + a `done` callback so it
   integrates with cljs.test's async runner.

   Inside the body, `<!` (from `clojure.core.async`) works uniformly
   on both platforms. Use it for any expression that may return a
   channel; for synchronous CLJ expressions a `(go x)` wrapper
   harmlessly turns `x` into a single-value channel that `<!` resolves
   immediately."
  [name & body]
  (if (cljs-env? &env)
    `(cljs.test/deftest ~name
       (cljs.test/async done#
                        (clojure.core.async/go
                          (try
                            ~@body
                            (catch :default e#
                              (cljs.test/is false (str "deftest-async error: "
                                                       (or (.-message e#) e#)
                                                       "\n"
                                                       (.-stack e#))))
                            (finally (done#))))))
    `(clojure.test/deftest ~name
       (clojure.core.async/<!!
        (clojure.core.async/go
          (try
            ~@body
            (catch Throwable e#
              (clojure.test/is false
                               (str "deftest-async error: "
                                    (.getMessage e#)
                                    "\n"
                                    (clojure.string/join
                                     "\n"
                                     (map str (.getStackTrace e#))))))))))))
