(ns datahike.middleware.utils)

(defn apply-middlewares
  "Combines a list of middleware functions into one."
  [middlewares handler]
  #?(:cljs (throw (ex-info "middleware resolution is not supported in cljs at this time" {:middlewares middlewares
                                                                                          :handler handler}))
     :clj
     (reduce
      (fn [acc f-sym]
        (if-let [f (resolve f-sym)]
          (f acc)
          (throw (ex-info "Invalid middleware.😱" {:fn f-sym}))))
      handler
      middlewares)))
