(ns ^:no-doc datahike.connections)

(def ^:dynamic *connections* (atom {}))

(defn get-connection [conn-id]
  (when-let [conn (get-in @*connections* [conn-id :conn])]
    (swap! *connections* update-in [conn-id :count] inc)
    conn))

(defn add-connection! [conn-id conn]
  (swap! *connections* assoc conn-id {:conn conn :count 1}))

(defn delete-connection! [conn-id]
  (when-let [conn (get-connection conn-id)]
    (reset! conn :released)
    (swap! *connections* dissoc conn-id)))
