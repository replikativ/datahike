(ns hitchhiker.tree.backend)

(defprotocol IBackend
  (-new-session [backend] "Returns a session object that will collect stats")
  (-write-node [backend node session] "Writes the given node to storage, returning a go-block with its assigned address")
  (-anchor-root [backend node] "Tells the backend this is a temporary root")
  (-delete-addr [backend addr session] "Deletes the given addr from storage"))
