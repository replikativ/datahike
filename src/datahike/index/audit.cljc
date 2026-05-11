(ns datahike.index.audit
  "Shared `IAuditable` protocol for both primary and secondary indexes
   that participate in datahike's audit chain.

   An index that extends `IAuditable` exposes a content-addressed merkle
   root for its current state. Datahike folds those roots into the
   commit-id under `:crypto-hash?` so pointer tampering on stored
   commits is detected by cid recomputation, and supports a deeper
   `(verify-chain db {:deep? true})` mode that re-derives each index's
   root from storage to detect bytes-level tampering.

   New index implementations only need to extend `IAuditable` to opt
   in. Existing impls without the extension are flagged `:advisory` by
   the auditor (their state still gets folded into the commit, but
   audit can't attest it).

   Conventions across replikativ repos (datahike, stratum):
   the protocol shape and result-map keys are intentionally identical so
   bridges (e.g. datahike's stratum secondary) can pass results through
   without translation.")

(defprotocol IAuditable
  (-merkle-root [this]
    "Cheap. Returns the cached/known content-addressed UUID of this
     thing's current state — post-flush this is a value the underlying
     library already computed. Returns `nil` when no root is available
     (e.g. unflushed, or storage layer doesn't content-address). Never
     throws.")

  (-recompute-merkle-root [this]
    "Expensive. Walks the underlying storage and re-derives the merkle
     root, asserting that every reachable node's bytes really hash back
     to its address. Returns a result map; never throws on a detected
     mismatch:

       {:status :ok          :root <uuid>}
       {:status :mismatch    :root <recomputed-uuid?>
                             :errors [{:address, :expected, :recomputed,
                                       :node-class, :type}]}
       {:status :unsupported :reason <kw>}

     Use `:status` for branching. `:errors` carries one entry per
     anomaly the walker found (mismatch, missing node, or unexpected
     node class). Konserve does not verify content on read, so impls
     must do the walking themselves."))
