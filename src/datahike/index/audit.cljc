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
   audit can't attest it).")

(defprotocol IAuditable
  (-merkle-root [this]
    "Content-addressed UUID of the index's current state. Cheap — post-
     flush this returns a value the underlying library already computed.
     Throws `{:type :audit/merkle-root-unsupported}` when the impl
     can't provide one (e.g. unflushed, or the storage layer doesn't
     content-address).")

  (-recompute-merkle-root [this]
    "Walk storage and re-derive the merkle root. The caller compares
     against `-merkle-root` to detect bytes-level tampering. May load
     and walk all data — used only by deep audit. The default behavior
     for storage layers that already content-address on read (e.g.
     konserve in `:crypto-hash?` mode) is to return `(-merkle-root this)`.
     Throws `{:type :audit/merkle-mismatch}` on detected corruption or
     `{:type :audit/recompute-unsupported}` when not implemented."))
