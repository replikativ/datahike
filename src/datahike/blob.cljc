(ns datahike.blob
  "Content ids for blobs referenced by `:db.type/store-ref`.

   One function, deliberately. Datahike does not offer a `put-blob` — writing bytes
   is konserve's job (`k/bassoc`) if the object lives in the database's store, and
   your object store's job (a presigned PUT) if it does not. What datahike does offer
   is the id, because the id is where the semantics live.

   WHY THE ID MUST PIN THE CONTENT. A store-ref is dereferenced when you read it —
   including when you read it `as-of` an old transaction. If the id is a MUTABLE
   POINTER, that old read hands you the reference, you fetch it, and you get whatever
   is behind it NOW rather than what was there THEN. The database's central promise
   silently stops holding for that attribute, and nothing tells you.

   A content hash makes that impossible: the reference IS the content, so dereferencing
   an old reference necessarily yields the old bytes. It is the same reason index nodes
   are content-addressed and the branch head is the ONLY mutable cell in the store — a
   blob behind a mutable name would be a second mutable cell, and one datahike can
   neither see nor protect.

   The requirement is therefore WRITE-ONCE. Content addressing is how you guarantee it.
   (A random uuid is permitted — sometimes you cannot hash the bytes, e.g. a streaming
   upload you never buffer — but then time travel holds only for as long as you never
   rewrite that object, and you lose dedup and idempotent re-upload. Content addressing
   removes the \"for as long as\".)

   A PATH IS NEVER AN ACCEPTABLE ID: paths move, and objects at a path get overwritten.
   Store the path as an ordinary `:db.type/string` datom beside the id — that is the git
   model (blobs are addressed by content; trees map names to hashes), and it means a
   rename touches one datom while every historical reference still resolves."
  (:require [hasch.core :as hasch]))

(defn blob-id
  "The content id of `bytes` — a `hasch` uuid, suitable as a `:db.type/store-ref`
   value and as the konserve key (or object-store key) the bytes are written under.

   Content-addressed, so:
     - the same bytes always give the same id — re-uploading is idempotent, and two
       entities referencing identical content share ONE object;
     - the object is immutable, so a torn write leaves a collectable orphan rather
       than a dangling pointer;
     - an `as-of` read of an old reference yields the bytes that were there then."
  [bytes]
  (hasch/uuid bytes))
