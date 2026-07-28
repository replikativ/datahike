(ns datahike.query.plan-check
  "SEAM for plan-level invariant checking. Ships; the checker does not.

   The only thing production needs is a place for a checker to attach: the
   lowering pass calls `maybe-check!` with each plan it builds, and that is a
   no-op unless something has installed a function.

   The checker itself — `datahike.test.query-eqcheck`, which verifies
   implied-equalities(plan) == enforced-equalities(plan) — lives under `test/`
   deliberately. It is a hand-derived MODEL of `datahike.query.execute`, so it
   must be updated whenever an enforcement mechanism there changes, and it
   already drifted once inside the PR that introduced it. A model of the executor
   is a maintenance liability, and shipping one to library users invites exactly
   that drift with no way for them to act on it. Its value is as a CI gate on our
   own invariant, and CI has `test/` on the classpath.

   To use it, require the test namespace — installing the checker is one of its
   load-time effects — and bind `*check-plan*`."
  (:refer-clojure :exclude [test]))

(def ^:dynamic *check-plan*
  "Function of one plan, called for every plan the lowering pass builds, or nil.
   Installed by `datahike.test.query-eqcheck`; nil in a production build, which
   makes `maybe-check!` a single nil test.

   `datahike.query/get-or-create-plan` also consults this: the check runs on plan
   CREATION and plans are LRU-cached, so a warm cache would mean a bound checker
   silently examined nothing."
  nil)

(defn maybe-check!
  "Hand `plan` to the installed checker, if any. Returns `plan` either way."
  [plan]
  (when-let [f *check-plan*]
    (f plan))
  plan)
