# GC audit issue inventory

Captured 2026-09-30 using the GitHub API. This is a metadata inventory; tracker status is not a reproduction verdict. See [the audit](gc-memory-model.md) for the focused body/comment and code review. All open issues are listed; closed issues are restricted to the last 90 days.

## Open issues

| Issue | Author | Updated |
| --- | --- | --- |
| [#1095 — Trim unnecessary runtime dependencies?](https://github.com/replikativ/datahike/issues/1095) | Ramblurr | 2026-09-24 |
| [#972 — Two attribute-refs rename loose ends: the old ident stays a live alias, and two validators disagree by spelling](https://github.com/replikativ/datahike/issues/972) | whilo | 2026-08-20 |
| [#971 — An Error (not Exception) during import is swallowed, producing a successful-looking report](https://github.com/replikativ/datahike/issues/971) | whilo | 2026-08-20 |
| [#964 — Deterministic query fuel: budget a query the way `:cancel` stops one](https://github.com/replikativ/datahike/issues/964) | whilo | 2026-08-17 |
| [#963 — Query plan cache is keyed without data, so plans built from data-dependent statistics go stale (0.8.1777)](https://github.com/replikativ/datahike/issues/963) | whilo | 2026-08-17 |
| [#960 — GC under Lambda freeze: a suspended collector can sweep with a stale mark](https://github.com/replikativ/datahike/issues/960) | whilo | 2026-08-17 |
| [#958 — `(d/sync conn T)` support](https://github.com/replikativ/datahike/issues/958) | theronic | 2026-08-16 |
| [#952 — Generalize IColumnarAggregate routing beyond stratum, and support external-engine/document-path columns](https://github.com/replikativ/datahike/issues/952) | whilo | 2026-08-13 |
| [#951 — Online GC is unsafe with diff-buf: the freed-address stream is only sound for a linear history](https://github.com/replikativ/datahike/issues/951) | whilo | 2026-08-09 |
| [#948 — sec/vt-aware? answers false for the whole duration of any write](https://github.com/replikativ/datahike/issues/948) | whilo | 2026-08-04 |
| [#947 — Stratum SCD2: a new version row is built only from that transaction's assertions, not merged from the open row](https://github.com/replikativ/datahike/issues/947) | whilo | 2026-08-04 |
| [#943 — :hash is inconsistent from birth on :attribute-refs? databases](https://github.com/replikativ/datahike/issues/943) | whilo | 2026-08-04 |
| [#938 — Value-free scalar :in bindings are dropped inside not-join / or bodies (silent wrong results on the planner)](https://github.com/replikativ/datahike/issues/938) | whilo | 2026-08-02 |
| [#878 — Writer fencing: make a stale writer's commit FAIL, not clobber (conditional head write)](https://github.com/replikativ/datahike/issues/878) | whilo | 2026-07-13 |
| [#736 — Expose Datahike Server Namespace](https://github.com/replikativ/datahike/issues/736) | alekcz | 2025-09-16 |
| [#731 — \[Bug\]: Infinite hang when connecting to sqlite again](https://github.com/replikativ/datahike/issues/731) | Ramblurr | 2026-07-02 |
| [#728 — Rebuild datahike index](https://github.com/replikativ/datahike/issues/728) | alekcz | 2025-04-01 |
| [#718 — Use transaction log](https://github.com/replikativ/datahike/issues/718) | whilo | 2024-11-03 |
| [#717 — \[Bug\]: Transaction report returns datom attributes as longs with attribute refs](https://github.com/replikativ/datahike/issues/717) | whilo | 2024-10-10 |
| [#633 — \[Bug\]: datahike.migrate has a problem with schema/double (which cbor converts to float)](https://github.com/replikativ/datahike/issues/633) | awb99 | 2023-07-28 |
| [#622 — Extend integration-test namespace ](https://github.com/replikativ/datahike/issues/622) | jsmassa | 2023-03-24 |
| [#619 — Clojure backward compatibility](https://github.com/replikativ/datahike/issues/619) | jsmassa | 2023-03-27 |
| [#616 — \[Bug\]: delete-database throws NPE for :file backend](https://github.com/replikativ/datahike/issues/616) | jjttjj | 2023-03-26 |
| [#605 — Add generative tests at least for API namespace](https://github.com/replikativ/datahike/issues/605) | jsmassa | 2023-07-24 |
| [#604 — Run benchmark tests in ci pipeline](https://github.com/replikativ/datahike/issues/604) | jsmassa | 2023-03-13 |
| [#600 — Add examples for q with varargs](https://github.com/replikativ/datahike/issues/600) | jsmassa | 2023-01-31 |
| [#579 — \[Bug\]: Pull with attr-name not in schema differs from Datomic](https://github.com/replikativ/datahike/issues/579) | KsVlad | 2022-11-10 |
| [#565 — \[Bug\]: Removed attributes still present in the index](https://github.com/replikativ/datahike/issues/565) | kordano | 2022-10-21 |
| [#552 — \[Bug\]:  "0.5.1506"  cannot import dump from "0.4.1480"](https://github.com/replikativ/datahike/issues/552) | awb99 | 2022-10-06 |
| [#551 — \[Bug\]: java.lang.IllegalArgumentException](https://github.com/replikativ/datahike/issues/551) | awb99 | 2023-03-16 |
| [#549 — Can't query by attribute :db/id instead of :db/ident](https://github.com/replikativ/datahike/issues/549) | Peluko | 2022-06-29 |
| [#548 — clean up project files](https://github.com/replikativ/datahike/issues/548) | kordano | 2022-09-06 |
| [#547 — \[Bug\]: Java unittests fail sometimes on circleci ](https://github.com/replikativ/datahike/issues/547) | kordano | 2022-06-24 |
| [#545 — Improve examples](https://github.com/replikativ/datahike/issues/545) | TimoKramer | 2023-12-06 |
| [#541 — \[Bug\]: Hitchhiker-tree index repeatedly retracts datom with re-upserted value instead of latest value](https://github.com/replikativ/datahike/issues/541) | yflim | 2022-05-30 |
| [#534 — \[Bug\]: Datahike implements `clojure.lang.Counted` in linear time](https://github.com/replikativ/datahike/issues/534) | phronmophobic | 2022-05-30 |
| [#533 — \[Bug\]: Datom input order is generally not registered within a transaction causing complicated upserts](https://github.com/replikativ/datahike/issues/533) | jsmassa | 2022-05-30 |
| [#532 — \[Bug\]: schema updates and removals are not consistent](https://github.com/replikativ/datahike/issues/532) | kordano | 2022-05-30 |
| [#531 — \[Bug\]: restore operation fails (with no transaction history)](https://github.com/replikativ/datahike/issues/531) | awb99 | 2022-10-24 |
| [#528 — Stress testing](https://github.com/replikativ/datahike/issues/528) | jsmassa | 2022-05-03 |
| [#524 — Datom comparator refactoring](https://github.com/replikativ/datahike/issues/524) | jsmassa | 2022-04-27 |
| [#520 — Better test coverage for different database configurations](https://github.com/replikativ/datahike/issues/520) | jsmassa | 2022-05-30 |
| [#510 — \[Bug\]: Behaviour of tuple with :db.unique/identity differs from Datomic (can't update an entity by transact)](https://github.com/replikativ/datahike/issues/510) | KsVlad | 2022-05-30 |
| [#508 — \[Bug\]: Future migrations for attribute-references](https://github.com/replikativ/datahike/issues/508) | jsmassa | 2022-04-11 |
| [#507 — \[Bug\]: Identical behavior for hitchhiker-tree and persistent-set index?](https://github.com/replikativ/datahike/issues/507) | jsmassa | 2022-05-05 |
| [#497 — Support for aliases](https://github.com/replikativ/datahike/issues/497) | MrEbbinghaus | 2022-05-30 |
| [#482 — \[Bug\]: Symbols not compared correctly in queries](https://github.com/replikativ/datahike/issues/482) | zoren | 2022-02-22 |
| [#465 — \[Bug\]: init max-ent-id from index if needed](https://github.com/replikativ/datahike/issues/465) | whilo | 2022-02-11 |
| [#449 — \[Bug\]: incomplete config documentation](https://github.com/replikativ/datahike/issues/449) | kordano | 2022-03-09 |
| [#448 — \[Bug\]: not-join variables not respected](https://github.com/replikativ/datahike/issues/448) | zoren | 2021-12-06 |
| [#393 — d/with throws IllegalArgumentException](https://github.com/replikativ/datahike/issues/393) | filipesilva | 2023-03-16 |
| [#391 — Can't create/open DB in Web Worker ](https://github.com/replikativ/datahike/issues/391) | MarkusH14 | 2022-05-30 |
| [#387 — Connect to existing filestore created in different process fails](https://github.com/replikativ/datahike/issues/387) | jsmassa | 2023-03-16 |
| [#377 — adjust import/export for tx log](https://github.com/replikativ/datahike/issues/377) | kordano | 2022-05-30 |
| [#370 — Ratios cannot be stored in datahike in the long term](https://github.com/replikativ/datahike/issues/370) | alekcz | 2023-03-16 |
| [#361 — evaluate tx-log indices for migrations](https://github.com/replikativ/datahike/issues/361) | kordano | 2022-06-24 |
| [#356 — \[Bug\] `d/entity` throws Exception](https://github.com/replikativ/datahike/issues/356) | MrEbbinghaus | 2023-06-29 |
| [#321 — add missing attributes in schema.md](https://github.com/replikativ/datahike/issues/321) | kordano | 2022-06-24 |
| [#285 — No implementation of method: :-resolve-chan of protocol: #'hitchhiker.tree.node/IAddress found for class: clojure.lang.PersistentArrayMap](https://github.com/replikativ/datahike/issues/285) | livtanong | 2021-06-10 |
| [#262 — Export needs sorting schema first to be importable](https://github.com/replikativ/datahike/issues/262) | markusalbertgraf | 2022-05-30 |
| [#247 — `:tx-data` cannot be used as a data source for `d/q`](https://github.com/replikativ/datahike/issues/247) | w9 | 2022-05-30 |
| [#246 — Consider running transaction function calls in parallel](https://github.com/replikativ/datahike/issues/246) | w9 | 2022-05-30 |
| [#215 — Added qseq support](https://github.com/replikativ/datahike/issues/215) | vgautamm | 2020-11-06 |
| [#206 — ClojureScript Support](https://github.com/replikativ/datahike/issues/206) | samdesota | 2022-05-30 |
| [#184 — Which attribute caused "Incomplete schema transaction attributes, expected..."?](https://github.com/replikativ/datahike/issues/184) | theronic | 2020-10-19 |
| [#176 — "Update not supported for these schema attributes" - which ones?](https://github.com/replikativ/datahike/issues/176) | theronic | 2022-05-30 |
| [#175 — Idiomatic way to express 'sum type' (db/valueType) spec?](https://github.com/replikativ/datahike/issues/175) | dgb23 | 2021-02-27 |
| [#150 — Can't pull istant by specifying attribute](https://github.com/replikativ/datahike/issues/150) | vlaaad | 2021-05-27 |
| [#147 — Datahike's as-of behavior does not match that of Datomic for number inputs](https://github.com/replikativ/datahike/issues/147) | vlaaad | 2020-05-06 |
| [#140 — Support efficient predicates on ordered values](https://github.com/replikativ/datahike/issues/140) | whilo | 2022-05-30 |
| [#139 — Replicating Datahike, is it possible to transact datoms directly?](https://github.com/replikativ/datahike/issues/139) | samdesota | 2022-06-02 |
| [#123 — Questions about the behavior of multiple db transacting](https://github.com/replikativ/datahike/issues/123) | wandersoncferreira | 2020-03-04 |
| [#118 — parameterized query with lookup ref returns different result to inlined lookup ref](https://github.com/replikativ/datahike/issues/118) | xificurC | 2020-04-26 |
| [#75 — Using idents as query parameters doesn't unify then](https://github.com/replikativ/datahike/issues/75) | purrgrammer | 2019-10-21 |
| [#67 — {:db/ident keyword} should be able to be transacted](https://github.com/replikativ/datahike/issues/67) | boxxxie | 2022-06-02 |

## Recently closed issues

| Issue | Author | Closed |
| --- | --- | --- |
| [#1063 — \[Bug\]: db-view-hash rescans every datom on each view hasheq/hashCode — O(n) per call, uncached](https://github.com/replikativ/datahike/issues/1063) | mokshasoft | 2026-09-05 |
| [#1024 — \[Bug\]: Query planner: _ argument to a rule in an or → "Cannot resolve any more clauses"](https://github.com/replikativ/datahike/issues/1024) | mokshasoft | 2026-09-30 |
| [#973 — \[Bug\]: Query planner: selective bound-probe query through an or-rule materializes the whole relation instead of seeking, 13-120× slower than legacy (persists on 0.8.1788, post-#956)](https://github.com/replikativ/datahike/issues/973) | mokshasoft | 2026-08-28 |
| [#969 — A query for an undeclared attribute returns the whole database under :attribute-refs?](https://github.com/replikativ/datahike/issues/969) | whilo | 2026-08-22 |
| [#967 — Datomic source labels datoms with final idents but replays the rename, so an attribute-refs import cannot work](https://github.com/replikativ/datahike/issues/967) | whilo | 2026-08-23 |
| [#961 — Make the GC sweep floor settable, so a store can be collected from outside the writer process](https://github.com/replikativ/datahike/issues/961) | whilo | 2026-08-17 |
| [#953 — \[Bug\]: Query planner OOMs on deep recursive queries](https://github.com/replikativ/datahike/issues/953) | mokshasoft | 2026-08-15 |
| [#945 — -temporal-insert uses the current comparator, silently dropping retraction markers](https://github.com/replikativ/datahike/issues/945) | whilo | 2026-08-04 |
| [#944 — :tx-data reports datoms that changed nothing, where DataScript filters them](https://github.com/replikativ/datahike/issues/944) | whilo | 2026-08-04 |
| [#928 — writer.cljc commit loop derefs the connection through the throwing path — every released connection that transacted leaks a tracked supervisor exception](https://github.com/replikativ/datahike/issues/928) | markaddleman | 2026-08-16 |
| [#920 — Planner silently drops rows on a two-level get-else chain (get-else off a get-else-bound entity) — 0.8.1758](https://github.com/replikativ/datahike/issues/920) | markaddleman | 2026-07-30 |
| [#918 — Planner silently under-computes a recursive rule when a recursive arg is rebound by an in-body expression clause (0.8.1757; residual of #911/#915)](https://github.com/replikativ/datahike/issues/918) | markaddleman | 2026-07-30 |
| [#911 — Query planner silently under-computes recursive rules (drops derived tuples) — planner-ON ⊊ planner-OFF (0.8.1752)](https://github.com/replikativ/datahike/issues/911) | markaddleman | 2026-07-29 |
| [#901 — Query planner NPE in post-filter-not-joins on a not-join with :in-bound vars (0.8.1747; distinct from #897)](https://github.com/replikativ/datahike/issues/901) | markaddleman | 2026-07-26 |
| [#897 — Recursive-rule NPE in execute/rel-dedup-into! — regressed at 0.8.1705 when the query planner became the default (#844); planner-off is correct](https://github.com/replikativ/datahike/issues/897) | markaddleman | 2026-07-24 |
| [#884 — Planner: get-else over a named source drops rows that need the default](https://github.com/replikativ/datahike/issues/884) | whilo | 2026-07-16 |
| [#799 — \[Bug\]: Useless warnings are printed when using pull API](https://github.com/replikativ/datahike/issues/799) | jonasseglare | 2026-07-07 |
| [#457 — \[Bug\](?): Transact result :tx-data no longer shows only changed datoms](https://github.com/replikativ/datahike/issues/457) | spieden | 2026-08-04 |
