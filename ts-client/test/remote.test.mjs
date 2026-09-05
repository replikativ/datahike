import assert from "node:assert/strict";
import test from "node:test";
import * as d from "../../npm-package/remote/index.mjs";

const url = process.env.DATAHIKE_REMOTE_URL;
const token = process.env.DATAHIKE_REMOTE_TOKEN;

function eventWhere(predicate, timeout = 5000) {
  let resolve;
  let reject;
  const promise = new Promise((ok, fail) => { resolve = ok; reject = fail; });
  const timer = setTimeout(() => reject(new Error("Timed out waiting for a change event")), timeout);
  return {
    promise,
    accept(value) {
      if (predicate(value)) {
        clearTimeout(timer);
        resolve(value);
      }
    },
  };
}

test("TypeScript thin client against the JVM server", { skip: !url || !token }, async () => {
  const calls = [];
  const realFetch = globalThis.fetch;
  let conn;
  let key;
  globalThis.fetch = (input, init) => {
    calls.push({ url: String(input), method: init?.method });
    return realFetch(input, init);
  };

  const config = {
    store: { backend: ":memory", id: d.randomUuid() },
    "schema-flexibility": ":read",
    "remote-peer": { backend: ":datahike-server", url, token },
  };

  try {
    const echoed = await d.createDatabase(config);
    assert.equal(echoed["remote-peer"].token, token);
    conn = await d.connect(config);
    const firstReport = await d.transact(conn, [{ name: "Ada", age: 36 }, { name: "Grace", age: 45 }]);
    assert.ok(firstReport["db-after"]);
    assert.ok(firstReport["tx-data"].every((datom) => typeof datom.e === "number"));

    const barrierPromise = d.writerBarrier(conn);
    assert.ok(barrierPromise instanceof Promise);
    const barrierDb = await barrierPromise;
    assert.deepEqual((await d.q("[:find ?name :where [?e :name ?name]]", barrierDb)).sort(),
      [["Ada"], ["Grace"]]);
    assert.ok(calls.some((call) => call.url.endsWith("/writer-barrier") && call.method === "POST"));

    const db = await d.db(conn);
    const small = await d.q("[:find ?n :where [?e :name ?n]]", db);
    assert.deepEqual(small.map((row) => row[0]).sort(), ["Ada", "Grace"]);
    const qCalls = calls.filter((call) => call.url.includes("/q"));
    assert.equal(qCalls.at(-1).method, "GET");
    assert.match(qCalls.at(-1).url, /\?args=.*&f=json$/);

    const ages = Array.from({ length: 3000 }, (_, index) => index);
    const large = await d.q("[:find ?n :in $ [?a ...] :where [?e :age ?a] [?e :name ?n]]", db, ages);
    assert.deepEqual(large.map((row) => row[0]).sort(), ["Ada", "Grace"]);
    const largeCall = calls.filter((call) => call.url.endsWith("/q")).at(-1);
    assert.equal(largeCall.method, "POST");

    const eid = await d.q("[:find ?e . :where [?e :name \"Ada\"]]", db);
    assert.deepEqual(await d.pull(db, [":name", ":age"], eid), { name: "Ada", age: 36 });

    const resyncEvent = eventWhere((report) => report.resync === true);
    const reportEvent = eventWhere((report) => Array.isArray(report["tx-data"]));
    key = d.listen(conn, (report) => {
      resyncEvent.accept(report);
      reportEvent.accept(report);
    });
    const resync = await resyncEvent.promise;
    assert.ok(resync["db-after"]);

    await d.transact(conn, [{ "listener/value": ":received" }]);
    const report = await reportEvent.promise;
    assert.equal(report["tx-data"].find((datom) => datom.a === ":listener/value").v, ":received");
    const laterDb = await d.db(conn);
    assert.equal(report["commit-id"], report["db-after"]["commit-id"]);
    assert.equal(report["commit-id"], laterDb["commit-id"]);
    assert.deepEqual(await d.q("[:find ?v :where [?e :listener/value ?v]]", report["db-after"]), [[":received"]]);

    d.unlisten(conn, key);
    await d.release(conn);
    await d.deleteDatabase(config);
    assert.equal(await d.databaseExists(config), false);
  } finally {
    if (conn && key) d.unlisten(conn, key);
    globalThis.fetch = realFetch;
  }
});
