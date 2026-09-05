import * as d from "./remote/index.js";

async function remoteTypescriptTest() {
  const config: d.DatabaseConfig = {
    store: { backend: ":memory", id: d.randomUuid() },
    "schema-flexibility": ":read",
    "remote-peer": { backend: ":datahike-server", url: "http://localhost:3000", token: "token" },
  };
  await d.createDatabase(config);
  const conn: d.Connection = await d.connect(config);
  await d.transact(conn, { "tx-data": [], "tx-options": { "allow-index-backfill?": true } });
  const barrierPromise: Promise<d.Database> = d.writerBarrier(conn);
  const barrierDb: d.Database = await barrierPromise;
  await d.q<Array<[string]>>("[:find ?name :where [?e :name ?name]]", barrierDb);
  const db: d.Database = await d.db(conn);
  const rows = await d.q<Array<[string]>>("[:find ?name :where [?e :name ?name]]", db);
  const listener: string = d.listen(conn, (report: d.RemoteReport) => console.log(report["db-after"]));
  d.unlisten(conn, listener);
  console.log(rows, d.isPromise(Promise.resolve(db)));
}

void remoteTypescriptTest;
