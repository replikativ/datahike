package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import org.junit.Test;

import java.util.*;

import static datahike.java.Datahike.dConn;
import static datahike.java.Util.*;
import static org.junit.Assert.*;

public class DatahikeTest {

    private String uri = "datahike:mem://test-empty-db";
    private Object conn;

    private Object schema = Clojure.read(" [{:db/ident :name\n" +
            "                 :db/valueType :db.type/string\n" +
            "                 :db/unique :db.unique/identity\n" +
            "                 :db/index true\n" +
            "                 :db/cardinality :db.cardinality/one}\n" +
            "                {:db/ident :age\n" +
            "                 :db/valueType :db.type/long\n" +
            "                 :db/cardinality :db.cardinality/one}]");
    private Date firstDate;
    private String query;

    @org.junit.Before
    public void setUp() throws Exception {
        Datahike.deleteDatabase(uri);
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @org.junit.Test
    public void q() {
        Datahike.createDatabase(uri);
        conn = Datahike.connect(uri);

        // Transacting new schema
        Datahike.transact(conn, vec(map(k(":db/ident"), k(":name"),
                k(":db/valueType"), k(":db.type/string"),
                k(":db/cardinality"), k(":db.cardinality/one"))));

        // Transacting with schema presence
        Datahike.transact(conn, vec(map(k(":name"), "Alice")));

        Object dConn = deref.invoke(conn);

        Set res = Datahike.q("[:find ?e :where [?e :name]]", dConn);
        assertTrue(res.size() == 1);

        res = Datahike.q("[:find ?v :where [_ :name ?v]]", dConn);
        //Assert.assertEquals("Alice", ((List)res.toArray()[0]).iterator().next());
        assertEquals(PersistentHashSet.create(Arrays.asList(PersistentVector.create("Alice"))), res);
    }

    private void transactOnce() {
        Datahike.createDatabase(uri, k(":initial-tx"), schema);

        conn = Datahike.connect(uri);
        query = "[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]";

        Datahike.transact(conn, vec(
                map(k(":name"), "Alice", k(":age"), 25L),
                map(k(":name"), "Bob", k(":age"), 30L)));
    }

    @org.junit.Test
    public void history() {
        transactOnce();

        Set res = Datahike.q((String) query, Datahike.history(dConn(conn)));
        // TODO: add better assert
        assertEquals(2, res.size());
    }

    @Test
    public void asOfAndSince() {
        transactOnce();

        firstDate = new Date();
        Datahike.transact(conn, vec(map(
                k(":db/id"), vec(k(":name"), "Alice"),
                k(":age"), 30L)));

        Set res = Datahike.q(query, Datahike.asOf(dConn(conn), firstDate));
        // TODO: add better assert
        assertEquals(2, res.size());

        res = Datahike.q(query, Datahike.since(dConn(conn), firstDate));
        // 0, because :name was transacted before the first date
        assertEquals(0, res.size());
    }

    @Test
    public void pullAndPullMany() {
        transactOnce();

        // TODO: does not work
//        Datahike.pull(dConn(conn), "[*]", "[:name 'Alice']");

        Datahike.transact(conn, vec(map(k(":db/id"), 10,
                                        k(":name"), "Joe",
                                        k(":age"), 50L)));

        Map res = Datahike.pull(dConn(conn), "[*]", 10);
        assertEquals("Joe", res.get(k(":name")));

        Datahike.transact(conn, vec(map(k(":db/id"), 20,
                k(":name"), "Jane",
                k(":age"), 25L)));
        List list = Datahike.pullMany(dConn(conn), "[*]", "[10, 20]");
        assertEquals(2, list.size());
    }

    @Test
    public void release() {
        Datahike.createDatabase(uri, k(":initial-tx"), schema);
        conn = Datahike.connect(uri);
        Datahike.release(conn);
    }

    // TODO: datom function
    // TODO: what else in core should be added?
}
