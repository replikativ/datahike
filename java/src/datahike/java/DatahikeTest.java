package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;

import java.util.Arrays;
import java.util.Set;

import static datahike.java.Util.*;
import static org.junit.Assert.*;

public class DatahikeTest {

    private String uri = "datahike:mem://test-empty-db";
    private Object conn;

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
        Datahike.transact(conn, Util.vector(map(k(":db/ident"), k(":name"),
                k(":db/valueType"), k(":db.type/string"),
                k(":db/cardinality"), k(":db.cardinality/one"))));

        // Transacting with schema presence
        Datahike.transact(conn, Util.vector(map(k(":name"), "Alice")));

        Object dConn = deref.invoke(conn);

        Set res = Datahike.q(Clojure.read("[:find ?e :where [?e :name]]"), dConn);
        assertTrue(res.size() == 1);

        res = Datahike.q(Clojure.read("[:find ?v :where [_ :name ?v]]"), dConn);
        //Assert.assertEquals("Alice", ((List)res.toArray()[0]).iterator().next());
        assertEquals(PersistentHashSet.create(Arrays.asList(PersistentVector.create("Alice"))), res);
    }

    @org.junit.Test
    public void history() {
        Object schema = Clojure.read(" [{:db/ident :name\n" +
                "                 :db/valueType :db.type/string\n" +
                "                 :db/unique :db.unique/identity\n" +
                "                 :db/index true\n" +
                "                 :db/cardinality :db.cardinality/one}\n" +
                "                {:db/ident :age\n" +
                "                 :db/valueType :db.type/long\n" +
                "                 :db/cardinality :db.cardinality/one}]");
        Datahike.createDatabase(uri, k(":initial-tx"), schema);

        conn = Datahike.connect(uri);
        Object dConn = deref.invoke(conn);

        Datahike.transact(conn, Util.vector(
                map(k(":name"), "Alice", k(":age"), 25L),
                map(k(":name"), "Bob", k(":age"), 30L)));

        Set res = Datahike.q(Clojure.read("[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]"),
                Datahike.history(deref.invoke(conn)));
        // TODO: assert the returned values
    }
}
