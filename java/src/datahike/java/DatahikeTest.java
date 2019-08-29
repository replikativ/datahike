package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static datahike.java.Util.deref;
import static datahike.java.Util.k;
import static org.junit.Assert.*;

public class DatahikeTest {

    private String uri = "datahike:mem://test-empty-db";

    @org.junit.Before
    public void setUp() throws Exception {
        Datahike.deleteDatabase(uri);
        Datahike.createDatabase(uri);
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @org.junit.Test
    public void q() {
        // Transacting new schema
        Object conn = Datahike.connect(uri);
        Datahike.transact(conn, Util.vector(Util.map(k(":db/ident"), k(":name"),
                k(":db/valueType"), k(":db.type/string"),
                k(":db/cardinality"), k(":db.cardinality/one"))));

        // Transacting with schema presence
        Datahike.transact(conn, Util.vector(Util.map(k(":name"), "Alice")));

        Object dConn = deref.invoke(conn);

        Set res = Datahike.q(Clojure.read("[:find ?e :where [?e :name]]"), dConn);
        assertTrue(res.size() == 1);

        res = Datahike.q(Clojure.read("[:find ?v :where [_ :name ?v]]"), dConn);
        //Assert.assertEquals("Alice", ((List)res.toArray()[0]).iterator().next());
        assertEquals(PersistentHashSet.create(Arrays.asList(PersistentVector.create("Alice"))), res);
    }
}
