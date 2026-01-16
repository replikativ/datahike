package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;

import java.util.*;

import static datahike.java.Util.derefFn;

/**
 * Public API for Datahike database operations.
 *
 * <p>This class extends DatahikeGenerated to expose all auto-generated API methods
 * from the Datahike specification. Additional convenience methods for Java ergonomics
 * are defined here.</p>
 *
 * <p>Configuration can be constructed using:</p>
 * <ul>
 *   <li>{@link #memoryConfig(String)} - In-memory database configuration</li>
 *   <li>{@link #fileConfig(String)} - File-based database configuration</li>
 *   <li>{@link Util#ednFromString(String)} - Parse EDN configuration strings</li>
 *   <li>{@link Util#map(Object...)} and {@link Util#kwd(String)} - Build maps programmatically</li>
 *   <li>Java {@code Map<String, Object>} - Direct Java maps (keys auto-converted to keywords)</li>
 * </ul>
 *
 * <p>Example:</p>
 * <pre>{@code
 * // Simple memory database
 * Map<String, Object> config = Datahike.memoryConfig("mydb");
 * Datahike.createDatabase(config);
 * Object conn = Datahike.connect(config);
 *
 * // Or with custom configuration
 * Map<String, Object> customConfig = Map.of(
 *     "store", Map.of("backend", ":memory", "id", "mydb"),
 *     "schema-flexibility", ":read",
 *     "keep-history?", true
 * );
 * Datahike.createDatabase(customConfig);
 * }</pre>
 *
 * @see <a href="https://github.com/replikativ/datahike">Datahike Documentation</a>
 */
public class Datahike extends DatahikeGenerated {

    private static final IFn qFn = Clojure.var("datahike.api", "q");
    private static final IFn pullFn = Clojure.var("datahike.api", "pull");
    private static final IFn pullManyFn = Clojure.var("datahike.api", "pull-many");
    private static final IFn seekDatomsFn = Clojure.var("datahike.api", "seek-datoms");

    /**
     * Forbids instance creation.
     */
    private Datahike() {}

    // =========================================================================
    // Configuration Helpers
    // =========================================================================

    /**
     * Creates an in-memory database configuration.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> config = Datahike.memoryConfig("mydb");
     * Datahike.createDatabase(config);
     * }</pre>
     *
     * @param id unique identifier for the database
     * @return configuration map for in-memory database
     */
    public static Map<String, Object> memoryConfig(String id) {
        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":memory");
        store.put("id", id);

        Map<String, Object> config = new HashMap<>();
        config.put("store", store);
        return config;
    }

    /**
     * Creates an in-memory database configuration with additional options.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> options = new HashMap<>();
     * options.put("schema-flexibility", ":read");
     * options.put("keep-history?", true);
     * Map<String, Object> config = Datahike.memoryConfig("mydb", options);
     * }</pre>
     *
     * @param id unique identifier for the database
     * @param options additional configuration options
     * @return configuration map for in-memory database with options
     */
    public static Map<String, Object> memoryConfig(String id, Map<String, Object> options) {
        Map<String, Object> config = new HashMap<>(options);

        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":memory");
        store.put("id", id);
        config.put("store", store);

        return config;
    }

    /**
     * Creates a file-based database configuration.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> config = Datahike.fileConfig("/tmp/mydb");
     * Datahike.createDatabase(config);
     * }</pre>
     *
     * @param path file system path for database storage
     * @return configuration map for file-based database
     */
    public static Map<String, Object> fileConfig(String path) {
        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":file");
        store.put("path", path);

        Map<String, Object> config = new HashMap<>();
        config.put("store", store);
        return config;
    }

    /**
     * Creates a file-based database configuration with additional options.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> options = new HashMap<>();
     * options.put("schema-flexibility", ":write");
     * Map<String, Object> config = Datahike.fileConfig("/tmp/mydb", options);
     * }</pre>
     *
     * @param path file system path for database storage
     * @param options additional configuration options
     * @return configuration map for file-based database with options
     */
    public static Map<String, Object> fileConfig(String path, Map<String, Object> options) {
        Map<String, Object> config = new HashMap<>(options);

        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":file");
        store.put("path", path);
        config.put("store", store);

        return config;
    }

    // =========================================================================
    // Convenience Methods
    // =========================================================================

    /**
     * Dereferences a connection to get the current database value.
     * Equivalent to {@code @conn} in Clojure.
     *
     * @param conn a connection to the database
     * @return the current immutable database value
     */
    public static Object deref(Object conn) {
        return derefFn.invoke(conn);
    }

    /**
     * Executes a datalog query with automatic query string parsing.
     * This is a convenience method that automatically parses the query string.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Set results = (Set) Datahike.q("[:find ?name :where [?e :name ?name]]", db);
     * }</pre>
     *
     * @param query query string in EDN format
     * @param inputs the database and optional input arguments
     * @return query results (typically a Set of tuples or a scalar value)
     */
    public static Object q(String query, Object... inputs) {
        List<Object> args = new ArrayList<>(Arrays.asList(inputs));
        args.add(0, Clojure.read(query));
        return qFn.applyTo(RT.seq(args));
    }

    /**
     * Pull pattern query with automatic pattern string parsing.
     * Convenience method that parses the pull pattern from a string.
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map result = Datahike.pull(db, "[:db/id :name :age]", entityId);
     * }</pre>
     *
     * @param db the database
     * @param pattern pull pattern as EDN string
     * @param eid entity id
     * @return map of pulled attributes
     */
    public static Map<?, ?> pull(Object db, String pattern, Object eid) {
        return (Map<?, ?>) pullFn.invoke(db, Clojure.read(pattern), eid);
    }

    /**
     * Pull many entities with automatic pattern string parsing.
     * Convenience method that parses the pull pattern from a string.
     *
     * <p>Example:</p>
     * <pre>{@code
     * List results = Datahike.pullMany(db, "[:db/id :name]", Arrays.asList(1, 2, 3));
     * }</pre>
     *
     * @param db the database
     * @param pattern pull pattern as EDN string
     * @param eids collection of entity ids
     * @return list of maps with pulled attributes
     */
    public static List<?> pullMany(Object db, String pattern, Iterable<?> eids) {
        return (List<?>) pullManyFn.invoke(db, Clojure.read(pattern), eids);
    }

    /**
     * Seek datoms with individual component arguments (Java-friendly API).
     * More ergonomic than the map-based API for simple seeks.
     *
     * @param db the database
     * @param index index keyword (e.g., {@code kwd(":eavt")})
     * @param c1 first component (entity id)
     * @return iterable of datoms
     */
    public static Iterable<?> seekdatoms(Object db, Keyword index, Object c1) {
        return (Iterable<?>) seekDatomsFn.invoke(db, index, c1);
    }

    /**
     * Seek datoms with two component arguments.
     *
     * @param db the database
     * @param index index keyword (e.g., {@code kwd(":eavt")})
     * @param c1 first component (entity id)
     * @param c2 second component (attribute)
     * @return iterable of datoms
     */
    public static Iterable<?> seekdatoms(Object db, Keyword index, Object c1, Object c2) {
        return (Iterable<?>) seekDatomsFn.invoke(db, index, c1, c2);
    }

    /**
     * Seek datoms with three component arguments.
     *
     * @param db the database
     * @param index index keyword (e.g., {@code kwd(":eavt")})
     * @param c1 first component (entity id)
     * @param c2 second component (attribute)
     * @param c3 third component (value)
     * @return iterable of datoms
     */
    public static Iterable<?> seekdatoms(Object db, Keyword index, Object c1, Object c2, Object c3) {
        return (Iterable<?>) seekDatomsFn.invoke(db, index, c1, c2, c3);
    }

    /**
     * Seek datoms with four component arguments.
     *
     * @param db the database
     * @param index index keyword (e.g., {@code kwd(":eavt")})
     * @param c1 first component (entity id)
     * @param c2 second component (attribute)
     * @param c3 third component (value)
     * @param c4 fourth component (transaction id)
     * @return iterable of datoms
     */
    public static Iterable<?> seekdatoms(Object db, Keyword index, Object c1, Object c2, Object c3, Object c4) {
        return (Iterable<?>) seekDatomsFn.invoke(db, index, c1, c2, c3, c4);
    }
}
