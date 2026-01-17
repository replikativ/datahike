package datahike.java;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Fluent builder for Datahike database configurations.
 *
 * <p>Provides a convenient, type-safe way to construct database configurations
 * with method chaining. Supports both simple factory methods for common use cases
 * and detailed configuration through builder methods.</p>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * // In-memory database
 * Map<String, Object> config = Database.memory(UUID.randomUUID())
 *     .keepHistory(true)
 *     .build();
 *
 * // File-based database
 * Map<String, Object> config = Database.file("/tmp/mydb")
 *     .schemaFlexibility(SchemaFlexibility.READ)
 *     .build();
 * }</pre>
 *
 * <h2>Configuration Options</h2>
 * <ul>
 *   <li>{@link #keepHistory(boolean)} - Enable/disable transaction history (default: true)</li>
 *   <li>{@link #schemaFlexibility(SchemaFlexibility)} - Control schema enforcement</li>
 *   <li>{@link #initialTx(Object)} - Set initial transaction data (e.g., schema)</li>
 *   <li>{@link #name(String)} - Set database name for logging/metrics</li>
 * </ul>
 *
 * @see Datahike
 * @see SchemaFlexibility
 */
public class Database {
    private final Map<String, Object> store;
    private final Map<String, Object> config;

    /**
     * Private constructor - use factory methods like {@link #memory(UUID)} or {@link #file(String)}.
     */
    private Database(Map<String, Object> store) {
        this.store = new HashMap<>(store);
        this.config = new HashMap<>();
    }

    // =========================================================================
    // Factory Methods
    // =========================================================================

    /**
     * Creates an in-memory database configuration.
     *
     * <p><strong>Important:</strong> Memory backend requires a UUID identifier.
     * This is required by the konserve store and is essential for distributed
     * database tracking.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * UUID id = UUID.randomUUID();
     * Map<String, Object> config = Database.memory(id).build();
     * Datahike.createDatabase(config);
     * }</pre>
     *
     * @param id unique UUID identifier for the database
     * @return Database builder for method chaining
     */
    public static Database memory(UUID id) {
        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":memory");
        store.put("id", id);
        return new Database(store);
    }

    /**
     * Creates an in-memory database configuration from a UUID string.
     *
     * <p>Convenience overload that accepts a string representation of a UUID.
     * The string will be parsed into a UUID object.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * String id = UUID.randomUUID().toString();
     * Map<String, Object> config = Database.memory(id).build();
     * }</pre>
     *
     * @param id UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000")
     * @return Database builder for method chaining
     * @throws IllegalArgumentException if the string is not a valid UUID
     */
    public static Database memory(String id) {
        return memory(UUID.fromString(id));
    }

    /**
     * Creates a file-based database configuration.
     *
     * <p>The file backend persists data to the local filesystem. The path
     * should be a directory where Datahike will store database files.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> config = Database.file("/var/lib/mydb")
     *     .keepHistory(true)
     *     .build();
     * Datahike.createDatabase(config);
     * }</pre>
     *
     * @param path filesystem path where database files will be stored
     * @return Database builder for method chaining
     */
    public static Database file(String path) {
        Map<String, Object> store = new HashMap<>();
        store.put("backend", ":file");
        store.put("path", path);
        return new Database(store);
    }

    /**
     * Creates a custom backend configuration.
     *
     * <p>Use this for backends not covered by the convenience methods,
     * such as PostgreSQL, S3, or custom storage implementations.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> storeConfig = Map.of(
     *     "backend", ":pg",
     *     "host", "localhost",
     *     "port", 5432,
     *     "username", "user",
     *     "password", "pass"
     * );
     * Map<String, Object> config = Database.custom(storeConfig).build();
     * }</pre>
     *
     * @param storeConfig map containing backend-specific configuration
     * @return Database builder for method chaining
     */
    public static Database custom(Map<String, Object> storeConfig) {
        return new Database(storeConfig);
    }

    // =========================================================================
    // Builder Methods
    // =========================================================================

    /**
     * Sets whether to keep transaction history.
     *
     * <p>When enabled (default), Datahike maintains a full history of all
     * transactions, enabling time-travel queries with {@code asOf} and {@code since}.</p>
     *
     * <p>Disable this for write-heavy workloads where history is not needed,
     * as it reduces storage requirements and improves write performance.</p>
     *
     * @param keep true to keep history (default), false to disable
     * @return this builder for method chaining
     */
    public Database keepHistory(boolean keep) {
        config.put("keep-history?", keep);
        return this;
    }

    /**
     * Sets the schema flexibility mode.
     *
     * <p>Controls how the database handles attributes not defined in the schema:</p>
     * <ul>
     *   <li>{@link SchemaFlexibility#READ} - Allow reading undefined attributes, reject writes</li>
     *   <li>{@link SchemaFlexibility#WRITE} - Allow both reading and writing undefined attributes</li>
     * </ul>
     *
     * <p>If not set, the database enforces strict schema validation by default.</p>
     *
     * @param mode the schema flexibility mode
     * @return this builder for method chaining
     */
    public Database schemaFlexibility(SchemaFlexibility mode) {
        config.put("schema-flexibility", mode.toEDNString());
        return this;
    }

    /**
     * Sets the initial transaction to be applied when creating the database.
     *
     * <p>This is typically used to define the schema before any data is added.
     * The transaction can be provided as:</p>
     * <ul>
     *   <li>A Clojure data structure (using {@link Util#vec} and {@link Util#map})</li>
     *   <li>An EDN string (parsed with {@link Util#ednFromString})</li>
     * </ul>
     *
     * <p>Example:</p>
     * <pre>{@code
     * import static datahike.java.Util.*;
     * import static datahike.java.Keywords.*;
     *
     * Object schema = vec(
     *     map(DB_IDENT, kwd(":person/name"),
     *         DB_VALUE_TYPE, STRING,
     *         DB_CARDINALITY, ONE)
     * );
     *
     * Map<String, Object> config = Database.memory(UUID.randomUUID())
     *     .initialTx(schema)
     *     .build();
     * }</pre>
     *
     * @param tx the initial transaction data
     * @return this builder for method chaining
     */
    public Database initialTx(Object tx) {
        config.put("initial-tx", tx);
        return this;
    }

    /**
     * Sets the database name for logging and metrics.
     *
     * <p>This is optional and used for identifying the database in logs,
     * error messages, and monitoring systems.</p>
     *
     * @param name the database name
     * @return this builder for method chaining
     */
    public Database name(String name) {
        config.put("name", name);
        return this;
    }

    /**
     * Adds a custom configuration option.
     *
     * <p>Use this for configuration options not covered by the builder methods.
     * Keys are automatically converted to keywords when passed to Datahike.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * Map<String, Object> config = Database.file("/tmp/db")
     *     .option("attribute-refs?", true)
     *     .option("temporal-index", true)
     *     .build();
     * }</pre>
     *
     * @param key configuration key (will be keywordized)
     * @param value configuration value
     * @return this builder for method chaining
     */
    public Database option(String key, Object value) {
        config.put(key, value);
        return this;
    }

    /**
     * Builds and returns the configuration map.
     *
     * <p>The returned map can be passed directly to Datahike API methods like
     * {@link Datahike#createDatabase(Object)}, {@link Datahike#connect(Object)}, etc.</p>
     *
     * <p>The map format matches Datahike's expected configuration structure with
     * keys automatically converted to keywords.</p>
     *
     * @return configuration map ready for use with Datahike API
     */
    public Map<String, Object> build() {
        Map<String, Object> result = new HashMap<>(config);
        result.put("store", store);
        return result;
    }
}
