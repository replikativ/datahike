package datahike.java;

import clojure.lang.Keyword;

/**
 * Pre-defined Datahike keyword constants.
 *
 * <p>Use these constants for common Datahike schema and transaction attributes
 * to avoid typos and get IDE autocompletion.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * import static datahike.java.Keywords.*;
 * import static datahike.java.EDN.*;
 *
 * Map<String, Object> schema = Map.of(
 *     DB_IDENT, keyword("person", "name"),
 *     DB_VALUE_TYPE, STRING,
 *     DB_CARDINALITY, ONE,
 *     DB_DOC, "Person's full name"
 * );
 * }</pre>
 */
public class Keywords {

    /**
     * Forbids instance creation.
     */
    private Keywords() {}

    // =========================================================================
    // Entity Attributes
    // =========================================================================

    /** Entity ID: :db/id */
    public static final Keyword DB_ID = Keyword.intern("db", "id");

    /** Entity identifier: :db/ident */
    public static final Keyword DB_IDENT = Keyword.intern("db", "ident");

    // =========================================================================
    // Schema Attributes
    // =========================================================================

    /** Value type: :db/valueType */
    public static final Keyword DB_VALUE_TYPE = Keyword.intern("db", "valueType");

    /** Cardinality: :db/cardinality */
    public static final Keyword DB_CARDINALITY = Keyword.intern("db", "cardinality");

    /** Documentation: :db/doc */
    public static final Keyword DB_DOC = Keyword.intern("db", "doc");

    /** Uniqueness constraint: :db/unique */
    public static final Keyword DB_UNIQUE = Keyword.intern("db", "unique");

    /** Component attribute: :db/isComponent */
    public static final Keyword DB_IS_COMPONENT = Keyword.intern("db", "isComponent");

    /** Disable history: :db/noHistory */
    public static final Keyword DB_NO_HISTORY = Keyword.intern("db", "noHistory");

    /** Index attribute: :db/index */
    public static final Keyword DB_INDEX = Keyword.intern("db", "index");

    /** Fulltext index: :db/fulltext */
    public static final Keyword DB_FULLTEXT = Keyword.intern("db", "fulltext");

    // =========================================================================
    // Value Types
    // =========================================================================

    /** String type: :db.type/string */
    public static final Keyword STRING = Keyword.intern("db.type", "string");

    /** Boolean type: :db.type/boolean */
    public static final Keyword BOOLEAN = Keyword.intern("db.type", "boolean");

    /** Long type: :db.type/long */
    public static final Keyword LONG = Keyword.intern("db.type", "long");

    /** Big integer type: :db.type/bigint */
    public static final Keyword BIGINT = Keyword.intern("db.type", "bigint");

    /** Float type: :db.type/float */
    public static final Keyword FLOAT = Keyword.intern("db.type", "float");

    /** Double type: :db.type/double */
    public static final Keyword DOUBLE = Keyword.intern("db.type", "double");

    /** Big decimal type: :db.type/bigdec */
    public static final Keyword BIGDEC = Keyword.intern("db.type", "bigdec");

    /** Instant type: :db.type/instant */
    public static final Keyword INSTANT = Keyword.intern("db.type", "instant");

    /** UUID type: :db.type/uuid */
    public static final Keyword UUID_TYPE = Keyword.intern("db.type", "uuid");

    /** Keyword type: :db.type/keyword */
    public static final Keyword KEYWORD_TYPE = Keyword.intern("db.type", "keyword");

    /** Symbol type: :db.type/symbol */
    public static final Keyword SYMBOL_TYPE = Keyword.intern("db.type", "symbol");

    /** Reference type: :db.type/ref */
    public static final Keyword REF = Keyword.intern("db.type", "ref");

    /** Bytes type: :db.type/bytes */
    public static final Keyword BYTES = Keyword.intern("db.type", "bytes");

    // =========================================================================
    // Cardinality Values
    // =========================================================================

    /** Cardinality one: :db.cardinality/one */
    public static final Keyword ONE = Keyword.intern("db.cardinality", "one");

    /** Cardinality many: :db.cardinality/many */
    public static final Keyword MANY = Keyword.intern("db.cardinality", "many");

    // =========================================================================
    // Uniqueness Values
    // =========================================================================

    /** Unique value constraint: :db.unique/value */
    public static final Keyword UNIQUE_VALUE = Keyword.intern("db.unique", "value");

    /** Unique identity constraint: :db.unique/identity */
    public static final Keyword UNIQUE_IDENTITY = Keyword.intern("db.unique", "identity");

    // =========================================================================
    // Transaction Metadata
    // =========================================================================

    /** Transaction entity: :db/tx */
    public static final Keyword TX = Keyword.intern("db", "tx");

    /** Transaction timestamp: :db/txInstant */
    public static final Keyword TX_INSTANT = Keyword.intern("db", "txInstant");

    // =========================================================================
    // Schema Flexibility Values
    // =========================================================================

    /** Schema flexibility read mode: :read */
    public static final Keyword SCHEMA_READ = Keyword.intern("read");

    /** Schema flexibility write mode: :write */
    public static final Keyword SCHEMA_WRITE = Keyword.intern("write");

    // =========================================================================
    // Storage Backends
    // =========================================================================

    /** Memory backend: :memory */
    public static final Keyword BACKEND_MEMORY = Keyword.intern("memory");

    /** File backend: :file */
    public static final Keyword BACKEND_FILE = Keyword.intern("file");
}
