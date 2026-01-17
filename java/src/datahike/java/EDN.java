package datahike.java;

import clojure.lang.Keyword;
import clojure.lang.Symbol;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * EDN type helpers for explicit type construction.
 *
 * <p>This class provides static methods for creating EDN types when you need
 * fine-grained control over conversion, or want to force a value to be treated
 * as a specific EDN type regardless of automatic conversion rules.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * import static datahike.java.EDN.*;
 *
 * Map<String, Object> schema = Map.of(
 *     "db/ident", keyword("person", "name"),
 *     "db/valueType", keyword("db.type", "string"),
 *     "db/cardinality", keyword("db.cardinality", "one"),
 *     "db/doc", forceString(":literal-colon-in-doc")
 * );
 * }</pre>
 */
public class EDN {

    private static final IFn readStringFn = Clojure.var("clojure.edn", "read-string");

    /**
     * Forbids instance creation.
     */
    private EDN() {}

    /**
     * Creates an EDN keyword from a name.
     *
     * <p>Example:</p>
     * <pre>{@code
     * keyword("name")  // → :name
     * }</pre>
     *
     * @param name keyword name
     * @return Clojure keyword
     */
    public static Keyword keyword(String name) {
        return Keyword.intern(name);
    }

    /**
     * Creates a namespaced EDN keyword.
     *
     * <p>Example:</p>
     * <pre>{@code
     * keyword("person", "name")  // → :person/name
     * }</pre>
     *
     * @param namespace keyword namespace
     * @param name keyword name
     * @return Clojure keyword
     */
    public static Keyword keyword(String namespace, String name) {
        return Keyword.intern(namespace, name);
    }

    /**
     * Creates an EDN symbol from a name.
     *
     * <p>Symbols are used for function names and special forms in Clojure.
     * Rarely needed in data transactions.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * symbol("my-fn")  // → my-fn
     * }</pre>
     *
     * @param name symbol name
     * @return Clojure symbol
     */
    public static Symbol symbol(String name) {
        return Symbol.intern(name);
    }

    /**
     * Creates a namespaced EDN symbol.
     *
     * <p>Example:</p>
     * <pre>{@code
     * symbol("clojure.core", "assoc")  // → clojure.core/assoc
     * }</pre>
     *
     * @param namespace symbol namespace
     * @param name symbol name
     * @return Clojure symbol
     */
    public static Symbol symbol(String namespace, String name) {
        return Symbol.intern(namespace, name);
    }

    /**
     * Creates an EDN UUID tagged literal.
     *
     * <p>Returns a string that will be parsed as #uuid when converted to EDN.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * uuid("550e8400-e29b-41d4-a716-446655440000")
     * // → #uuid "550e8400-e29b-41d4-a716-446655440000"
     * }</pre>
     *
     * @param uuidString UUID string (with or without #uuid prefix)
     * @return UUID tagged literal
     */
    public static Object uuid(String uuidString) {
        // Strip #uuid prefix if present
        String cleaned = uuidString.trim();
        if (cleaned.startsWith("#uuid")) {
            cleaned = cleaned.substring(5).trim().replaceAll("\"", "");
        }
        return readStringFn.invoke("#uuid \"" + cleaned + "\"");
    }

    /**
     * Creates an EDN instant (timestamp) tagged literal.
     *
     * <p>Returns a string that will be parsed as #inst when converted to EDN.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * inst("2024-01-01T00:00:00Z")
     * // → #inst "2024-01-01T00:00:00Z"
     * }</pre>
     *
     * @param isoTimestamp ISO 8601 timestamp string
     * @return instant tagged literal
     */
    public static Object inst(String isoTimestamp) {
        // Strip #inst prefix if present
        String cleaned = isoTimestamp.trim();
        if (cleaned.startsWith("#inst")) {
            cleaned = cleaned.substring(5).trim().replaceAll("\"", "");
        }
        return readStringFn.invoke("#inst \"" + cleaned + "\"");
    }

    /**
     * Forces a value to be treated as a string, even if it starts with ':'.
     *
     * <p>Use this to create strings that start with ':' without them becoming keywords.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * forceString(":literal-colon")
     * // → ":literal-colon" (string, not :literal-colon keyword)
     * }</pre>
     *
     * <p>Note: This is equivalent to using backslash escaping: "\\:literal-colon"</p>
     *
     * @param value string value (can start with :)
     * @return the string as-is (will not be converted to keyword)
     */
    public static String forceString(String value) {
        // If value starts with ':', we need to escape it to prevent keyword conversion
        if (value.startsWith(":")) {
            return "\\" + value;  // Add backslash escape
        }
        return value;
    }
}
