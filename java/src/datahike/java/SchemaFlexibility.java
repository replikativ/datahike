package datahike.java;

import clojure.lang.Keyword;

/**
 * Schema flexibility modes for Datahike databases.
 *
 * <p>Controls how the database handles attributes not defined in the schema:</p>
 * <ul>
 *   <li>{@link #READ} - Strict mode: Only allow reads of undefined attributes, reject writes</li>
 *   <li>{@link #WRITE} - Flexible mode: Allow both reads and writes of undefined attributes</li>
 * </ul>
 *
 * @see <a href="https://github.com/replikativ/datahike/blob/main/doc/schema.md">Schema Documentation</a>
 */
public enum SchemaFlexibility {
    /**
     * Read-only flexibility: Allow reading undefined attributes but reject writes.
     * This is the recommended mode for production use with schemaless data.
     */
    READ(Keywords.SCHEMA_READ),

    /**
     * Write flexibility: Allow both reading and writing undefined attributes.
     * Use this for fully schemaless databases or during prototyping.
     */
    WRITE(Keywords.SCHEMA_WRITE);

    private final Keyword keyword;

    SchemaFlexibility(Keyword keyword) {
        this.keyword = keyword;
    }

    /**
     * Returns the Clojure keyword representation.
     *
     * @return the keyword for this schema flexibility mode
     */
    public Keyword toKeyword() {
        return keyword;
    }

    /**
     * Returns the string representation suitable for EDN/config maps.
     *
     * @return the string representation (e.g., ":read", ":write")
     */
    public String toEDNString() {
        return keyword.toString();
    }
}
