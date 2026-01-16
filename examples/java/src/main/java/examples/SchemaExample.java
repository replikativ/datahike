package examples;

import datahike.java.Datahike;
import datahike.java.Database;
import datahike.java.SchemaFlexibility;

import java.util.*;

import static datahike.java.Keywords.*;
import static datahike.java.Util.*;

/**
 * Example demonstrating schema definition and validation.
 *
 * Shows:
 * - Defining schema attributes with Keywords constants
 * - Using unique constraints
 * - Reference types for relationships
 * - Cardinality (one vs many)
 */
public class SchemaExample {
    public static void main(String[] args) {
        System.out.println("=== Schema Definition Example ===\n");

        // Define schema using Keywords constants
        System.out.println("1. Defining schema...");
        Object schema = vec(
            // Person name - unique identity
            map(
                DB_IDENT, kwd(":person/name"),
                DB_VALUE_TYPE, STRING,
                DB_CARDINALITY, ONE,
                DB_UNIQUE, UNIQUE_IDENTITY,
                DB_DOC, "Person's full name (unique)"
            ),
            // Person email - unique value
            map(
                DB_IDENT, kwd(":person/email"),
                DB_VALUE_TYPE, STRING,
                DB_CARDINALITY, ONE,
                DB_UNIQUE, UNIQUE_VALUE,
                DB_DOC, "Person's email address (unique)"
            ),
            // Person age
            map(
                DB_IDENT, kwd(":person/age"),
                DB_VALUE_TYPE, LONG,
                DB_CARDINALITY, ONE,
                DB_DOC, "Person's age in years"
            ),
            // Person friends - many references
            map(
                DB_IDENT, kwd(":person/friends"),
                DB_VALUE_TYPE, REF,
                DB_CARDINALITY, MANY,
                DB_DOC, "Person's friends (entity references)"
            ),
            // Person skills - many strings
            map(
                DB_IDENT, kwd(":person/skills"),
                DB_VALUE_TYPE, STRING,
                DB_CARDINALITY, MANY,
                DB_DOC, "Person's skills"
            )
        );

        // Create database with initial schema
        Map<String, Object> config = Database.memory(UUID.randomUUID())
            .initialTx(schema)
            .build();

        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);

        // 2. Add data respecting the schema
        System.out.println("2. Adding people with relationships...");

        // Using modern Java API - strings auto-convert to keywords
        Datahike.transact(conn, List.of(
            // Alice
            Map.of(
                "person/name", "Alice",
                "person/email", "alice@example.com",
                "person/age", 30L,
                "person/skills", List.of("Java", "Clojure", "Datalog")
            ),
            // Bob
            Map.of(
                "person/name", "Bob",
                "person/email", "bob@example.com",
                "person/age", 25L,
                "person/skills", List.of("Python", "SQL")
            )
        ));

        // 3. Use unique identity to reference and update
        System.out.println("3. Using unique identity for upsert...");

        Datahike.transact(conn, List.of(
            Map.of(
                "person/name", "Alice",  // Upsert by unique identity
                "person/age", 31L        // Update age
            )
        ));

        // 4. Query relationships
        System.out.println("4. Querying people and skills...");

        Set<?> peopleWithSkills = (Set<?>) Datahike.q(
            """
            [:find ?name ?skill
             :where
             [?e :person/name ?name]
             [?e :person/skills ?skill]]
            """,
            Datahike.deref(conn)
        );

        System.out.println("People and their skills:");
        peopleWithSkills.forEach(System.out::println);

        // 5. Count aggregates
        Object skillCounts = Datahike.q(
            """
            [:find ?name (count ?skill)
             :where
             [?e :person/name ?name]
             [?e :person/skills ?skill]]
            """,
            Datahike.deref(conn)
        );

        System.out.println("\nSkill counts per person:");
        System.out.println(skillCounts);

        // 6. Verify schema
        System.out.println("\n5. Retrieving schema...");
        Map<?, ?> dbSchema = (Map<?, ?>) Datahike.schema(Datahike.deref(conn));

        System.out.println("Schema has " + dbSchema.size() + " attributes defined");

        // Cleanup
        Datahike.deleteDatabase(config);

        System.out.println("\n=== Example completed successfully! ===");
    }
}
