package examples;

import datahike.java.Datahike;
import datahike.java.Database;
import datahike.java.SchemaFlexibility;

import java.util.*;

/**
 * Quick start example demonstrating basic Datahike usage.
 *
 * This example shows:
 * - Database configuration with builder pattern
 * - Creating and connecting to a database
 * - Transacting data
 * - Querying with Datalog
 * - Cleanup
 */
public class QuickStart {
    public static void main(String[] args) {
        System.out.println("=== Datahike Java Quick Start ===\n");

        // 1. Configure database with builder pattern
        System.out.println("1. Configuring database...");
        Map<String, Object> config = Database.memory(UUID.randomUUID())
            .schemaFlexibility(SchemaFlexibility.READ)
            .keepHistory(true)
            .name("quickstart-example")
            .build();

        // 2. Create and connect to database
        System.out.println("2. Creating database...");
        Datahike.createDatabase(config);

        System.out.println("3. Connecting to database...");
        Object conn = Datahike.connect(config);

        // 3. Transact data using Java Maps
        System.out.println("4. Adding data...");
        Datahike.transact(conn, List.of(
            Map.of("name", "Alice", "age", 30, "city", "Berlin"),
            Map.of("name", "Bob", "age", 25, "city", "London"),
            Map.of("name", "Charlie", "age", 35, "city", "Berlin")
        ));

        // 4. Query data with Datalog
        System.out.println("5. Querying data...");

        // Find all people
        Set<?> allPeople = (Set<?>) Datahike.q(
            "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]",
            Datahike.deref(conn)
        );
        System.out.println("All people: " + allPeople);

        // Find people in Berlin
        Set<?> berliners = (Set<?>) Datahike.q(
            """
            [:find ?name ?age
             :where
             [?e :name ?name]
             [?e :age ?age]
             [?e :city "Berlin"]]
            """,
            Datahike.deref(conn)
        );
        System.out.println("People in Berlin: " + berliners);

        // Find people over 30
        Set<?> over30 = (Set<?>) Datahike.q(
            """
            [:find ?name
             :where
             [?e :name ?name]
             [?e :age ?age]
             [(>= ?age 30)]]
            """,
            Datahike.deref(conn)
        );
        System.out.println("People over 30: " + over30);

        // 5. Update data
        System.out.println("\n6. Updating data...");
        // Find Alice's entity ID first
        Set<?> aliceResult = (Set<?>) Datahike.q(
            "[:find ?e :where [?e :name \"Alice\"]]",
            Datahike.deref(conn)
        );

        if (!aliceResult.isEmpty()) {
            Object aliceId = ((List<?>) aliceResult.iterator().next()).get(0);

            // Update Alice's age
            Datahike.transact(conn, List.of(
                Map.of(":db/id", aliceId, "age", 31)
            ));

            System.out.println("Updated Alice's age to 31");
        }

        // Query again to see the update
        Set<?> updated = (Set<?>) Datahike.q(
            "[:find ?name ?age :where [?e :name \"Alice\"] [?e :age ?age] [?e :name ?name]]",
            Datahike.deref(conn)
        );
        System.out.println("Alice after update: " + updated);

        // 6. Cleanup
        System.out.println("\n7. Cleaning up...");
        Datahike.deleteDatabase(config);

        System.out.println("\n=== Example completed successfully! ===");
        System.exit(0);
    }
}
