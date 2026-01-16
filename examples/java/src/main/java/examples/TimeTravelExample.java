package examples;

import datahike.java.Datahike;
import datahike.java.Database;
import datahike.java.SchemaFlexibility;

import java.time.Instant;
import java.util.*;

/**
 * Example demonstrating time-travel queries.
 *
 * Shows:
 * - Querying historical database states
 * - Using asOf for point-in-time queries
 * - Using since for change tracking
 * - Querying full history with all versions
 */
public class TimeTravelExample {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Time Travel Example ===\n");

        // Configure database with history enabled
        Map<String, Object> config = Database.memory(UUID.randomUUID())
            .keepHistory(true)
            .schemaFlexibility(SchemaFlexibility.READ)
            .build();

        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);

        // 1. Initial state - add Alice
        System.out.println("1. Initial state - adding Alice...");
        Datahike.transact(conn, List.of(
            Map.of("name", "Alice", "status", "active")
        ));

        Date t1 = new Date();
        System.out.println("Timestamp T1: " + t1);

        Thread.sleep(100);  // Small delay for distinct timestamps

        // 2. Add Bob
        System.out.println("\n2. Adding Bob...");
        Datahike.transact(conn, List.of(
            Map.of("name", "Bob", "status", "active")
        ));

        Date t2 = new Date();
        System.out.println("Timestamp T2: " + t2);

        Thread.sleep(100);

        // 3. Update Alice's status
        System.out.println("\n3. Updating Alice's status...");
        Set<?> aliceResult = (Set<?>) Datahike.q(
            "[:find ?e :where [?e :name \"Alice\"]]",
            Datahike.deref(conn)
        );

        Object aliceId = ((List<?>) aliceResult.iterator().next()).get(0);
        Datahike.transact(conn, List.of(
            Map.of(":db/id", aliceId, "status", "inactive")
        ));

        Date t3 = new Date();
        System.out.println("Timestamp T3: " + t3);

        // 4. Query current state
        System.out.println("\n4. Current state:");
        Set<?> currentState = (Set<?>) Datahike.q(
            "[:find ?name ?status :where [?e :name ?name] [?e :status ?status]]",
            Datahike.deref(conn)
        );
        currentState.forEach(System.out::println);

        // 5. Query as of T1 (only Alice existed)
        System.out.println("\n5. State as of T1 (only Alice):");
        Object dbAtT1 = Datahike.asOf(Datahike.deref(conn), t1);
        Set<?> stateAtT1 = (Set<?>) Datahike.q(
            "[:find ?name ?status :where [?e :name ?name] [?e :status ?status]]",
            dbAtT1
        );
        stateAtT1.forEach(System.out::println);

        // 6. Query as of T2 (both existed, Alice was active)
        System.out.println("\n6. State as of T2 (both, Alice active):");
        Object dbAtT2 = Datahike.asOf(Datahike.deref(conn), t2);
        Set<?> stateAtT2 = (Set<?>) Datahike.q(
            "[:find ?name ?status :where [?e :name ?name] [?e :status ?status]]",
            dbAtT2
        );
        stateAtT2.forEach(System.out::println);

        // 7. Query changes since T2
        System.out.println("\n7. Changes since T2:");
        Object dbSinceT2 = Datahike.since(Datahike.deref(conn), t2);
        Set<?> changesSinceT2 = (Set<?>) Datahike.q(
            "[:find ?name ?status :where [?e :name ?name] [?e :status ?status]]",
            dbSinceT2
        );
        System.out.println("Changed entities:");
        changesSinceT2.forEach(System.out::println);

        // 8. Query full history (includes all versions)
        System.out.println("\n8. Full history of Alice's status:");
        Object historyDb = Datahike.history(Datahike.deref(conn));
        Set<?> aliceHistory = (Set<?>) Datahike.q(
            "[:find ?status :where [?e :name \"Alice\"] [?e :status ?status]]",
            historyDb
        );
        System.out.println("All status values for Alice:");
        aliceHistory.forEach(System.out::println);

        // Cleanup
        Datahike.deleteDatabase(config);

        System.out.println("\n=== Example completed successfully! ===");
        System.exit(0);
    }
}
