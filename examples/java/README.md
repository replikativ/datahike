# Datahike Java Examples

This directory contains example applications demonstrating the Datahike Java API.

## Prerequisites

- Java 17 or higher (required for text blocks and modern Java features)
- Maven 3.6+

## Running the Examples

### Option 1: Maven Exec (Recommended)

Run examples directly with Maven:

```bash
# Quick Start
mvn compile exec:java -Dexec.mainClass="examples.QuickStart"

# Schema Example
mvn compile exec:java -Dexec.mainClass="examples.SchemaExample"

# Time Travel Example
mvn compile exec:java -Dexec.mainClass="examples.TimeTravelExample"
```

### Option 2: Build and Run JAR

Build the project and run the JAR:

```bash
# Build
mvn clean package

# Run (using QuickStart as main class from pom.xml)
java -jar target/datahike-java-examples-1.0.0-SNAPSHOT.jar

# Or run specific examples
java -cp target/datahike-java-examples-1.0.0-SNAPSHOT.jar examples.SchemaExample
```

### Option 3: IDE

Import the Maven project into your IDE (IntelliJ IDEA, Eclipse, VSCode) and run the main classes directly.

## Examples

### QuickStart.java

Basic introduction to Datahike:
- Database configuration with builder pattern
- Creating and connecting to databases
- Transacting data with Java Maps
- Querying with Datalog
- Updates and cleanup

**Key concepts:** Builder pattern, basic queries, CRUD operations

### SchemaExample.java

Schema definition and validation:
- Defining attributes with type constraints
- Unique constraints (identity vs value)
- Reference types for relationships
- Cardinality (one vs many)
- Using Keywords constants

**Key concepts:** Schema definition, relationships, data validation

### TimeTravelExample.java

Historical queries and time travel:
- Querying database state at specific points in time
- Using `asOf` for point-in-time queries
- Using `since` for change tracking
- Querying full history
- Transaction metadata

**Key concepts:** Immutability, history, audit trails

## Project Structure

```
examples/java/
├── pom.xml                                 # Maven configuration
├── README.md                               # This file
└── src/main/java/examples/
    ├── QuickStart.java                     # Basic usage
    ├── SchemaExample.java                  # Schema definition
    └── TimeTravelExample.java              # Time travel queries
```

## Using Datahike in Your Project

To use Datahike in your own Maven project, add to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>clojars</id>
        <url>https://repo.clojars.org/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.replikativ</groupId>
        <artifactId>datahike</artifactId>
        <version>LATEST_VERSION</version>
    </dependency>
</dependencies>
```

For Gradle:

```gradle
repositories {
    maven { url "https://repo.clojars.org/" }
}

dependencies {
    implementation 'io.replikativ:datahike:LATEST_VERSION'
}
```

**Note:** Replace `LATEST_VERSION` with the latest published version from [Clojars](https://clojars.org/io.replikativ/datahike).

## Key Java API Classes

- **`Datahike`** - Main API with all database operations
- **`Database`** - Fluent builder for configuration
- **`Keywords`** - Pre-defined constants for schema
- **`SchemaFlexibility`** - Enum for schema modes
- **`Util`** - Low-level utilities (map, vec, kwd)
- **`EDN`** - EDN data type constructors

## Further Reading

- [Java API Documentation](../../doc/java-api.md) - Comprehensive API guide
- [Main README](../../README.md) - Project overview
- [Schema Guide](../../doc/schema.md) - Detailed schema documentation
- [Storage Backends](../../doc/storage-backends.md) - Backend configuration
- [Datalog Tutorial](https://docs.datomic.com/on-prem/query.html) - Query language

## Troubleshooting

**Build fails with "Could not find artifact"**
- Ensure Clojars repository is configured
- Check internet connection
- Try `mvn clean install -U` to force update

**Runtime errors about Clojure**
- Datahike includes Clojure as a dependency
- Check for dependency conflicts with `mvn dependency:tree`

**ClassCastException in query results**
- Query results are Clojure collections (Set, List, Map)
- Cast appropriately: `(Set<?>) Datahike.q(...)`

## License

Eclipse Public License 1.0 (EPL-1.0)
