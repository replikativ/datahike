# Libdatahike - C/C++ Native Library

Libdatahike provides a C/C++ interface to Datahike, allowing you to use Datahike databases from native applications. This library is built using GraalVM Native Image.

## Maturity

This feature of Datahike is still early and is subject to change.

## Building

To build the native library and test executable you need [GraalVM-JDK](https://www.graalvm.org/latest/getting-started/) installed with `native-compile`, [babashka](https://babashka.org/) and [Clojure](https://clojure.org/guides/install_clojure).

```bash
# Compile the native library
bb ni-compile

# Compile the C++ test executable
./libdatahike/compile-cpp
```

This will create:
- `libdatahike/target/libdatahike.so` - The shared library
- `libdatahike/target/test_cpp` - Test executable

## API Functions

The library provides the following C functions (defined in `libdatahike/target/libdatahike.h`):

### Database Operations
- `create_database(thread, config, format, callback)` - Create a new database
- `database_exists(thread, config, format, callback)` - Check if database exists
- `delete_database(thread, config, format, callback)` - Delete a database

### Data Operations
- `transact(thread, config, input_format, data, output_format, callback)` - Execute transactions
- `query(thread, query, num_inputs, input_formats, inputs, output_format, callback)` - Execute queries
- `pull(thread, config, format, pattern, entity_id, output_format, callback)` - Pull entity data
- `pull_many(thread, config, format, pattern, entity_ids, output_format, callback)` - Pull multiple entities
- `entity(thread, config, format, entity_id, output_format, callback)` - Get entity
- `datoms(thread, config, format, index, components, callback)` - Get datoms
- `schema(thread, config, format, output_format, callback)` - Get schema
- `reverse_schema(thread, config, format, output_format, callback)` - Get reverse schema

### Utility Operations
- `metrics(thread, config, format, output_format, callback)` - Get database metrics
- `gc_storage(thread, config, older_than, output_format, callback)` - Garbage collect storage

## Usage Example

### C++ Example

Please take a look at the C++ example that is part of our test suite: [../libdatahike/src/test_cpp.cpp]

## Data Formats

The library supports multiple data formats:
- `"edn"` - Extensible Data Notation (default)
- `"json"` - JSON format
- `"cbor"` - RFC 8949 Concise Binary Object Representation

## Error Handling

Errors are returned through the callback functions as strings. Always check the callback results for error conditions.

## Memory Management

- The GraalVM isolate should be created once and reused
- Callback functions receive string data that should be processed immediately
- The library handles memory management for internal operations

## Thread Safety

Each thread requires its own `graal_isolatethread_t`. For multi-threaded applications, create separate thread contexts for each thread that will use the library.

## Building Your Application

When building applications that use libdatahike:

```bash
# Compile your C++ code
g++ -I./libdatahike/target -L./libdatahike/target -ldatahike -o your_app your_app.cpp

# Run with library path
LD_LIBRARY_PATH=./libdatahike/target ./your_app

# or on MacOS
DYLD_LIBRARY_PATH=./libdatahike/target ./your_app
```
