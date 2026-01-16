"""Basic tests for Datahike Python bindings."""
import json
import uuid
import sys
import os

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datahike import (
    create_database,
    delete_database,
    database_exists,
    transact,
    q,
    pull,
    schema,
    DatahikeException,
)


def test_database_lifecycle():
    """Test create, exists, delete database."""
    db_id = str(uuid.uuid4())
    config = f'{{:store {{:backend :memory :id #uuid "{db_id}"}}}}'

    print(f"Using config: {config}")

    # Create
    print("Creating database...")
    create_database(config)

    # Check exists
    print("Checking if database exists...")
    exists = database_exists(config)
    print(f"Database exists: {exists}")
    assert exists == True

    # Delete
    print("Deleting database...")
    delete_database(config)

    # Check deleted
    exists = database_exists(config)
    print(f"Database exists after delete: {exists}")
    assert exists == False

    print("SUCCESS: Database lifecycle test passed!")


def test_transact_and_query_with_schema_flexibility():
    """Test transact and query with schema-flexibility :read (flexible schema)."""
    db_id = str(uuid.uuid4())
    config = f'{{:store {{:backend :memory :id #uuid "{db_id}"}} :schema-flexibility :read}}'

    print(f"\nUsing config: {config}")

    try:
        # Create database
        print("Creating database...")
        create_database(config)

        # Transact data using JSON format
        print("Transacting data...")
        tx_data = json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ])
        result = transact(config, tx_data, input_format='json')
        print(f"Transaction result: {result}")

        # Query for Alice
        print("Querying for Alice...")
        query_result = q(
            '[:find ?e ?name :where [?e :name ?name] [?e :name "Alice"]]',
            [('db', config)],
            output_format='json'
        )
        print(f"Query result: {query_result}")
        assert len(query_result) > 0, "Should find Alice"

        # Pull entity
        print("Pulling entity...")
        # Query result is ['!set', [[eid, name], ...]]
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        entity_id = actual_results[0][0]
        entity = pull(config, '[*]', entity_id, output_format='json')
        print(f"Pulled entity: {entity}")
        # In JSON mode, keywords don't have : prefix
        assert entity.get('name') == 'Alice' or entity.get(':name') == 'Alice'

        print("SUCCESS: Transact and query test passed!")

    finally:
        # Cleanup
        print("Cleaning up...")
        delete_database(config)


def test_transact_with_explicit_schema():
    """Test transact and query with explicit schema."""
    db_id = str(uuid.uuid4())
    config = f'{{:store {{:backend :memory :id #uuid "{db_id}"}}}}'

    print(f"\nUsing config: {config}")

    try:
        # Create database
        print("Creating database...")
        create_database(config)

        # First transact schema using EDN format (keywords need proper representation)
        print("Transacting schema...")
        schema_tx = '[{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one} {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]'
        transact(config, schema_tx, input_format='edn')

        # Check schema was added
        print("Checking schema...")
        db_schema = schema(config, output_format='json')
        print(f"Schema: {db_schema}")

        # Transact data using EDN (to preserve Long type for :age)
        print("Transacting data...")
        tx_data = '[{:name "Charlie" :age 35} {:name "Diana" :age 28}]'
        transact(config, tx_data, input_format='edn')

        # Query
        print("Querying...")
        query_result = q(
            '[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]',
            [('db', config)],
            output_format='json'
        )
        print(f"Query result: {query_result}")
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        assert len(actual_results) == 2, f"Expected 2 results, got {len(actual_results)}"

        print("SUCCESS: Explicit schema test passed!")

    finally:
        # Cleanup
        print("Cleaning up...")
        delete_database(config)


def test_exception_handling():
    """Test that exceptions are properly raised."""
    db_id = str(uuid.uuid4())
    config = f'{{:store {{:backend :memory :id #uuid "{db_id}"}}}}'

    print("\nTesting exception handling...")

    try:
        # Try to query non-existent database
        q('[:find ?e :where [?e :name "test"]]', [('db', config)])
        assert False, "Should have raised exception"
    except DatahikeException as e:
        print(f"Caught expected exception: {e}")
        print("SUCCESS: Exception handling test passed!")


if __name__ == '__main__':
    test_database_lifecycle()
    test_transact_and_query_with_schema_flexibility()
    test_transact_with_explicit_schema()
    test_exception_handling()
    print("\n=== ALL TESTS PASSED ===")
