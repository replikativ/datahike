"""Tests for EDN conversion rules.

Tests the universal Python → EDN conversion that follows the rules:
- Keys: always keywordized
- Values: : prefix = keyword, else literal
- Escape: \\: for literal colon strings
"""
import pytest
from datahike.database import _key_to_keyword, _value_to_edn, _dict_to_edn, _python_to_edn
from datahike import edn, kw


class TestKeyConversion:
    """Test key → keyword conversion."""

    def test_simple_key(self):
        assert _key_to_keyword("name") == ":name"

    def test_namespaced_key(self):
        assert _key_to_keyword("person/name") == ":person/name"

    def test_key_with_leading_colon(self):
        # Leading : is stripped (convenience)
        assert _key_to_keyword(":db/id") == ":db/id"

    def test_key_with_question_mark(self):
        assert _key_to_keyword("keep-history?") == ":keep-history?"

    def test_key_with_dash(self):
        assert _key_to_keyword("schema-flexibility") == ":schema-flexibility"


class TestValueConversion:
    """Test value conversion rules."""

    def test_string_without_colon(self):
        assert _value_to_edn("Alice") == '"Alice"'

    def test_string_with_colon_prefix(self):
        # : prefix → keyword
        assert _value_to_edn(":active") == ":active"

    def test_string_with_escaped_colon(self):
        # \\: → literal string with :
        assert _value_to_edn("\\:literal") == '":literal"'

    def test_string_with_quotes(self):
        # Quotes should be escaped
        assert _value_to_edn('say "hello"') == '"say \\"hello\\""'

    def test_integer(self):
        assert _value_to_edn(42) == "42"

    def test_float(self):
        assert _value_to_edn(3.14) == "3.14"

    def test_boolean_true(self):
        assert _value_to_edn(True) == "true"

    def test_boolean_false(self):
        assert _value_to_edn(False) == "false"

    def test_none(self):
        assert _value_to_edn(None) == "nil"

    def test_list(self):
        assert _value_to_edn([1, 2, 3]) == "[1 2 3]"

    def test_nested_list(self):
        assert _value_to_edn([1, [2, 3]]) == "[1 [2 3]]"

    def test_nested_dict(self):
        result = _value_to_edn({"a": 1})
        assert result == "{:a 1}"


class TestDictConversion:
    """Test dict → EDN map conversion."""

    def test_simple_dict(self):
        assert _dict_to_edn({"name": "Alice"}) == '{:name "Alice"}'

    def test_dict_with_keyword_value(self):
        assert _dict_to_edn({"status": ":active"}) == '{:status :active}'

    def test_dict_with_multiple_keys(self):
        result = _dict_to_edn({"name": "Alice", "age": 30})
        # Dict order might vary, check both possibilities
        assert result in [
            '{:name "Alice" :age 30}',
            '{:age 30 :name "Alice"}'
        ]

    def test_nested_dict(self):
        result = _dict_to_edn({
            "store": {
                "backend": ":mem",
                "id": "test"
            }
        })
        assert ":store" in result
        assert ":backend :mem" in result
        assert ':id "test"' in result


class TestCompleteConversion:
    """Test complete Python → EDN conversion."""

    def test_config_conversion(self):
        config = {
            "store": {
                "backend": ":mem",
                "id": "test"
            },
            "schema-flexibility": ":read",
            "keep-history?": True
        }
        edn = _python_to_edn(config)

        assert ":store" in edn
        assert ":backend :mem" in edn
        assert ':id "test"' in edn
        assert ":schema-flexibility :read" in edn
        assert ":keep-history? true" in edn

    def test_transaction_data(self):
        data = [
            {"name": "Alice", "status": ":active"},
            {"name": "Bob", "status": ":inactive"}
        ]
        edn = _python_to_edn(data)

        assert ':name "Alice"' in edn
        assert ":status :active" in edn
        assert ':name "Bob"' in edn
        assert ":status :inactive" in edn

    def test_schema_transaction(self):
        schema = [{
            "db/ident": ":person/name",
            "db/valueType": ":db.type/string",
            "db/cardinality": ":db.cardinality/one"
        }]
        edn = _python_to_edn(schema)

        assert ":db/ident :person/name" in edn
        assert ":db/valueType :db.type/string" in edn
        assert ":db/cardinality :db.cardinality/one" in edn


class TestEDNHelpers:
    """Test explicit EDN type helpers."""

    def test_keyword_simple(self):
        kw = edn.keyword("name")
        assert kw.to_edn() == ":name"

    def test_keyword_namespaced(self):
        kw = edn.keyword("name", "person")
        assert kw.to_edn() == ":person/name"

    def test_symbol(self):
        sym = edn.symbol("my-fn")
        assert sym.to_edn() == "my-fn"

    def test_symbol_namespaced(self):
        sym = edn.symbol("assoc", "clojure.core")
        assert sym.to_edn() == "clojure.core/assoc"

    def test_uuid(self):
        u = edn.uuid("550e8400-e29b-41d4-a716-446655440000")
        assert u.to_edn() == '#uuid "550e8400-e29b-41d4-a716-446655440000"'

    def test_inst(self):
        i = edn.inst("2024-01-01T00:00:00Z")
        assert i.to_edn() == '#inst "2024-01-01T00:00:00Z"'

    def test_forced_string(self):
        s = edn.string(":literal")
        assert s.startswith("<STRING>")
        # When converted to EDN, should be quoted string
        assert _value_to_edn(s) == '":literal"'

    def test_keyword_in_dict(self):
        data = {
            "db/ident": edn.keyword("person/name"),
            "db/valueType": kw.STRING
        }
        result = _python_to_edn(data)

        assert ":db/ident :person/name" in result
        assert ":db/valueType :db.type/string" in result


class TestKeywordConstants:
    """Test pre-defined keyword constants."""

    def test_db_id(self):
        assert kw.DB_ID == ":db/id"

    def test_db_ident(self):
        assert kw.DB_IDENT == ":db/ident"

    def test_value_types(self):
        assert kw.STRING == ":db.type/string"
        assert kw.LONG == ":db.type/long"
        assert kw.BOOLEAN == ":db.type/boolean"
        assert kw.REF == ":db.type/ref"

    def test_cardinality(self):
        assert kw.ONE == ":db.cardinality/one"
        assert kw.MANY == ":db.cardinality/many"

    def test_unique(self):
        assert kw.UNIQUE_VALUE == ":db.unique/value"
        assert kw.UNIQUE_IDENTITY == ":db.unique/identity"

    def test_schema_flexibility(self):
        assert kw.SCHEMA_READ == ":read"
        assert kw.SCHEMA_WRITE == ":write"


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_dict(self):
        assert _dict_to_edn({}) == "{}"

    def test_empty_list(self):
        assert _value_to_edn([]) == "[]"

    def test_deeply_nested(self):
        data = {
            "a": {
                "b": {
                    "c": {
                        "d": "value"
                    }
                }
            }
        }
        edn = _python_to_edn(data)
        assert ":a" in edn
        assert ":b" in edn
        assert ":c" in edn
        assert ":d" in edn
        assert '"value"' in edn

    def test_list_of_dicts(self):
        data = [
            {"name": "Alice"},
            {"name": "Bob"}
        ]
        edn = _python_to_edn(data)
        assert '[' in edn
        assert '{:name "Alice"}' in edn
        assert '{:name "Bob"}' in edn

    def test_dict_with_list_value(self):
        data = {"tags": [":important", ":verified"]}
        edn = _python_to_edn(data)
        assert ":tags" in edn
        assert ":important" in edn
        assert ":verified" in edn

    def test_string_with_slash(self):
        # Slash in regular string should be preserved
        result = _value_to_edn("path/to/file")
        assert result == '"path/to/file"'

    def test_string_with_backslash(self):
        # Backslash should be escaped
        result = _value_to_edn("C:\\path\\to\\file")
        assert result == '"C:\\\\path\\\\to\\\\file"'


class TestCustomBackendScenario:
    """Test conversion for custom backend configurations."""

    def test_custom_s3_backend(self):
        config = {
            "store": {
                "backend": ":my-s3",
                "bucket": "my-bucket",
                "region": "us-west-2",
                "encryption": {
                    "type": ":aes256",
                    "key-id": "secret"
                }
            }
        }
        edn = _python_to_edn(config)

        assert ":backend :my-s3" in edn
        assert ':bucket "my-bucket"' in edn
        assert ':region "us-west-2"' in edn
        assert ":encryption" in edn
        assert ":type :aes256" in edn
        assert ':key-id "secret"' in edn
