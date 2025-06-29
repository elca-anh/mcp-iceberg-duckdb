""" Test of class QueryManager in charge of handling the SQL query"""

import pytest

# Import the class to test
from mcp_server_iceberg.QueryManager import QueryManager

# Fixtures for parameterized tests
@pytest.mark.parametrize("query,expected_type", [
    ("SELECT * FROM users", "SELECT"),
    ("SELECT name FROM customers WHERE active = 1", "SELECT"),
    ("SELECT 1 + 1", "SELECT"),
    ("INSERT INTO orders VALUES (1, 100)", "INSERT"),
    ("INSERT INTO test.users (id, name) VALUES (1, 'John')", "INSERT"),
    ("UPDATE products SET price = 10", "UPDATE"),
    ("UPDATE users SET name = 'Jane' WHERE id = 1", "UPDATE"),
    ("DELETE FROM logs WHERE date < '2023-01-01'", "DELETE"),
    ("CREATE TABLE new_table (id INT)", "CREATE"),
    ("CREATE TABLE IF NOT EXISTS users (id INT, name STRING)", "CREATE"),
    ("LIST NAMESPACES", "LIST"),
    ("LIST TABLES", "LIST"),
    ("LIST TABLES myNamespace", "LIST"),
    ("DESCRIBE TABLE silver.myTable", "DESCRIBE"),
    ("", None),
    ("   \n  \t  ", None),
    ("INVALID SQL QUERY", None)
])
def test_parse_sql_parametrized(query, expected_type):
    """Parametrized test for SQL parsing"""
    parsedQuery = QueryManager(query)
    
    assert parsedQuery.type == expected_type

# Fixtures for parameterized tests
@pytest.mark.parametrize("query,expected_subtype, expected_argument", [
    ("LIST NAMESPACES", "NAMESPACES", None),
    ("LIST NAMESPACES myNamespace", "NAMESPACES", "myNamespace"),
    ("LIST NAMESPACES IN myNamespace", "NAMESPACES", "myNamespace"),
    ("LIST TABLES", "TABLES", None),
    ("LIST TABLES myNamespace", "TABLES", "myNamespace"),
    ("LIST TABLES IN myNamespace", "TABLES", "myNamespace"),
    ("LIST TABLES myNamespace.subnamespace", "TABLES", "myNamespace.subnamespace")
])
def test_parse_sql_list_parametrized(query, expected_subtype, expected_argument):
    """Parametrized test for SQL SELECT table name parsing"""
    parsedQuery = QueryManager(query)
    
    (subtype, argument) = parsedQuery.extract_list_arguments()
    
    assert subtype == expected_subtype
    assert argument == expected_argument


# Fixtures for parameterized tests
@pytest.mark.parametrize("query,expected_table_name, expected_schema", [
    ("CREATE TABLE silver.new_table (id INT)", "silver.new_table", {'id': 'INT'}),
    ("CREATE TABLE IF NOT EXISTS users (id INT, name string)", "users", {'id': 'INT', 'name': 'STRING'}), # first NVARCHAR(64))
])
def test_parse_sql_create_Table_parametrized(query, expected_table_name, expected_schema):
    """Parametrized test for SQL SELECT table name parsing"""
    parsedQuery = QueryManager(query)
    
    (table_name, schema) = parsedQuery.extract_create_table_arguments()
    
    assert table_name == expected_table_name
    assert schema == expected_schema

# Fixtures for parameterized tests
@pytest.mark.parametrize("query,expected_table", [
    ("SELECT * FROM users", "users"),
    ("SELECT * FROM users u", "users"),
    ("SELECT * FROM users AS u", "users"),
    ("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id", "users"),
    ("SELECT * FROM schema.table_name LIMIT(5)", "schema.table_name"),
    ("SELECT name FROM customers WHERE active = 1", "customers"),
    ("SELECT 1 + 1", None)
])
def test_parse_sql_select_table_parametrized(query, expected_table):
    """Parametrized test for SQL SELECT table name parsing"""
    parsedQuery = QueryManager(query)
    
    table_name = parsedQuery.extract_select_table()
    
    assert table_name == expected_table

# Fixtures for parameterized tests
@pytest.mark.parametrize("query,expected_table, expected_values", [
    ("INSERT INTO orders VALUES (1, 100)", "orders", [1, 100]),
    ("INSERT INTO test.users (id, name) VALUES (1, 'John')", "test.users", [1, 'John']),
])
def test_parse_sql_insert_table_parametrized(query, expected_table,expected_values):
    """Parametrized test for SQL INSERT table name parsing"""
    parsedQuery = QueryManager(query)
    
    (table_name, values) = parsedQuery.extract_insert_table_values()
    
    assert table_name == expected_table
    assert values == expected_values

            
if __name__ == "__main__":
    pytest.main([__file__, "-v"])