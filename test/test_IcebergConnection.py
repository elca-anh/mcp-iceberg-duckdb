import pytest
from unittest.mock import Mock, patch
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, DoubleType, NestedField, TimestampType, TimestamptzType
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

# Import the class to test
from mcp_server_iceberg.IcebergConnection import IcebergConnection

class TestIcebergConnection:
    """Test suite for IcebergConnection class"""
    
    @pytest.fixture
    def connection(self) -> IcebergConnection:
        """Create a fresh IcebergConnection instance for each test"""
        return IcebergConnection()
    
    @pytest.fixture
    def mock_catalog(self):
        """Create a mock catalog for testing"""
        catalog = Mock(spec=Catalog)
        return catalog
    
    @pytest.fixture
    def mock_table(self):
        """Create a mock table for testing"""
        table = Mock(spec=Table)
        
        # Mock schema
        schema_fields = [
            NestedField(0, "id", IntegerType(), required=True),
            NestedField(1, "name", StringType(), required=False),
            NestedField(2, "score", DoubleType(), required=False)
        ]
        schema = Schema(*schema_fields)
        table.schema.return_value = schema
        
        # Mock spec and sort_order
        table.spec.return_value = Mock()
        table.spec.return_value.fields = []
        table.sort_order.return_value = Mock()
        table.sort_order.return_value.fields = []
        table.properties = {"test": "value"}
        
        return table       
    
    @patch('mcp_server_iceberg.IcebergConnection.load_catalog')
    def test_ensure_connection_success(self, mock_load_catalog, connection, mock_catalog):
        """Test successful catalog connection"""

        assert connection.catalog is None

        mock_load_catalog.return_value = mock_catalog
        
        result = connection._ensure_connection()
        
        mock_load_catalog.assert_called_once_with("iceberg")
        assert connection.catalog == mock_catalog
        assert result == mock_catalog
    
    @patch('mcp_server_iceberg.IcebergConnection.load_catalog')
    def test_ensure_connection_already_connected(self, mock_load_catalog, connection, mock_catalog):
        """Test that ensure_connection doesn't reconnect if already connected"""
        connection.catalog = mock_catalog
        
        result = connection._ensure_connection()
        
        mock_load_catalog.assert_not_called()
        assert result == mock_catalog
    
    @patch('mcp_server_iceberg.IcebergConnection.load_catalog')
    def test_ensure_connection_failure(self, mock_load_catalog, connection):
        """Test connection failure handling"""
        mock_load_catalog.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            connection._ensure_connection()

    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_list_namespaces(self, mock_ensure_connection, connection, mock_catalog):
        """Test LIST TABLES command"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.list_namespaces.return_value = [("default",), ("test",)]
        
        result = connection.query_catalog("LIST NAMESPACES")
        
        mock_catalog.list_namespaces.assert_called_once()
        expected = [
            {"namespace": "default"},
            {"namespace": "test"}
        ]
        assert result == expected

    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_list_specific_namespaces(self, mock_ensure_connection, connection, mock_catalog):
        """Test LIST TABLES command"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.list_namespaces.return_value = [("silver",), ("gold",)]
        
        result = connection.query_catalog("LIST NAMESPACES myNamespace")
        
        mock_catalog.list_namespaces.assert_called_once_with("myNamespace")
        expected = [
            {"namespace": "silver"},
            {"namespace": "gold"}
        ]
        assert result == expected

    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_list_tables(self, mock_ensure_connection, connection, mock_catalog):
        """Test LIST TABLES command"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.list_namespaces.return_value = [("default",), ("test",)]
        mock_catalog.list_tables.side_effect = [
            [("default", "table1"), ("default", "table2")],
            [("test", "table3")]
        ]
        
        result = connection.query_catalog("LIST TABLES")
        
        expected = [
            {"namespace": "default", "table": ("default", "table1")},
            {"namespace": "default", "table": ("default", "table2")},
            {"namespace": "test", "table": ("test", "table3")}
        ]
        assert result == expected

    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_list_specific_tables(self, mock_ensure_connection, connection, mock_catalog):
        """Test LIST TABLES IN myNamespace command"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.list_namespaces.return_value = []
        mock_catalog.list_tables.side_effect = [
            [("test", "table3")]
        ]
        
        result = connection.query_catalog("LIST TABLES IN test")
        
        expected = [           
            {"namespace": "test", "table": ("test", "table3")}
        ]
        assert result == expected
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_describe_table(self, mock_ensure_connection, connection, mock_catalog, mock_table):
        """Test DESCRIBE TABLE command"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.load_table.return_value = mock_table
        mock_table.schema.return_value = "test_schema"
        
        result = connection.query_catalog("DESCRIBE TABLE myschema.users")
        
        mock_catalog.load_table.assert_called_once_with("myschema.users")
        assert len(result) == 1
        assert "schema" in result[0]
        assert result[0]["schema"] == "test_schema"
        assert "partition_spec" in result[0]
        assert "sort_order" in result[0]
        assert "properties" in result[0]
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_unsupported(self, mock_ensure_connection, connection, mock_catalog):
        """Test unsupported catalog query"""
        mock_ensure_connection.return_value = mock_catalog
        
        with pytest.raises(ValueError, match="Unsupported catalog query type"):
            connection.query_catalog("INVALID QUERY")
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_table_select(self, mock_ensure_connection, connection, mock_catalog, mock_table):
        """Test SELECT query on table"""
        mock_ensure_connection.return_value = mock_catalog
        mock_catalog.load_table.return_value = mock_table
        
        # Mock scan and DuckDB connection
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        
        mock_conn = Mock()
        mock_scan.to_duckdb.return_value = mock_conn
        
        mock_sql_result = Mock()
        mock_conn.sql.return_value = mock_sql_result
        
        # PyArrow table
        data = {
            'id': pa.array([1, 2]),
            'name': pa.array(['Alice', 'Bob'])
        }
        mock_sql_result.arrow.return_value = pa.table(data)

        # Act  
        result = connection.query_table("SELECT * FROM users")
        
        # Assert
        mock_catalog.load_table.assert_called_once_with("users")
        mock_scan.to_duckdb.assert_called_once_with(table_name="users")
        assert isinstance(result, list)
        assert len(result) == 2
        # Assert result values
        for tag in ('id', 'name'):
            for idx in range(2):
                assert result[idx][tag] == data[tag][idx].as_py()
    
    @patch.object(IcebergConnection, '_ensure_connection')    
    def test_query_table_insert(self, mock_ensure_connection, connection, mock_catalog, mock_table):
        """Test INSERT query on table"""
        mock_ensure_connection.return_value = mock_catalog
        # mock_parse_sql.return_value = {"type": "INSERT", "table": "users"}
        mock_catalog.load_table.return_value = mock_table
        
        # Mock schema for INSERT
        schema_fields = [
            NestedField(0, "id", IntegerType(), required=True),
            NestedField(1, "name", StringType(), required=False),
            NestedField(2, "date", TimestampType(), required=False),
            NestedField(3, "datez", TimestamptzType(), required=False)
        ]
        schema = Schema(*schema_fields)
        mock_table.schema.return_value = schema
        
        # Act
        query = "INSERT INTO users VALUES (1, 'John', '2025-06-24 12:00:00+01:00', '2025-06-24 12:00:00+01:00')"
        result = connection.query_table(query)
            
        mock_table.append.assert_called_once()
        assert result == [{"status": "Inserted 1 row successfully"}]
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_table_create(self, mock_ensure_connection, connection, mock_catalog):
        """Test CREATE TABLE query"""
        mock_ensure_connection.return_value = mock_catalog
        
        query = "CREATE TABLE users (id INT, name STRING, score DOUBLE)"
        
        result = connection.query_table(query)
        
        # Verify create_table was called
        mock_catalog.create_table.assert_called_once()
        args, kwargs = mock_catalog.create_table.call_args
        assert args[0] == "users"  # table name
        assert isinstance(args[1], Schema) # schema
        assert len(args[1]) == 3
        assert args[1].fields[0].name == 'id'
        assert args[1].fields[0].field_type == IntegerType()
        assert result == [{"status": "Table created successfully"}]
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_table_unsupported(self, mock_ensure_connection, connection, mock_catalog):
        """Test unsupported table query type"""
        mock_ensure_connection.return_value = mock_catalog
        
        with pytest.raises(ValueError, match="Unsupported query type: DELETE"):
            connection.query_table("DELETE FROM users WHERE id = 1")
    
    def test_close(self, connection, mock_catalog):
        """Test close method"""
        connection.catalog = mock_catalog
        
        connection.close()
        
        assert connection.catalog is None
    
    def test_close_no_catalog(self, connection):
        """Test close method when no catalog is connected"""
        connection.close()  # Should not raise any exception
        assert connection.catalog is None
    
    @patch('mcp_server_iceberg.IcebergConnection.logger')
    def test_logging_behavior(self, mock_logger, connection):
        """Test that logging is properly called"""
        with patch.object(connection, '_ensure_connection') as mock_ensure:
            mock_ensure.side_effect = Exception("Test error")
            
            with pytest.raises(Exception):
                connection.query_catalog("LIST TABLES")
            
            mock_logger.error.assert_called()
    

# Performance and error handling tests
class TestIcebergConnectionErrorHandling:
    """Test error handling scenarios"""
    
    @pytest.fixture
    def connection(self):
        return IcebergConnection()
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_catalog_connection_error(self, mock_ensure_connection, connection):
        """Test error handling when connection fails"""
        mock_ensure_connection.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            connection.query_catalog("LIST TABLES")
    
    @patch.object(IcebergConnection, '_ensure_connection')
    def test_query_table_connection_error(self, mock_ensure_connection, connection):
        """Test error handling when connection fails during table query"""
        mock_ensure_connection.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            connection.query_table("SELECT * FROM users")
            
if __name__ == "__main__":
    pytest.main([__file__, "-v"])