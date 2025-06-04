import logging
import time
from typing import Any, Dict
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import *
from pyiceberg.schema import Schema
from pyiceberg.types import *
import sqlparse
from sqlparse.tokens import DML
import pyarrow as pa

logger = logging.getLogger('iceberg_server')

class IcebergConnection:
    """
    Iceberg catalog connection management class
    """
    def __init__(self):
        self.catalog = None
            
    def ensure_connection(self):
        """
        Ensure catalog connection is available
        """
        try:
            if self.catalog is None:
                logger.info("Creating new Iceberg catalog connection for catalog 'iceberg'")
                self.catalog = load_catalog("iceberg")
                logger.info("New catalog connection established")
            return self.catalog
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            raise

    def parse_sql(self, query: str) -> Dict:
        """
        Parse SQL query and extract relevant information
        
        Args:
            query (str): SQL query to parse
            
        Returns:
            Dict: Parsed query information
        """
        parsed = sqlparse.parse(query)[0]
        tokens = [token for token in parsed.tokens if not token.is_whitespace]
        
        result = {
            "type": None,
            "table": None,
        }
        
        # Determine query type
        for token in tokens:
            if token.ttype is DML:
                result["type"] = token.value.upper()
                break
        
        # Extract table name
        for i, token in enumerate(tokens):
            if token.value.upper() == "FROM":
                if i + 1 < len(tokens):
                    result["table"] = tokens[i + 1].value
                break
        
        return result

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """
        Execute query on Iceberg tables
        
        Args:
            query (str): Query to execute
            
        Returns:
            list[dict[str, Any]]: Query results
        """
        start_time = time.time()
        logger.info(f"Executing query: {query[:200]}...")
        
        try:
            catalog = self.ensure_connection()
            query_upper = query.strip().upper()
            
            # Handle special commands
            if query_upper.startswith("LIST TABLES"):
                results = []
                for namespace in catalog.list_namespaces():
                    for table in catalog.list_tables(namespace):
                        results.append({
                            "namespace": ".".join(namespace),
                            "table": table
                        })
                logger.info(f"Listed {len(results)} tables in {time.time() - start_time:.2f}s")
                return results
            
            elif query_upper.startswith("DESCRIBE TABLE"):
                table_name = query[len("DESCRIBE TABLE"):].strip()
                table = catalog.load_table(table_name)
                schema_dict = {
                    "schema": str(table.schema()),
                    "partition_spec": [str(field) for field in (table.spec().fields if table.spec() else [])],
                    "sort_order": [str(field) for field in (table.sort_order().fields if table.sort_order() else [])],
                    "properties": table.properties
                }
                return [schema_dict]
            
            # Handle SQL queries
            parsed = self.parse_sql(query)
            
            if parsed["type"] == "SELECT":

                # Load and scan table
                table = catalog.load_table(parsed["table"])
                scan = table.scan()
                
                # Use DuckDB to postprocess the table
                # to_duckdb() is using register() which is in schema main -> "flatten" the original table name
                table_name2 = parsed["table"].replace('.', '_')
                conn = scan.to_duckdb(table_name=table_name2) # Full scan at this time, to optimize
                query_duck = query.replace(parsed["table"], "main." + table_name2)
                duckdb_result = conn.sql(query_duck)
                arrow_table = duckdb_result.arrow()
                
                # Convert PyArrow Table to list of dicts
                results = []
                for batch in arrow_table.to_batches():
                    for row_idx in range(len(batch)):
                        row_dict = {}
                        for col_name in batch.schema.names:
                            val = batch[col_name][row_idx].as_py()
                            row_dict[col_name] = val
                        results.append(row_dict)
                
                logger.info(f"Query returned {len(results)} rows in {time.time() - start_time:.2f}s")
                return results
            
            elif parsed["type"] == "INSERT":
                # Extract table name and values
                table_name = None
                values = []
                
                # Parse INSERT INTO table_name VALUES (...) syntax
                parsed_stmt = sqlparse.parse(query)[0]
                logger.info(f"Parsed statement: {parsed_stmt}")
                
                # Find the VALUES token and extract values
                values_token = None
                table_identifier = None
                
                for token in parsed_stmt.tokens:
                    logger.info(f"Token: {token}, Type: {token.ttype}, Value: {token.value}")
                    if isinstance(token, sqlparse.sql.Identifier):
                        table_identifier = token
                    elif token.value.upper() == 'VALUES':
                        values_token = token
                        break
                
                if table_identifier:
                    # Handle multi-part identifiers (e.g., schema.table)
                    table_name = str(table_identifier)
                    logger.info(f"Found table name: {table_name}")
                
                if values_token and len(parsed_stmt.tokens) > parsed_stmt.tokens.index(values_token) + 1:
                    next_token = parsed_stmt.tokens[parsed_stmt.tokens.index(values_token) + 1]
                    if isinstance(next_token, sqlparse.sql.Parenthesis):
                        values_str = next_token.value.strip('()').split(',')
                        values = []
                        for v in values_str:
                            v = v.strip()
                            if v.startswith("'") and v.endswith("'"):
                                values.append(v.strip("'"))
                            elif v.lower() == 'true':
                                values.append(True)
                            elif v.lower() == 'false':
                                values.append(False)
                            elif v.lower() == 'null':
                                values.append(None)
                            else:
                                try:
                                    values.append(int(v))
                                except ValueError:
                                    try:
                                        values.append(float(v))
                                    except ValueError:
                                        values.append(v)
                        logger.info(f"Extracted values: {values}")
                
                if not table_name or values is None:
                    raise ValueError(f"Invalid INSERT statement format. Table: {table_name}, Values: {values}")
                
                logger.info(f"Inserting into table: {table_name}")
                logger.info(f"Values: {values}")
                
                # Load table and schema
                table = catalog.load_table(table_name)
                schema = table.schema()
                
                # Create PyArrow arrays for each field
                arrays = []
                names = []
                for i, field in enumerate(schema.fields):
                    names.append(field.name)
                    value = values[i] if i < len(values) else None
                    if isinstance(field.field_type, IntegerType):
                        arrays.append(pa.array([value], type=pa.int32()))
                    elif isinstance(field.field_type, StringType):
                        arrays.append(pa.array([value], type=pa.string()))
                    elif isinstance(field.field_type, BooleanType):
                        arrays.append(pa.array([value], type=pa.bool_()))
                    elif isinstance(field.field_type, DoubleType):
                        arrays.append(pa.array([value], type=pa.float64()))
                    elif isinstance(field.field_type, TimestampType):
                        arrays.append(pa.array([value], type=pa.timestamp('us')))
                    else:
                        arrays.append(pa.array([value], type=pa.string()))
                
                # Create PyArrow table
                pa_table = pa.Table.from_arrays(arrays, names=names)
                
                # Append the PyArrow table directly to the Iceberg table
                table.append(pa_table)
                
                return [{"status": "Inserted 1 row successfully"}]
            
            elif parsed["type"] == "CREATE":
                # Basic CREATE TABLE support
                if "CREATE TABLE" in query_upper:
                    # Extract table name and schema
                    parts = query.split("(", 1)
                    table_name = parts[0].replace("CREATE TABLE", "").strip()
                    schema_str = parts[1].strip()[:-1]  # Remove trailing )
                    
                    # Parse schema definition
                    schema_fields = []
                    for field in schema_str.split(","):
                        name, type_str = field.strip().split(" ", 1)
                        type_str = type_str.upper()
                        if "STRING" in type_str:
                            field_type = StringType()
                        elif "INT" in type_str:
                            field_type = IntegerType()
                        elif "DOUBLE" in type_str:
                            field_type = DoubleType()
                        elif "TIMESTAMP" in type_str:
                            field_type = TimestampType()
                        else:
                            field_type = StringType()
                        schema_fields.append(NestedField(len(schema_fields), name, field_type, required=False))
                    
                    schema = Schema(*schema_fields)
                    catalog.create_table(table_name, schema)
                    return [{"status": "Table created successfully"}]
            
            else:
                raise ValueError(f"Unsupported query type: {parsed['type']}")
                
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise

    def close(self):
        """
        Clean up resources
        """
        if self.catalog:
            logger.info("Cleaning up catalog resources")
            self.catalog = None