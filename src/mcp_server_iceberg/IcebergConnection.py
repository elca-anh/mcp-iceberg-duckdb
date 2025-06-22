import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *

import logging
import time
from typing import Any

from .QueryManager import QueryManager

logger = logging.getLogger('iceberg_server')

class IcebergConnection:
    """
    Iceberg catalog connection management class
    """
    def __init__(self):
        self.catalog = None
    
    def query_catalog(self, query: str) -> list[dict[str, Any]]:
        """
        Retrieve information or create table (DDL) on Iceberg catalog
        
        Args:
            query (str): Query to execute
            
        Returns:
            list[dict[str, Any]]: Query results
        """
        start_time = time.time()
        logger.info(f"Executing query: {query[:200]}...")
        
        try:
            catalog = self._ensure_connection()
            parsedQuery = QueryManager(query)
            
            # Handle special commands
            if parsedQuery.type == "LIST":
                (subtype, argument) = parsedQuery.extract_list_arguments()
                if subtype == "NAMESPACES":
                    results = []
                    namespaces = catalog.list_namespaces(argument) if argument else catalog.list_namespaces()
                    for namespace in namespaces:
                        results.append({
                                "namespace": ".".join(namespace)
                            })
                    logger.info(f"Listed {len(results)} namespaces in {time.time() - start_time:.2f}s")
                    return results
                
                elif subtype == "TABLES":
                    results = []
                    # List tables at namespace level or all available namespaces in root
                    namespaces = [(argument,)] if argument else catalog.list_namespaces()
                    for namespace in namespaces:
                        for table in catalog.list_tables(namespace):
                            results.append({
                                "namespace": ".".join(namespace),
                                "table": table
                            })
                    logger.info(f"Listed {len(results)} tables in {time.time() - start_time:.2f}s")
                    return results
            
            elif parsedQuery.type == "DESCRIBE":
                if parsedQuery.statement.tokens[2].value.upper() == "TABLE":
                    table_name = parsedQuery.statement.tokens[4].value
                    table = catalog.load_table(table_name)
                    schema_dict = {
                        "schema": str(table.schema()),
                        "partition_spec": [str(field) for field in (table.spec().fields if table.spec() else [])],
                        "sort_order": [str(field) for field in (table.sort_order().fields if table.sort_order() else [])],
                        "properties": table.properties
                    }
                    return [schema_dict]          
            else:
                raise ValueError(f"Unsupported catalog query type")
            
        except Exception as e:
            logger.error(f"Query error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            raise

    def query_table(self, query: str) -> list[dict[str, Any]]:
        """
        Execute a data modeling query on an Iceberg table
        
        Args:
            query (str): Query to execute
            
        Returns:
            list[dict[str, Any]]: Query results
        """

        start_time = time.time()
        logger.info(f"Executing query: {query[:200]}...")
        
        try:
            catalog = self._ensure_connection()
                 
            # Handle SQL queries
            parsedQuery = QueryManager(query)
            
            if parsedQuery.type == "SELECT":

                # Load and scan table
                table_name = parsedQuery.extract_select_table()
                table = catalog.load_table(table_name)
                scan = table.scan()
                
                # Use DuckDB to postprocess the table
                # to_duckdb() is using register() which is in schema main -> "flatten" the original table name
                table_name2 = table_name.replace('.', '_')
                conn = scan.to_duckdb(table_name=table_name2) # Full scan at this time, to optimize
                query_duck = query.replace(table_name, "main." + table_name2)
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
            
            elif parsedQuery.type == "INSERT":
                # Extract table name and values
                (table_name, values) = parsedQuery.extract_insert_table_values()
                
                if not table_name or values is None:
                    raise ValueError(f"Invalid INSERT statement, missing table name or values")
                
                logger.info(f"Inserting into table: {table_name}")
                logger.info(f"Values: {values}")
                
                # Load table and schema
                table = catalog.load_table(table_name.split('.'))
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

            if parsedQuery.type == "CREATE":
                # Basic CREATE TABLE support            
                if "CREATE TABLE" in query.strip().upper():
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
                raise ValueError(f"Unsupported query type: {parsedQuery.type}")
                
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

    def _ensure_connection(self):
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
