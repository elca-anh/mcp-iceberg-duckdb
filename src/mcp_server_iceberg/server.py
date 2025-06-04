#!/usr/bin/env python
import asyncio
import logging
import json
import time
from dotenv import load_dotenv
import mcp.server.stdio
from mcp.server import Server
from mcp.types import Tool, TextContent
from pyiceberg.expressions import *
from pyiceberg.types import *
from .IcebergConnection import IcebergConnection

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('iceberg_server')

load_dotenv()


class IcebergServer(Server):
    """
    Iceberg MCP server class, handles client interactions
    """

    def __init__(self):
        super().__init__(name="iceberg-server")
        self.db = IcebergConnection()
        logger.info("IcebergServer initialized")

        @self.list_tools()
        async def handle_tools():
            """
            Return list of available tools
            """
            return [
                Tool(
                    name="execute_query",
                    description="Execute a query on Iceberg tables",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Query to execute (supports: LIST TABLES, DESCRIBE TABLE, SELECT, CREATE TABLE)"
                            }
                        },
                        "required": ["query"]
                    }
                )
            ]

        @self.call_tool()
        async def handle_call_tool(name: str, arguments: dict):
            """
            Handle tool call requests
            
            Args:
                name (str): Tool name
                arguments (dict): Tool arguments
                
            Returns:
                list[TextContent]: Execution results
            """
            if name == "execute_query":
                start_time = time.time()
                try:
                    result = self.db.execute_query(arguments["query"])
                    execution_time = time.time() - start_time
                    
                    return [TextContent(
                        type="text",
                        text=f"Results (execution time: {execution_time:.2f}s):\n{json.dumps(result, indent=2, default=str)}"
                    )]
                except Exception as e:
                    error_message = f"Error executing query: {str(e)}"
                    logger.error(error_message)
                    return [TextContent(
                        type="text",
                        text=error_message
                    )]

    def __del__(self):
        """
        Clean up resources
        """
        if hasattr(self, 'db'):
            self.db.close()

async def main():
    """
    Main function, starts server and handles requests
    """
    try:
        server = IcebergServer()
        initialization_options = server.create_initialization_options()
        logger.info("Starting server")
        
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                initialization_options
            )
    except Exception as e:
        logger.critical(f"Server failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Server shutting down")

def run_server():
    """
    Entry point for running the server
    """
    asyncio.run(main())

if __name__ == "__main__":
    run_server()