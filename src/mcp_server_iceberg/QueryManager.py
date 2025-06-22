import sqlparse
from sqlparse.sql import Statement
from sqlparse import tokens as sqltokens

class QueryManager:

    """ The raw query (stripped) """
    _raw_query: str
    """ The type of query (INSERT, SELECT...)"""
    type: str

    statement: Statement

    def __init__(self, query: str):
        self._raw_query = query.strip()
        self._parse_sql()
 
    def extract_select_table(self) -> str | None:
        """ Extract table name for the SELECT
            At this time only support single table in the FROM sub-statement
            JOINs are not supported
        """
        if self.type != "SELECT":
            raise ValueError(f"Query must be SELECT")

        for i, token in enumerate(self.statement.tokens):
            if token.value.upper() == "FROM" and len(self.statement.tokens) > i + 2:
                # Skip whitespace
                name_token = self.statement.tokens[i + 2]
                # Discard any table alias
                return name_token.normalized.split(' ')[0]
            
        return None
    
    def extract_insert_table_values(self) -> tuple[str, list]:
        """ Extract the table and the list of values for the INSERT
            At this time, only single row/record INSERT are supported
        """
        if self.type != "INSERT":
            raise ValueError(f"Query must be INSERT")

        values_token = None
        table_name = None
        for token in self.statement.tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                # Handle multi-part identifiers (e.g., schema.table) but do not take schema
                table_name = str(token).split(" ")[0]
            elif isinstance(token, sqlparse.sql.Values):
                values_token = token
                break
        
        values = []
        if values_token:
            for token in values_token.tokens:
                if isinstance(token, sqlparse.sql.Parenthesis):
                    if values: # Multiple record insert is not supported so far
                        raise ValueError(f"INSERT with multiple rows unsupported")

                    # TODO rewrite from the tokens of token instead of serializing to a string
                    values_str = token.value.strip('()').split(',')
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
                    
        return (table_name, values)
    
    def _parse_sql(self):
        """
        Parse SQL query and extract relevant information
        """
        
        self.type = None

        # Check for empty query
        if not self._raw_query:            
            return
        
        self.statement = sqlparse.parse(self._raw_query)[0]
                        
        # Determine query type
        for token in self.statement.tokens:
            if token.ttype is sqltokens.DML or token.ttype is sqltokens.DDL:
                self.type = token.value.upper()
                break