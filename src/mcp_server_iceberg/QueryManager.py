import sqlparse
from sqlparse.sql import Statement
from sqlparse import keywords
from sqlparse import tokens as sqltokens

from typing import Any

class QueryManager:

    """ The raw query (stripped) """
    _raw_query: str
    """ The type of query (INSERT, SELECT...)"""
    type: str

    statement: Statement

    def __init__(self, query: str):
        self._raw_query = query.strip()

        # Extend sqlparse
        sqlLexer = sqlparse.lexer.Lexer.get_default_instance()
        # Reset Lexer
        sqlLexer.clear()
        sqlLexer.add_keywords(
            {'LIST': sqlparse.tokens.DDL,
             'DESCRIBE': sqlparse.tokens.DDL,
             'NAMESPACES': sqlparse.tokens.Keyword,
             'IN': sqlparse.tokens.Keyword })
        # Add standard regexp & keywords
        sqlLexer.set_SQL_REGEX(keywords.SQL_REGEX)
        sqlLexer.add_keywords(keywords.KEYWORDS_COMMON)
        sqlLexer.add_keywords(keywords.KEYWORDS_ORACLE)
        sqlLexer.add_keywords(keywords.KEYWORDS)

        self._parse_sql()
 
    def extract_list_arguments(self) -> tuple[str, Any]:
        """ Extract the subtype of LIST and the optional parameter 
            Subtype is either NAMESPACES or TABLES
        """
        subtype = None
        argument = None
        if self.statement.tokens[2].value.upper() == "NAMESPACES":
            subtype = "NAMESPACES"
        elif self.statement.tokens[2].value.upper() == "TABLES":
            subtype = "TABLES"
        
        # Look for a namespace to list from
        for token in self.statement.tokens[4:]:
            if isinstance(token, sqlparse.sql.Identifier):
                argument = token.value

        return (subtype, argument)

    def extract_create_table_arguments(self) -> tuple[str, Any]:
        """ Extract the table name and arguments of the table to create
            and the schema arguments
        """
        if self.statement.tokens[2].value.upper() != "TABLE":
            raise ValueError(f"Unsupported CREATE statement for {self.statement.tokens[2].value}")
        
        i: int
        table_name: str = None
        schema: dict = None
        for i, token in enumerate(self.statement.tokens[4:]):
            if isinstance(token, sqlparse.sql.Identifier):
                table_name = token.value
                break

        if i + 2 < len(self.statement.tokens) and \
            isinstance(self.statement.tokens[4+i+2], sqlparse.sql.Parenthesis):
            schema = self._extract_table_def(self.statement.tokens[4+i+2])
                    
        return (table_name, schema)


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

    def _extract_table_def(self, token_list: sqlparse.sql.Parenthesis):
        """ Extract the table definition from the Parenthesis token 
            Adapted from sqlparse examples, could be nicer    
        """
        definitions = {}
        tmp = []
        par_level = 0
        for token in token_list.flatten():
            if token.is_whitespace:
                continue
            elif token.match(sqlparse.tokens.Punctuation, '('):
                par_level += 1
                if par_level > 1:
                    tmp.append(token)
                continue
            elif token.match(sqlparse.tokens.Punctuation, ')'):
                if par_level <= 1:
                    if len(tmp) > 1:
                        definitions[str(tmp[0])] = " ".join(str(t).upper() for t in tmp[1:])
                    break
                else:
                    tmp.append(token)
                    par_level -= 1
            elif token.match(sqlparse.tokens.Punctuation, ','):
                if len(tmp) > 1:
                    definitions[str(tmp[0])] = " ".join(str(t).upper() for t in tmp[1:])
                tmp = []
            else:
                tmp.append(token)
        return definitions