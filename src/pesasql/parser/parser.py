"""
SQL Parser - Parses tokens into AST
"""


from .lexer import Lexer, Token, TokenType
from .ast import *
from .exceptions import PesaSQLSyntaxError
from ..types.value import Type



class Parser:
    """SQL parser that builds AST from tokens"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None

    def parse(self) -> Node:
        """Parse tokens into AST"""
        if not self.tokens:
            raise PesaSQLSyntaxError("No tokens to parse")

        # Check for statement type
        if self._match(TokenType.SELECT):
            return self.parse_select()
        elif self._match(TokenType.INSERT):
            return self.parse_insert()
        elif self._match(TokenType.CREATE):
            return self.parse_create()
        elif self._match(TokenType.DROP):
            return self.parse_drop()
        elif self._match(TokenType.DESCRIBE):
            return self.parse_describe()
        elif self._match(TokenType.SHOW):
            return self.parse_show()
        elif self._match(TokenType.USE):
            return self.parse_use()
        elif self._match(TokenType.DELETE):
            return self.parse_delete()
        elif self._match(TokenType.UPDATE):
            return self.parse_update()
        else:
            raise PesaSQLSyntaxError(f"Unexpected token: {self.current_token}")

    def parse_select(self) -> SelectStatement:
        """Parse SELECT statement"""
        # SELECT already consumed


        # Parse columns
        columns = self.parse_column_list()

        # FROM clause
        self._consume(TokenType.FROM, "Expected FROM after columns")
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        # Optional table alias (e.g., FROM merchants m)
        table_alias = None
        if self.current_token.type == TokenType.IDENTIFIER:
            # Check if it looks like an alias (not a keyword like JOIN, WHERE, etc.)
            next_val = self.current_token.value.upper()
            if next_val not in ('JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'WHERE', 'ORDER', 'LIMIT', 'OFFSET', 'ON'):
                table_alias = self.current_token.value
                self._advance()
        
        # JOIN clauses
        joins = self.parse_join_clauses()


        # WHERE clause (optional)
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self.parse_expression()

        # ORDER BY clause (optional)
        order_by = []
        if self._match(TokenType.ORDER):
            self._consume(TokenType.BY, "Expected BY after ORDER")
            order_by = self.parse_order_by()

        # LIMIT clause (optional)
        limit = None
        if self._match(TokenType.LIMIT):
            limit_token = self._consume(TokenType.NUMBER, "Expected number after LIMIT")
            limit = int(limit_token.value)

        # OFFSET clause (optional)
        offset = None
        if self._match(TokenType.OFFSET):
            offset_token = self._consume(TokenType.NUMBER, "Expected number after OFFSET")
            offset = int(offset_token.value)

        return SelectStatement(
            columns=columns,
            table_name=table_name,
            table_alias=table_alias,
            where_clause=where_clause,
            limit=limit,
            offset=offset,
            order_by=order_by,
            joins=joins
        )

    def parse_join_clauses(self) -> List[JoinClause]:
        """Parse JOIN clauses"""
        joins = []
        
        while True:
            join_type = None
            if self._match(TokenType.JOIN):
                join_type = JoinType.INNER
            elif self._match(TokenType.INNER):
                self._consume(TokenType.JOIN, "Expected JOIN after INNER")
                join_type = JoinType.INNER
            elif self._match(TokenType.LEFT):
                self._match(TokenType.OUTER) # Optional OUTER
                self._consume(TokenType.JOIN, "Expected JOIN after LEFT")
                join_type = JoinType.LEFT
            elif self._match(TokenType.RIGHT):
                self._match(TokenType.OUTER) # Optional OUTER
                self._consume(TokenType.JOIN, "Expected JOIN after RIGHT")
                join_type = JoinType.RIGHT
            elif self._match(TokenType.FULL):
                self._match(TokenType.OUTER) # Optional OUTER
                self._consume(TokenType.JOIN, "Expected JOIN after FULL")
                join_type = JoinType.FULL
                
            if not join_type:
                break
                
            table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
            
            # Optional table alias after join table name (e.g., INNER JOIN users u ON ...)
            join_table_alias = None
            if self.current_token.type == TokenType.IDENTIFIER:
                next_val = self.current_token.value.upper()
                if next_val != 'ON':
                    join_table_alias = self.current_token.value
                    self._advance()
            
            self._consume(TokenType.ON, "Expected ON after table name")
            on_condition = self.parse_expression()
            
            joins.append(JoinClause(table_name, join_type, on_condition, join_table_alias))
            
        return joins


    def parse_column_list(self) -> List[Column]:
        """Parse list of columns"""
        columns = []

        if self._match(TokenType.STAR):  # SELECT *
            columns.append(Column(name="*"))
        else:
            # Parse first column
            columns.append(self.parse_column())

            # Parse additional columns
            while self._match(TokenType.COMMA):
                columns.append(self.parse_column())

        return columns

    def parse_column(self) -> Column:
        """Parse a column reference"""
        # Could be: column_name or table_name.column_name
        first = self._consume(TokenType.IDENTIFIER, "Expected column name")

        if self._match(TokenType.DOT):
            table_alias = first.value
            column_name = self._consume(TokenType.IDENTIFIER, "Expected column name after .").value
            return Column(name=column_name, table_alias=table_alias)
        else:
            return Column(name=first.value)

    def parse_insert(self) -> InsertStatement:
        """Parse INSERT statement"""
        # INSERT already consumed

        self._consume(TokenType.INTO, "Expected INTO after INSERT")

        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value

        # Optional column list
        columns = None
        if self.current_token.type == TokenType.LPAREN:
            self._advance()  # Skip '('
            columns = self.parse_identifier_list()
            self._consume(TokenType.RPAREN, "Expected ) after column list")

        self._consume(TokenType.VALUES, "Expected VALUES")

        # Parse values lists
        values_lists = []
        while True:
            self._consume(TokenType.LPAREN, "Expected ( before values")
            values = self.parse_expression_list()
            self._consume(TokenType.RPAREN, "Expected ) after values")
            values_lists.append(values)

            if not self._match(TokenType.COMMA):
                break

        return InsertStatement(
            table_name=table_name,
            columns=columns,
            values=values_lists
        )

    def parse_create(self) -> Union[CreateTableStatement, Node]:
        """Parse CREATE statement"""
        # CREATE already consumed


        if self._match(TokenType.TABLE):
            return self.parse_create_table()
        elif self._match(TokenType.DATABASE):
            # Handle CREATE DATABASE (simplified for now)
            db_name = self._consume(TokenType.IDENTIFIER, "Expected database name").value
            # Return simple node for CLI to handle
            return type('CreateDatabase', (), {'db_name': db_name})()
        else:
            raise PesaSQLSyntaxError(f"Expected TABLE or DATABASE after CREATE")

    def parse_create_table(self) -> CreateTableStatement:
        """Parse CREATE TABLE statement"""
        if_not_exists = False
        if self._match(TokenType.IF):
            self._consume(TokenType.NOT, "Expected NOT after IF")
            self._consume(TokenType.EXISTS, "Expected EXISTS after NOT")
            if_not_exists = True

        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value

        self._consume(TokenType.LPAREN, "Expected ( after table name")

        columns = []
        foreign_keys = []
        
        # Parse definitions (columns or constraints)
        while True:
            if self._match(TokenType.FOREIGN):
                # Parse Foreign Key: FOREIGN KEY (col) REFERENCES table(col)
                self._consume(TokenType.KEY, "Expected KEY after FOREIGN")
                
                self._consume(TokenType.LPAREN, "Expected ( after FOREIGN KEY")
                col_name = self._consume(TokenType.IDENTIFIER, "Expected column name in FK").value
                self._consume(TokenType.RPAREN, "Expected ) after FK column")
                
                self._consume(TokenType.REFERENCES, "Expected REFERENCES in FK")
                ref_table = self._consume(TokenType.IDENTIFIER, "Expected referenced table").value
                
                self._consume(TokenType.LPAREN, "Expected ( after ref table")
                ref_col = self._consume(TokenType.IDENTIFIER, "Expected referenced column").value
                self._consume(TokenType.RPAREN, "Expected ) after ref column")
                
                foreign_keys.append(ForeignKeyDefinition(col_name, ref_table, ref_col))
                
            else:
                columns.append(self.parse_column_definition())

            if not self._match(TokenType.COMMA):
                break

        self._consume(TokenType.RPAREN, "Expected ) after column definitions")

        return CreateTableStatement(
            table_name=table_name,
            columns=columns,
            if_not_exists=if_not_exists,
            foreign_keys=foreign_keys
        )

    def parse_column_definition(self) -> ColumnDefinition:
        """Parse column definition in CREATE TABLE"""
        name = self._consume(TokenType.IDENTIFIER, "Expected column name").value

        # Parse data type
        data_type_token = self.current_token
        if data_type_token.type in [TokenType.INT, TokenType.INTEGER]:
            self._advance()
            data_type = "INT"
        elif data_type_token.type == TokenType.STRING:
            self._advance()
            if self._match(TokenType.LPAREN):
                length = self._consume(TokenType.NUMBER, "Expected string length").value
                self._consume(TokenType.RPAREN, "Expected ) after string length")
                data_type = f"STRING({length})"
            else:
                data_type = "STRING(255)"  # Default
        elif data_type_token.type == TokenType.FLOAT:
            self._advance()
            data_type = "FLOAT"
        elif data_type_token.type == TokenType.DOUBLE:
            self._advance()
            data_type = "DOUBLE"
        elif data_type_token.type == TokenType.BOOLEAN or data_type_token.type == TokenType.BOOL:
            self._advance()
            data_type = "BOOLEAN"
        elif data_type_token.type == TokenType.TIMESTAMP:
            self._advance()
            data_type = "TIMESTAMP"
        else:
            raise PesaSQLSyntaxError(f"Expected data type, got {data_type_token}")

        # Parse constraints
        constraints = []
        default_value = None
        
        while True:
            if self._match(TokenType.PRIMARY):
                self._consume(TokenType.KEY, "Expected KEY after PRIMARY")
                constraints.append("PRIMARY KEY")
            elif self._match(TokenType.UNIQUE):
                constraints.append("UNIQUE")
            elif self._match(TokenType.NOT):
                self._consume(TokenType.NULL, "Expected NULL after NOT")
                constraints.append("NOT NULL")
            elif self._match(TokenType.DEFAULT):
                # Parse Literal
                if self._match(TokenType.NUMBER):
                    default_value = int(self.previous_token().value)
                elif self._match(TokenType.FLOAT_LITERAL):
                    default_value = float(self.previous_token().value)
                elif self._match(TokenType.STRING_LITERAL):
                    default_value = self.previous_token().value
                elif self._match(TokenType.BOOLEAN_LITERAL):
                    default_value = self.previous_token().value.lower() == 'true'
                elif self._match(TokenType.NULL_LITERAL):
                    default_value = None
                else:
                    raise PesaSQLSyntaxError("Expected literal after DEFAULT")
            else:
                break

        return ColumnDefinition(
            name=name,
            data_type=data_type,
            constraints=constraints,
            default_value=default_value
        )

    def parse_drop(self) -> DropTableStatement:
        """Parse DROP statement"""
        # DROP already consumed


        if self._match(TokenType.TABLE):
            if_exists = False
            if self._match(TokenType.IF):
                self._consume(TokenType.EXISTS, "Expected EXISTS after IF")
                if_exists = True

            table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
            return DropTableStatement(table_name=table_name, if_exists=if_exists)
        else:
            raise PesaSQLSyntaxError("Expected TABLE after DROP")

    def parse_describe(self) -> type:
        """Parse DESCRIBE statement"""
        # DESCRIBE already consumed

        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        return type('DescribeTable', (), {'table_name': table_name})()

    def parse_show(self) -> type:
        """Parse SHOW statement"""
        # SHOW already consumed


        if self._match(TokenType.TABLES):
            return type('ShowTables', (), {})()
        elif self._match(TokenType.DATABASES):
            return type('ShowDatabases', (), {})()
        else:
            raise PesaSQLSyntaxError("Expected TABLES or DATABASES after SHOW")

    def parse_use(self) -> type:
        """Parse USE statement"""
        # USE already consumed

        db_name = self._consume(TokenType.IDENTIFIER, "Expected database name").value
        return type('UseDatabase', (), {'db_name': db_name})()

    def parse_expression(self) -> Expression:
        """Parse an expression"""
        return self.parse_logical_or()

    def parse_logical_or(self) -> Expression:
        """Parse OR expressions"""
        expr = self.parse_logical_and()

        while self._match(TokenType.OR):
            operator = self.previous_token().value
            right = self.parse_logical_and()
            expr = BinaryExpression(left=expr, operator=operator, right=right)

        return expr

    def parse_logical_and(self) -> Expression:
        """Parse AND expressions"""
        expr = self.parse_comparison()

        while self._match(TokenType.AND):
            operator = self.previous_token().value
            right = self.parse_comparison()
            expr = BinaryExpression(left=expr, operator=operator, right=right)

        return expr

    def parse_comparison(self) -> Expression:
        """Parse comparison expressions"""
        expr = self.parse_term()

        while self.current_token.type in [TokenType.EQ, TokenType.NEQ, TokenType.LT,
                                          TokenType.GT, TokenType.LTE, TokenType.GTE]:
            operator = self.current_token.value
            self._advance()
            right = self.parse_term()
            expr = BinaryExpression(left=expr, operator=operator, right=right)

        return expr

    def parse_term(self) -> Expression:
        """Parse + and - expressions"""
        expr = self.parse_factor()

        while self.current_token.type in [TokenType.PLUS, TokenType.MINUS]:
            operator = self.current_token.value
            self._advance()
            right = self.parse_factor()
            expr = BinaryExpression(left=expr, operator=operator, right=right)

        return expr

    def parse_factor(self) -> Expression:
        """Parse * and / expressions"""
        expr = self.parse_unary()

        while self.current_token.type in [TokenType.STAR, TokenType.SLASH]:
            operator = self.current_token.value
            self._advance()
            right = self.parse_unary()
            expr = BinaryExpression(left=expr, operator=operator, right=right)

        return expr

    def parse_unary(self) -> Expression:
        """Parse unary expressions"""
        if self._match(TokenType.NOT):
            operator = self.previous_token().value
            operand = self.parse_unary()
            return UnaryExpression(operator=operator, operand=operand)
        elif self._match(TokenType.PLUS, TokenType.MINUS):
            operator = self.previous_token().value
            operand = self.parse_unary()
            return UnaryExpression(operator=operator, operand=operand)

        return self.parse_primary()

    def parse_primary(self) -> Expression:
        """Parse primary expressions"""
        if self._match(TokenType.NUMBER):
            value = int(self.previous_token().value)
            return LiteralExpression(Literal(value=value, value_type=Type.INTEGER))

        elif self._match(TokenType.FLOAT_LITERAL):
            value = float(self.previous_token().value)
            return LiteralExpression(Literal(value=value, value_type=Type.DOUBLE))

        elif self._match(TokenType.STRING_LITERAL):
            value = self.previous_token().value
            return LiteralExpression(Literal(value=value, value_type=Type.STRING))

        elif self._match(TokenType.BOOLEAN_LITERAL):
            value = self.previous_token().value.lower() == 'true'
            return LiteralExpression(Literal(value=value, value_type=Type.BOOLEAN))

        elif self._match(TokenType.NULL_LITERAL):
            return LiteralExpression(Literal(value=None, value_type=Type.NULL))

        elif self._match(TokenType.LPAREN):
            expr = self.parse_expression()
            self._consume(TokenType.RPAREN, "Expected ) after expression")
            return expr

        else:
            # Must be a column reference
            column = self.parse_column()
            return ColumnExpression(column=column)

    def parse_expression_list(self) -> List[Expression]:
        """Parse list of expressions"""
        expressions = []

        if self.current_token.type == TokenType.RPAREN:
            return expressions  # Empty list

        expressions.append(self.parse_expression())

        while self._match(TokenType.COMMA):
            expressions.append(self.parse_expression())

        return expressions

    def parse_identifier_list(self) -> List[str]:
        """Parse list of identifiers"""
        identifiers = []

        identifiers.append(self._consume(TokenType.IDENTIFIER, "Expected identifier").value)

        while self._match(TokenType.COMMA):
            identifiers.append(self._consume(TokenType.IDENTIFIER, "Expected identifier").value)

        return identifiers

    def parse_order_by(self) -> List[OrderByClause]:
        """Parse ORDER BY clause"""
        clauses = []

        while True:
            column = self.parse_column()
            ascending = True

            if self._match(TokenType.ASC):
                ascending = True
            elif self._match(TokenType.DESC):
                ascending = False

            clauses.append(OrderByClause(column=column, ascending=ascending))

            if not self._match(TokenType.COMMA):
                break

        return clauses

    def parse_use(self) -> UseStatement:
        """Parse USE database statement"""
        db_name = self._consume(TokenType.IDENTIFIER, "Expected database name").value
        return UseStatement(database_name=db_name)

    def parse_update(self) -> UpdateStatement:
        """Parse UPDATE statement"""
        # UPDATE already consumed
        
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        self._consume(TokenType.SET, "Expected SET after table name")
        
        updates = []
        while True:
            column_name = self._consume(TokenType.IDENTIFIER, "Expected column name").value
            self._consume(TokenType.EQ, "Expected = after column name")
            expression = self.parse_expression()
            updates.append((column_name, expression))
            
            if not self._match(TokenType.COMMA):
                break
                
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self.parse_expression()
            
        return UpdateStatement(table_name=table_name, updates=updates, where_clause=where_clause)

    def parse_delete(self) -> DeleteStatement:
        """Parse DELETE statement"""
        # DELETE already consumed
        self._consume(TokenType.FROM, "Expected FROM after DELETE")
        table_name = self._consume(TokenType.IDENTIFIER, "Expected table name").value
        
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self.parse_expression()
            
        return DeleteStatement(table_name=table_name, where_clause=where_clause)

    # Helper methods
    def _advance(self) -> Token:
        """Move to next token"""
        if self.position < len(self.tokens):
            self.position += 1
            if self.position < len(self.tokens):
                self.current_token = self.tokens[self.position]
            else:
                self.current_token = None
        return self.previous_token()

    def _match(self, *token_types: str) -> bool:
        """Check if current token matches any of the given types"""
        if self.current_token and self.current_token.type in token_types:
            self._advance()
            return True
        return False

    def _consume(self, token_type: str, message: str) -> Token:
        """Consume token of expected type or raise error"""
        if self.current_token and self.current_token.type == token_type:
            return self._advance()
        raise PesaSQLSyntaxError(f"{message}, got {self.current_token}")

    def previous_token(self) -> Token:
        """Get previous token"""
        if self.position > 0:
            return self.tokens[self.position - 1]
        return None

    @classmethod
    def parse_sql(cls, sql: str) -> Node:
        """Parse SQL string into AST"""
        lexer = Lexer(sql)
        tokens = lexer.tokenize()

        # Check for lexer errors
        for token in tokens:
            if token.type == TokenType.ERROR:
                raise PesaSQLSyntaxError(f"Lexer error at {token.line}:{token.column}: {token.value}")

        parser = cls(tokens)
        return parser.parse()