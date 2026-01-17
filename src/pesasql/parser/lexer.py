"""
SQL Lexer - Tokenizes SQL strings into tokens
"""

import re
from typing import List, Iterator
from .exceptions import PesaSQLLexerError


class TokenType:
    """Token types for SQL"""
    # Keywords
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    INTO = 'INTO'
    VALUES = 'VALUES'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    FROM = 'FROM'
    WHERE = 'WHERE'
    JOIN = 'JOIN'
    ON = 'ON'
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'
    OUTER = 'OUTER'
    CREATE = 'CREATE'
    TABLE = 'TABLE'
    DROP = 'DROP'
    DESCRIBE = 'DESCRIBE'
    SHOW = 'SHOW'
    TABLES = 'TABLES'
    DATABASE = 'DATABASE'
    USE = 'USE'
    PRIMARY = 'PRIMARY'
    KEY = 'KEY'
    UNIQUE = 'UNIQUE'
    NOT = 'NOT'
    NULL = 'NULL'
    AND = 'AND'
    OR = 'OR'
    ORDER = 'ORDER'
    BY = 'BY'
    LIMIT = 'LIMIT'
    OFFSET = 'OFFSET'
    INT = 'INT'
    INTEGER = 'INTEGER'
    STRING = 'STRING'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    BOOLEAN = 'BOOLEAN'
    BOOL = 'BOOL'
    TIMESTAMP = 'TIMESTAMP'
    
    # Constraints & Default
    DEFAULT = 'DEFAULT'
    FOREIGN = 'FOREIGN'
    REFERENCES = 'REFERENCES'

    # Conditionals & Ordering
    IF = 'IF'
    EXISTS = 'EXISTS'
    ASC = 'ASC'
    DESC = 'DESC'


    # Literals
    IDENTIFIER = 'IDENTIFIER'
    STRING_LITERAL = 'STRING_LITERAL'
    NUMBER = 'NUMBER'
    FLOAT_LITERAL = 'FLOAT_LITERAL'
    BOOLEAN_LITERAL = 'BOOLEAN_LITERAL'
    NULL_LITERAL = 'NULL_LITERAL'

    # Operators
    EQ = 'EQ'  # =
    NEQ = 'NEQ'  # != or <>
    LT = 'LT'  # <
    GT = 'GT'  # >
    LTE = 'LTE'  # <=
    GTE = 'GTE'  # >=
    PLUS = 'PLUS'  # +
    MINUS = 'MINUS'  # -
    STAR = 'STAR'  # *
    SLASH = 'SLASH'  # /

    # Punctuation
    COMMA = 'COMMA'  # ,
    SEMICOLON = 'SEMICOLON'  # ;
    LPAREN = 'LPAREN'  # (
    RPAREN = 'RPAREN'  # )
    DOT = 'DOT'  # .

    # Special
    EOF = 'EOF'
    ERROR = 'ERROR'


class Token:
    """A single token in the SQL stream"""

    def __init__(self, token_type: str, value: str, line: int, column: int):
        self.type = token_type
        self.value = value
        self.line = line
        self.column = column

    def __repr__(self):
        return f"Token({self.type}, '{self.value}', {self.line}:{self.column})"

    def __eq__(self, other):
        return (self.type == other.type and
                self.value == other.value and
                self.line == other.line and
                self.column == other.column)


class Lexer:
    """SQL lexer/tokenizer"""

    # Token patterns
    TOKEN_PATTERNS = [
        # Whitespace (ignored)
        (r'\s+', None),

        # Comments
        (r'--.*', None),
        (r'/\*[\s\S]*?\*/', None),

        # Keywords (must come before identifiers)
        (r'(?i)SELECT\b', TokenType.SELECT),
        (r'(?i)INSERT\b', TokenType.INSERT),
        (r'(?i)INTO\b', TokenType.INTO),
        (r'(?i)VALUES\b', TokenType.VALUES),
        (r'(?i)UPDATE\b', TokenType.UPDATE),
        (r'(?i)DELETE\b', TokenType.DELETE),
        (r'(?i)FROM\b', TokenType.FROM),
        (r'(?i)WHERE\b', TokenType.WHERE),
        (r'(?i)JOIN\b', TokenType.JOIN),
        (r'(?i)ON\b', TokenType.ON),
        (r'(?i)INNER\b', TokenType.INNER),
        (r'(?i)LEFT\b', TokenType.LEFT),
        (r'(?i)RIGHT\b', TokenType.RIGHT),
        (r'(?i)FULL\b', TokenType.FULL),
        (r'(?i)OUTER\b', TokenType.OUTER),
        (r'(?i)CREATE\b', TokenType.CREATE),
        (r'(?i)TABLE\b', TokenType.TABLE),
        (r'(?i)DROP\b', TokenType.DROP),
        (r'(?i)DESCRIBE\b', TokenType.DESCRIBE),
        (r'(?i)SHOW\b', TokenType.SHOW),
        (r'(?i)TABLES\b', TokenType.TABLES),
        (r'(?i)DATABASE\b', TokenType.DATABASE),
        (r'(?i)USE\b', TokenType.USE),
        (r'(?i)PRIMARY\b', TokenType.PRIMARY),
        (r'(?i)KEY\b', TokenType.KEY),
        (r'(?i)UNIQUE\b', TokenType.UNIQUE),
        (r'(?i)NOT\b', TokenType.NOT),
        (r'(?i)NULL\b', TokenType.NULL),
        (r'(?i)AND\b', TokenType.AND),
        (r'(?i)OR\b', TokenType.OR),
        (r'(?i)ORDER\b', TokenType.ORDER),
        (r'(?i)BY\b', TokenType.BY),
        (r'(?i)LIMIT\b', TokenType.LIMIT),
        (r'(?i)OFFSET\b', TokenType.OFFSET),
        (r'(?i)DEFAULT\b', TokenType.DEFAULT),
        (r'(?i)FOREIGN\b', TokenType.FOREIGN),
        (r'(?i)REFERENCES\b', TokenType.REFERENCES),
        (r'(?i)IF\b', TokenType.IF),
        (r'(?i)EXISTS\b', TokenType.EXISTS),
        (r'(?i)ASC\b', TokenType.ASC),
        (r'(?i)DESC\b', TokenType.DESC),


        # Data types
        (r'(?i)INT\b', TokenType.INT),
        (r'(?i)INTEGER\b', TokenType.INTEGER),
        (r'(?i)STRING\b', TokenType.STRING),
        (r'(?i)FLOAT\b', TokenType.FLOAT),
        (r'(?i)DOUBLE\b', TokenType.DOUBLE),
        (r'(?i)BOOLEAN\b', TokenType.BOOLEAN),
        (r'(?i)BOOL\b', TokenType.BOOL),
        (r'(?i)TIMESTAMP\b', TokenType.TIMESTAMP),

        # Boolean literals
        (r'(?i)TRUE\b', TokenType.BOOLEAN_LITERAL),
        (r'(?i)FALSE\b', TokenType.BOOLEAN_LITERAL),

        # Null literal
        (r'(?i)NULL\b', TokenType.NULL_LITERAL),

        # Operators
        (r'!=', TokenType.NEQ),
        (r'<>', TokenType.NEQ),
        (r'<=', TokenType.LTE),
        (r'>=', TokenType.GTE),
        (r'=', TokenType.EQ),
        (r'<', TokenType.LT),
        (r'>', TokenType.GT),
        (r'\+', TokenType.PLUS),
        (r'-', TokenType.MINUS),
        (r'\*', TokenType.STAR),
        (r'/', TokenType.SLASH),

        # Punctuation
        (r',', TokenType.COMMA),
        (r';', TokenType.SEMICOLON),
        (r'\(', TokenType.LPAREN),
        (r'\)', TokenType.RPAREN),
        (r'\.', TokenType.DOT),

        # Numbers
        (r'\d+\.\d+', TokenType.FLOAT_LITERAL),  # Float
        (r'\d+', TokenType.NUMBER),  # Integer

        # String literals
        (r"'([^'\\]|\\.)*'", TokenType.STRING_LITERAL),
        (r'"([^"\\]|\\.)*"', TokenType.STRING_LITERAL),

        # Identifiers
        (r'[a-zA-Z_][a-zA-Z0-9_]*', TokenType.IDENTIFIER),
    ]

    def __init__(self, text: str):
        self.text = text
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens = []

    def tokenize(self) -> List[Token]:
        """Tokenize the input text"""
        self.tokens = []

        while self.position < len(self.text):
            token = self._next_token()
            if token:
                self.tokens.append(token)

        # Add EOF token
        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _next_token(self) -> Token:
        """Get next token from input"""
        if self.position >= len(self.text):
            return None

        # Try each pattern
        for pattern, token_type in self.TOKEN_PATTERNS:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.match(self.text, self.position)

            if match:
                value = match.group(0)
                start_line = self.line
                start_col = self.column

                # Update position
                self._update_position(value)

                # Skip whitespace and comments
                if token_type is None:
                    return self._next_token()

                # For string literals, strip quotes
                if token_type == TokenType.STRING_LITERAL:
                    value = value[1:-1]  # Remove quotes
                    # Handle escape sequences
                    value = value.replace("\\'", "'").replace('\\"', '"')

                # For case-insensitive keywords, standardize case
                if token_type in [TokenType.SELECT, TokenType.INSERT, TokenType.CREATE,
                                  TokenType.TABLE, TokenType.INT, TokenType.STRING]:
                    value = value.upper()

                return Token(token_type, value, start_line, start_col)

        # No pattern matched
        error_char = self.text[self.position]
        self.position += 1
        self.column += 1
        return Token(TokenType.ERROR, error_char, self.line, self.column - 1)

    def _update_position(self, text: str) -> None:
        """Update line and column based on consumed text"""
        for char in text:
            self.position += 1
            if char == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1

    def peek(self, offset: int = 0) -> Token:
        """Look ahead at tokens without consuming them"""
        idx = len(self.tokens) + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return None

    def __iter__(self) -> Iterator[Token]:
        """Iterate over tokens"""
        return iter(self.tokens)