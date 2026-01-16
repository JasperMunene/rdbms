"""
Parser Exceptions - Custom exceptions for parsing errors
"""

class PesaSQLParseError(Exception):
    """Base class for parsing errors"""
    pass

class PesaSQLLexerError(PesaSQLParseError):
    """Lexer/tokenization errors"""
    pass

class PesaSQLSyntaxError(PesaSQLParseError):
    """Syntax errors in SQL statements"""
    pass

class PesaSQLSemanticError(PesaSQLParseError):
    """Semantic errors (type mismatches, etc.)"""
    pass