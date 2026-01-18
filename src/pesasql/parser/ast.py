"""
AST Nodes - Abstract Syntax Tree for SQL
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Union
from enum import Enum
from ..types.value import Value, Type


@dataclass
class Node:
    """Base AST node"""
    pass


class JoinType(Enum):
    """Types of JOINs (for future use)"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


@dataclass
class JoinClause(Node):
    """JOIN clause"""
    table_name: str
    join_type: JoinType
    on_condition: 'Expression'
    table_alias: Optional[str] = None


@dataclass
class SelectStatement(Node):
    """SELECT statement"""
    columns: List['Column']  # Columns to select
    table_name: str  # Table name
    table_alias: Optional[str] = None  # Table alias (e.g., FROM merchants m)
    where_clause: Optional['Expression'] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: List['OrderByClause'] = field(default_factory=list)

    joins: List[JoinClause] = field(default_factory=list)


@dataclass
class InsertStatement(Node):
    """INSERT statement"""
    table_name: str
    columns: Optional[List[str]] = None  # Column names (if specified)
    values: List[List['Expression']] = field(default_factory=list)  # List of value lists


@dataclass
class ColumnDefinition(Node):
    """Column definition in CREATE TABLE"""
    name: str
    data_type: str  # e.g., 'INT', 'STRING(100)'
    constraints: List[str] = field(default_factory=list)  # e.g., 'PRIMARY KEY', 'NOT NULL'
    default_value: Any = None


@dataclass
class ForeignKeyDefinition(Node):
    """Foreign key constraint in CREATE TABLE"""
    column_name: str
    ref_table: str
    ref_column: str


@dataclass
class CreateTableStatement(Node):
    """CREATE TABLE statement"""
    table_name: str
    columns: List['ColumnDefinition']
    if_not_exists: bool = False
    foreign_keys: List[ForeignKeyDefinition] = field(default_factory=list)


@dataclass
class DropTableStatement(Node):
    """DROP TABLE statement"""
    table_name: str
    if_exists: bool = False


@dataclass
class Column(Node):
    """Column reference"""
    name: str
    table_alias: Optional[str] = None  # For qualified names: table.column


@dataclass
class Literal(Node):
    """Literal value"""
    value: Any
    value_type: Optional[Type] = None

    def to_value(self) -> Value:
        """Convert to Value object"""
        if self.value_type:
            return Value(self.value_type, self.value)
        # Infer type
        if isinstance(self.value, str):
            return Value(Type.STRING, self.value)
        elif isinstance(self.value, int):
            return Value(Type.INTEGER, self.value)
        elif isinstance(self.value, float):
            return Value(Type.DOUBLE, self.value)
        elif isinstance(self.value, bool):
            return Value(Type.BOOLEAN, self.value)
        else:
            return Value(Type.NULL, None)


@dataclass
class Expression(Node):
    """Base expression"""
    pass


@dataclass
class BinaryExpression(Expression):
    """Binary expression: left op right"""
    left: Expression
    operator: str  # =, !=, <, >, <=, >=, AND, OR, +, -, *, /
    right: Expression


@dataclass
class UnaryExpression(Expression):
    """Unary expression: op operand"""
    operator: str  # NOT, +, -
    operand: Expression


@dataclass
class ColumnExpression(Expression):
    """Column reference expression"""
    column: Column


@dataclass
class LiteralExpression(Expression):
    """Literal expression"""
    literal: Literal


@dataclass
class FunctionCall(Expression):
    """Function call (for future use)"""
    function_name: str
    arguments: List[Expression]


@dataclass
class OrderByClause(Node):
    """ORDER BY clause"""
    column: Column
    ascending: bool = True  # True for ASC, False for DESC


@dataclass
class UseStatement(Node):
    """USE database statement"""
    database_name: str

@dataclass
class DeleteStatement(Node):
    """DELETE FROM table WHERE condition"""
    table_name: str
    where_clause: Optional[Expression]

@dataclass
class UpdateStatement(Node):
    """UPDATE table SET col=val, ... WHERE condition"""
    table_name: str
    updates: List[tuple[str, Expression]] = field(default_factory=list) # List of (column_name, expression)
    where_clause: Optional[Expression] = None