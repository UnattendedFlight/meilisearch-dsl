# pylint: disable=W0719,E1101,C0103,E1131
"""
Query (Q) Module
This module provides the Q class, which is designed to help you create 
complex query expressions for searching documents in your MeiliSearch 
indexes. The Q class allows you to build queries using logical operators 
(AND, OR, NOT) and various comparison operators 
(EQUALS, NOT_EQUALS, GREATER_THAN, etc.). 
You can use this class to create MeiliSearch queries that filter and 
sort search results based on specific conditions.

Classes:
- Q: A class for constructing complex query expressions for MeiliSearch.

Usage example:
```from query import Q

# Create a Q object with a single condition
q = Q(title__eq='The Catcher in the Rye')

# Combine conditions using logical operators
q = Q(title__eq='The Catcher in the Rye') | Q(author__eq='J.D. Salinger')

# Negate conditions using the ~ operator
q = ~Q(title__eq='The Catcher in the Rye')

# Generate a MeiliSearch query string from the Q object
query_string = q.to_query_string()
```
"""
from enum import Enum
from typing import Any, List, Union


class Q:
    """A class for constructing complex query expressions for MeiliSearch."""

    class OPERATIONS(str, Enum):
        """A class for defining the available comparison operations."""

        EQUALS = "eq"
        NOT_EQUALS = "neq"
        GREATER_THAN = "gt"
        GREATER_THAN_OR_EQUALS = "gte"
        LESS_THAN = "lt"
        LESS_THAN_OR_EQUALS = "lte"
        IN = "in"
        NOT_IN = "nin"
        EXISTS = "exists"
        NOT_EXISTS = "not exists"

        @classmethod
        def get_values(cls):
            """Returns a list of all values in the enum."""
            return list(cls._member_map_.values())

    op_map = {
        OPERATIONS.EQUALS: "=",
        OPERATIONS.NOT_EQUALS: "!=",
        OPERATIONS.GREATER_THAN: ">",
        OPERATIONS.GREATER_THAN_OR_EQUALS: ">=",
        OPERATIONS.LESS_THAN: "<",
        OPERATIONS.LESS_THAN_OR_EQUALS: "<=",
        OPERATIONS.IN: "IN",
        OPERATIONS.NOT_IN: "NOT IN",
        OPERATIONS.EXISTS: "EXISTS",
        OPERATIONS.NOT_EXISTS: "NOT EXISTS",
    }
    negate_map = {
        "=": "!=",
        "!=": "=",
        ">": "<=",
        ">=": "<",
        "<": ">=",
        "<=": ">",
        op_map[OPERATIONS.IN]: op_map[OPERATIONS.NOT_IN],
        op_map[OPERATIONS.NOT_IN]: op_map[OPERATIONS.IN],
        op_map[OPERATIONS.EXISTS]: op_map[OPERATIONS.NOT_EXISTS],
        op_map[OPERATIONS.NOT_EXISTS]: op_map[OPERATIONS.EXISTS],
    }

    def __init__(self, **kwargs) -> None:
        """Initializes a Q object with the given conditions."""
        self.conditions = kwargs
        self.operator = None
        self.negate = False
        self.children = []

    def __or__(self, other) -> "Q":
        """Returns a new Q object with the OR operator applied to the two Q objects."""
        new_q = Q()
        new_q.operator = "OR"
        new_q.children = [self, other]
        return new_q

    def __and__(self, other) -> "Q":
        """Returns a new Q object with the AND operator applied to the two Q objects."""
        new_q = Q()
        new_q.operator = "AND"
        new_q.children = [self, other]
        return new_q

    def __invert__(self) -> "Q":
        """Returns a new negated Q object."""
        new_q = Q()
        new_q.negate = not self.negate
        new_q.conditions = self.conditions
        new_q.operator = self.operator
        new_q.children = self.children
        return new_q

    def _clean_value(self, val) -> Union[str, int, dict, List[Any]]:
        """Returns a proper (cleaned) representation of the given value."""
        retval = val
        if isinstance(val, str):
            if " " in val:
                retval = f'"{val}"'
            else:
                retval = val
        elif isinstance(val, bool):
            retval = str(val).lower()
        else:
            retval = str(val)
        expression_ops = list(self.OPERATIONS.get_values()) + list(
            self.negate_map.values()
        )
        if retval in expression_ops:
            retval = f'"{retval}"'
        return retval

    def __repr__(self) -> str:
        """Returns a string representation of the Q object."""
        return f"Q({self.to_query_string()})"

    def to_query_string(self) -> str:
        """Returns a MeiliSearch query string representation of the Q object."""
        if self.operator:
            left = self.children[0].to_query_string()
            right = self.children[1].to_query_string()
            return f"({left}) {self.operator} ({right})"
        conditions = []
        for key, value in self.conditions.items():
            *fields, operation = (
                key.split("__") if "__" in key else (key, self.OPERATIONS.EQUALS)
            )
            field = ".".join(fields)
            assert operation in self.OPERATIONS.get_values(), ValueError(
                f"Invalid operation {operation}"
            )
            if operation == self.OPERATIONS.IN:
                assert isinstance(value, list), ValueError(
                    f"Value for IN operation must be a list. Got {value}"
                )
                escaped_values = [self._clean_value(i) for i in value]
                value = f"[{','.join(escaped_values)}]"
            elif operation == self.OPERATIONS.NOT_IN:
                assert isinstance(value, list), ValueError(
                    f"Value for NOT_IN operation must be a list. Got {value}"
                )
                escaped_values = [self._clean_value(i) for i in value]
                value = f"[{','.join(escaped_values)}]"
            else:
                if isinstance(value, str) and " " in value:
                    value = f'"{value}"'
            if self.negate:
                operation = self.negate_map[self.op_map[operation]]
            else:
                operation = self.op_map[operation]
            condition = ""
            if operation in ["EXISTS", "NOT EXISTS"]:
                condition = f"{field} {operation}"
            else:
                condition = f"{field} {operation} {value}"
            conditions.append(condition)
        return " AND ".join(conditions)

    def to_query_list(self, lvl: int = 0) -> list:
        """Returns a MeiliSearch query list representation of the Q object."""
        if lvl > 2:
            raise Exception(
                "Query nesting too deep, meilisearch only supports 2 levels of nesting"
            )
        if self.operator:
            left = self.children[0].to_query_list(lvl + 1)
            right = self.children[1].to_query_list(lvl + 1)
            return [left, right]
        conditions = []
        for key, value in self.conditions.items():
            *fields, operation = (
                key.split("__") if "__" in key else (key, self.OPERATIONS.EQUALS)
            )
            field = ".".join(fields)
            assert (
                operation in self.OPERATIONS.get_values()
            ), f"Invalid operation {operation}"
            if operation == self.OPERATIONS.IN:
                assert isinstance(
                    value, list
                ), f"Value for IN operation must be a list. Got {value}"
                escaped_values = [self._clean_value(i) for i in value]
                value = f"[{','.join(escaped_values)}]"
            elif operation == self.OPERATIONS.NOT_IN:
                assert isinstance(
                    value, list
                ), f"Value for NOT_IN operation must be a list. Got {value}"
                escaped_values = [self._clean_value(i) for i in value]
                value = f"[{','.join(escaped_values)}]"
            else:
                if isinstance(value, str) and " " in value:
                    value = f'"{value}"'
            if self.negate:
                operation = self.negate_map[self.op_map[operation]]
            else:
                operation = self.op_map[operation]
            if operation in ["EXISTS", "NOT EXISTS"]:
                condition = f"{field} {operation}"
            else:
                condition = f"{field} {operation} {value}"
            conditions.append(condition)
        return " AND ".join(conditions)  # type: ignore

    def prettify_query_string(self) -> str:
        """Returns a prettified MeiliSearch query string representation of the Q object."""
        query_string = self.to_query_string()
        stack = []
        result = []
        indent = 0
        space = "    "

        for char in query_string:
            if char == "(":
                stack.append(char)
                indent += 1
                result.append(char)
            elif char == ")":
                stack.pop()
                indent -= 1
                result.append(char)
            elif char in ["A", "O"] and "".join(stack[-3:]) in ["AND", "OR "]:
                result.append("\n" + indent * space + char)
                stack.pop()
            else:
                stack.append(char)
                result.append(char)

            if len(stack) >= 3 and "".join(stack[-3:]) in ["AND", "OR "]:
                result.append("\n" + indent * space)

        return "".join(result)

    def explain(self, indent_level: int = 0) -> str:
        """Returns a string representation of the Q object with
        indentation to show the nesting of the query."""

        def indent(text: str, level: int) -> str:
            return "    " * level + text

        if self.operator:
            left = self.children[0].explain(indent_level + 1)
            right = self.children[1].explain(indent_level + 1)
            return (
                f"{indent('BEGIN', indent_level)}\n{left}"
                + f"\n{indent(self.operator, indent_level)}\n{right}"
            )
        conditions = []
        for key, value in self.conditions.items():
            *fields, operation = (
                key.split("__") if "__" in key else (key, Q.OPERATIONS.EQUALS)
            )
            field = ".".join(fields)
            if self.negate:
                operation = self.negate_map[self.op_map[operation]]
            else:
                operation = self.op_map[operation]
            explanation = ""
            if operation == "EXISTS":
                explanation = "exists"
            elif operation == "NOT EXISTS":
                explanation = "does not exist"
            elif operation == "IN":
                explanation = f"is in {value}"
            elif operation == "NOT IN":
                explanation = f"is not in {value}"
            else:
                explanation = f"is {operation.lower()} {value}"
            conditions.append(
                indent(
                    f'field "{field}":\n{indent(f"* {explanation}", indent_level + 1)}',
                    indent_level,
                )
            )
        return "\n".join(conditions)
