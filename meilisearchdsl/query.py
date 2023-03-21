from enum import Enum


class Q:
    class OPERATIONS(str, Enum):
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
    op_map = {
        OPERATIONS.EQUALS: '=',
        OPERATIONS.NOT_EQUALS: '!=',
        OPERATIONS.GREATER_THAN: '>',
        OPERATIONS.GREATER_THAN_OR_EQUALS: '>=',
        OPERATIONS.LESS_THAN: '<',
        OPERATIONS.LESS_THAN_OR_EQUALS: '<=',
        OPERATIONS.IN: 'IN',
        OPERATIONS.NOT_IN: 'NOT IN',
        OPERATIONS.EXISTS: 'EXISTS',
        OPERATIONS.NOT_EXISTS: 'NOT EXISTS'
    }
    negate_map = {
        '=': '!=',
        '!=': '=',
        '>': '<=',
        '>=': '<',
        '<': '>=',
        '<=': '>',
        op_map[OPERATIONS.IN]: op_map[OPERATIONS.NOT_IN],
        op_map[OPERATIONS.NOT_IN]: op_map[OPERATIONS.IN],
        op_map[OPERATIONS.EXISTS]: op_map[OPERATIONS.NOT_EXISTS],
        op_map[OPERATIONS.NOT_EXISTS]: op_map[OPERATIONS.EXISTS]
    }
    def __init__(self, **kwargs):
        self.conditions = kwargs
        self.operator = None
        self.negate = False
        self.children = []

    def __or__(self, other):
        new_q = Q()
        new_q.operator = "OR"
        new_q.children = [self, other]
        return new_q

    def __and__(self, other):
        new_q = Q()
        new_q.operator = "AND"
        new_q.children = [self, other]
        return new_q

    def __invert__(self):
        new_q = Q()
        new_q.negate = not self.negate
        new_q.conditions = self.conditions
        new_q.operator = self.operator
        new_q.children = self.children
        return new_q


    def _clean_value(self, val):
        retval = val
        if isinstance(val, str):
            if " " in val:
                retval = f'\"{val}\"'
            else:
                retval = val
        elif isinstance(val, bool):
            retval = str(val).lower()
        else:
            retval = str(val)
        expression_ops = list(self.OPERATIONS._member_map_.values()) + list(self.negate_map.values())
        if retval in expression_ops:
            retval = f'\"{retval}\"'
        return retval

    def to_query_string(self) -> str:
        if self.operator:
            left = self.children[0].to_query_string()
            right = self.children[1].to_query_string()
            return f"({left} {self.operator} {right})"
        else:
            conditions = []
            for k, v in self.conditions.items():
                field, op = k.split('__') if '__' in k else (k, self.OPERATIONS.EQUALS)
                assert op in self.OPERATIONS._member_map_.values(), f"Invalid operation {op}"
                if op == self.OPERATIONS.IN:
                    assert isinstance(v, list), f"Value for IN operation must be a list. Got {v}"
                    escaped_values = [self._clean_value(i) for i in v]
                    v = f"[{','.join(escaped_values)}]"
                elif op == self.OPERATIONS.NOT_IN:
                    assert isinstance(v, list), f"Value for NOT_IN operation must be a list. Got {v}"
                    escaped_values = [self._clean_value(i) for i in v]
                    v = f"[{','.join(escaped_values)}]"
                else:
                    if isinstance(v, str) and " " in v:
                        v = f'\"{v}\"'
                if self.negate:
                    op = self.negate_map[self.op_map[op]]
                else:
                    op = self.op_map[op]
                if op in ["EXISTS", "NOT EXISTS"]:
                    condition = f"{field} {op}"
                else:
                    condition = f"{field} {op} {v}"
                conditions.append(condition)
            return " AND ".join(conditions)

    def to_query_list(self, lvl: int=0) -> list:
        if lvl > 2:
            raise Exception("Query nesting too deep, meilisearch only supports 2 levels of nesting")
        if self.operator:
            left = self.children[0].to_query_list(lvl+1)
            right = self.children[1].to_query_list(lvl+1)
            return [left, right]
        else:
            conditions = []
            for k, v in self.conditions.items():
                field, op = k.split('__') if '__' in k else (
                    k, self.OPERATIONS.EQUALS)
                assert op in self.OPERATIONS._member_map_.values(
                ), f"Invalid operation {op}"
                if op == self.OPERATIONS.IN:
                    assert isinstance(
                        v, list), f"Value for IN operation must be a list. Got {v}"
                    escaped_values = [self._clean_value(i) for i in v]
                    v = f"[{','.join(escaped_values)}]"
                elif op == self.OPERATIONS.NOT_IN:
                    assert isinstance(
                        v, list), f"Value for NOT_IN operation must be a list. Got {v}"
                    escaped_values = [self._clean_value(i) for i in v]
                    v = f"[{','.join(escaped_values)}]"
                else:
                    if isinstance(v, str) and " " in v:
                        v = f'\"{v}\"'
                if self.negate:
                    op = self.negate_map[self.op_map[op]]
                else:
                    op = self.op_map[op]
                if op in ["EXISTS", "NOT EXISTS"]:
                    condition = f"{field} {op}"
                else:
                    condition = f"{field} {op} {v}"
                conditions.append(condition)
            return " AND ".join(conditions)
