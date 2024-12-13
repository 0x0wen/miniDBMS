from .Condition import Condition
from .JoinCondition import JoinCondition

class DataRetrieval:
    def __init__(self, table: list[str], column: list[str] = [], 
                 conditions: list[Condition] = [], join_conditions: list[JoinCondition] = []) -> None:
        self.table = table
        self.column = column
        self.conditions = conditions  # Conditions for WHERE clause
        self.join_conditions = join_conditions    # Conditions for JOIN clause

    # Debugging representation
    def __repr__(self) -> str:
        # Format tabel
        table_str = ", ".join(self.table) if self.table else "None"        # Format kolom
        column_str = ", ".join(self.column) if self.column else "All Columns"
        
        # Format kondisi WHERE
        if self.conditions:
            conditions_str = "\n    ".join([repr(cond) for cond in self.conditions])
        else:
            conditions_str = "None"
        
        # Format kondisi JOIN
        if self.join_conditions:
            join_conditions_str = "\n    ".join([repr(cond) for cond in self.join_conditions])
        else:
            join_conditions_str = "None"

        return (
            f"DataRetrieval(\n"
            f"  Tables: [{table_str}],\n"
            f"  Columns: [{column_str}],\n"
            f"  WHERE Conditions: [\n    {conditions_str}\n  ],\n"
            f"  JOIN Conditions: [\n    {join_conditions_str}\n  ]\n"
            f")"
        )
