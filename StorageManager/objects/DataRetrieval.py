from .Condition import Condition
class DataRetrieval:
    def __init__(self, table: list[str], column: list[str] = [], conditions: list[Condition] = []) -> None:
        self.table = table
        self.column = column
        self.conditions = conditions

    #debugging
    def __repr__(self) -> str:
        return (f"DataRetrieval(table={self.table}, column={self.column}, "
                f"conditions={self.conditions})")

