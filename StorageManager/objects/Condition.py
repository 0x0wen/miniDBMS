from typing import List,Union

class Condition:
    def __init__(self, column: str, operation: str, operand: Union[int, str], connector: str = None) -> None:
        self.column = column
        self.operation = operation
        self.operand = operand
        self.connector = connector

    # Debugging
    def __repr__(self) -> str:
        return (f"Condition(column={self.column}, operation={self.operation}, "
                f"operand={self.operand}, connector={self.connector})")
