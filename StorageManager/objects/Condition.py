from typing import List,Union

class Condition:
    def __init__(self, column: str, operation:str, operand: Union[int, str]) -> None:
        self.column = column
        self.operation = operation
        self.operand = operand

    #debugging
    def __repr__(self) -> str:
        return f"Condition(column={self.column}, operation={self.operation}, operand={self.operand})"