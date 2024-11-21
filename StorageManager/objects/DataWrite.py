from typing import Generic, TypeVar, Optional
from .Condition import Condition

T = TypeVar('T')

class DataWrite(Generic[T]):
    def __init__(self, overwrite: bool, selected_table: str, column: list[str], conditions: list[Condition], 
                 new_value: Optional[list[T]] = None, joined_table: Optional[list[str]] = None) -> None:
        self.overwrite = overwrite
        self.selected_table = selected_table
        self.joined_table = joined_table
        self.column = column
        self.conditions = conditions
        self.new_value = new_value

    #debugging
    def __repr__(self) -> str:
        return (f"DataWrite(type={self.type},table={self.table}, column={self.column}, "
                f"conditions={self.conditions}), new_value={self.new_value})")