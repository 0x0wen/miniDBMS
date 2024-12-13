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
        return (
            f"DataWrite(\n"
            f"  Overwrite: {self.overwrite},\n"
            f"  Table: {self.selected_table},\n"
            f"  Joined Table: {self.joined_table},\n"
            f"  Columns: {self.column},\n"
            f"  Conditions: {self.conditions},\n"
            f"  New Value: {self.new_value}\n"
            f")"
        )