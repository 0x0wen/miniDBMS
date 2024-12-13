from typing import Generic, TypeVar, List

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.rows_count = len(data)

    def __str__(self):
        return str(self.data)
