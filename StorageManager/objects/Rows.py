class Rows(list):
    def __init__(self, rows: list[dict]) -> None:
        super().__init__(rows)
    
    #print 
    def __repr__(self) -> str:
        return f"Rows(rows={self})"
    
    def extend(self, new_rows: list[dict]) -> None:
        if all(isinstance(row, dict) for row in new_rows):
            super().extend(new_rows)
        else:
            raise ValueError("All items in new_rows must be dictionaries.")
        
    def append(self, item: dict) -> None:
        if isinstance(item, dict):
            super().append(item)
        else:
            raise ValueError("Item must be a dictionary.")
    
    def to_set(self) -> set:
        """ Converts the rows (list of dicts) into a set of frozensets for efficient lookup. """
        return {frozenset(row.items()) for row in self}
        