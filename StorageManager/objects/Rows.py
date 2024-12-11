class Rows(list):
    def __init__(self, rows: list[dict]) -> None:
        super().__init__(rows)
        self.indexed_column = None
        
    
    # #print 
    # def __repr__(self) -> str:
    #     return f"Rows(rows={self})"
    
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
        

    def getRowsNotMatching(self, conditional_rows : 'Rows') -> 'Rows':
        """
        Return data that is not matching the conditional_rows

        Args: 
            conditional_rows : rows that is filtered based on conditions

        Returns:
            Rows : data that doesn't match the conditional_rows
        """
        newData : Rows = []
        conditional_rows = conditional_rows._toSet()
        for row in self:
            if frozenset(row.items()) not in conditional_rows:
                newData.append(list(row.values()))
        return newData

    
    def _toSet(self) -> set:
        """ Converts the rows (list of dicts) into a set of frozensets for efficient lookup. """
        return {frozenset(row.items()) for row in self}
    
    def setIndex(self, column : str) -> None:
        """
        Set column in rows that have index
        """
        self.indexed_column = column
    
    def isIndexed(self) -> bool:
        """
        Is rows having any index
        """
        return self.indexed_column != None

    def getIndexColumn(self) -> str:
        """
        return the primay column of index
        """
        if(self.isIndexed()):
            return self.indexed_column
        return "No Index"