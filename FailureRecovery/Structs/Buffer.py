from FailureRecovery.Structs import Row
from FailureRecovery.Structs.Row import Row
from StorageManager.objects import DataRetrieval
from .Table import Table
from typing import List, TypeVar
T = TypeVar('T')

'''
1) Buffer instance hanya ada 1
2) Di dalam buffer ada banyak tables
3) Tables terdiri dari header, dan list of rows

Contoh:

    Buffer
        self.tables isinya
            Table1 : "DataNama"
                Header  : [id, name, age]
                Rows    : [[1, 'John', 20], 
                        [2, 'Doe', 30],
                        [3, 'Jane', 25]]
            Table2 : "DataKota"
                Header  : [id, city, country]
                Rows    : [[1, 'Jakarta', 'Indonesia'],
                        [2, 'New York', 'USA'],
                        [3, 'Tokyo', 'Japan']]

'''
MAX_BUFFER_SIZE = 100
'''ini valuenya ngasal sih, intinya buffer punya batas maksimal bakal dicek tiap kali ada operasi CRUD'''

class Buffer:
    def __init__(self):
        self.tables : List[Table] = []
        self.size: int = 0
        
    def addTabble(self, table: Table) -> bool:
        if self.size >= MAX_BUFFER_SIZE:
            return False
        self.tables.append(table)
        self.size += 1
        return True

    def getTable(self, table_name: str) -> Table:
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None
    
    def getTables(self) -> List[Table]:
        return self.tables
    
    def clearBuffer(self) -> None:
        self.tables = []
        
    def getRowsBuffer(self, data: DataRetrieval) -> List[Row]:
        '''ngecek apakah data yang mau diambil ada di buffer atau gk'''
        table = self.getTable(data.table)
        matching_rows = []
        
        if table:
            for row in table.rows:
                if row.isRowFullfilngCondition(data.conditions):
                    matching_rows.append(row)
        
        return matching_rows
    
    def retrieveDataInBuffer(self, data: DataRetrieval) -> List[Row]:
        '''ngecek apakah data yang mau diambil ada di buffer atau gk'''
        matching_rows = []
        
        for table_name in data.table:
            table = self.getTable(table_name)
            if table:
                for row in table.rows:
                    if all(row.isRowFullfilngCondition(condition) for condition in data.conditions):
                        matching_rows.append(row)
        
        return matching_rows
        