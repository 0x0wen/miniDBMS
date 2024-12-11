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
        self.tables : List[Table] = None
        self.size: int = 0
        
    def addTabble(self, table: Table) -> None:
        self.tables.append(table)

    def getTable(self, table_name: str) -> Table:
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None
    
    def getTables(self) -> List[Table]:
        return self.tables
    
    def clearBuffer(self) -> None:
        self.tables = []