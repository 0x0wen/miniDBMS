from .Table import Table
from typing import List, TypeVar
T = TypeVar('T')

from StorageManager.objects.DataRetrieval import DataRetrieval, Condition

from StorageManager.objects.Rows import Rows
from FailureRecovery.Structs.Header import Header
from FailureRecovery.Structs.Row import Row


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
        print("Buffer initialized")
        self.tables : List[Table] = []
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
        
    def retrieveData(self, data: DataRetrieval) -> List[dict]:
        '''mengambil data dari tabel berdasarkan kondisi yang diberikan'''
        print("Retrieving data from buffer")
        # table = self.getTable(data.table[0])
        # if table:
        #     rows = table.findRows(data.conditions)
        #     return rows
        return None
    
    def writeData(self, rows: Rows, dataRetrieval: DataRetrieval) -> bool:
        
        # new_table = Table("course")
        # header = Header()
        # self.addTabble(new_table)
        
        if len(rows) == 0:
            return False
        
        print("\n\nInside Buffer.writeData()\n")
        print("Adding data to buffer\n")
        
        table_name = dataRetrieval.table[0]
        is_table_exist = self.getTable(table_name)
        
        if not is_table_exist:
            print ("Table not found, creating new table\n")
            
            new_table = Table(table_name)
            table_header = Header()
            
            for column in rows[0]:
                table_header.addColumn(column, "str")
                
            new_table.setHeader(table_header)
            
            for row in rows:
                row_data = []
                for column in row:
                    row_data.append(row[column])
                    
                new_table.addRow(Row(new_table.numRows(), row_data))
                
            self.addTabble(new_table)
            print(new_table)
                
        else:
            print("Table found, adding data to existing table\n")
            
            table = self.getTable(table_name)
            
            for row in rows:
                row_data = []
                for column in row:
                    row_data.append(row[column])
                    
                table.addRow(Row(table.numRows(), row_data))

            print(table)
        
        print("Data successfully added to buffer\n")
        