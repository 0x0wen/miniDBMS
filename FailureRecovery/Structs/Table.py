from typing import List, TypeVar

from StorageManager.objects.Condition import Condition
T = TypeVar('T')

from FailureRecovery.Structs import Row
from FailureRecovery.Structs import Header

from StorageManager.objects.Rows import Rows

'''
class ini itu untuk nyimpen data tabel, 1 tabel terdiri dari header dan list of rows, 
contohnhya ada di Buffer.py
'''

class Table:
    def __init__(self, table_name: str, header: Header = None, rows: list[Row] = []):
        
        self.table_name = table_name
        self.header = header
        self.rows = rows
        self.num_rows = 0
    
    def setHeader(self, header: Header) -> None:
        self.header = header
        
    def addRow(self, row: Row) -> None:
        '''ini kaya perlu verifikasi dlu apakah row yang dimasukin itu,
           jumlah kolom serta tipe datanya sesuai header atau gk'''
        self.rows.append(row)
        self.num_rows += 1
        # if row.isRowValid(self.header):
        #     self.rows.append(row)
        # else:
        #     raise ValueError("Row data doesn't match header")
        
    def getRowByid(self, row_id: int) -> Row:
        for row in self.rows:
            if row.row_id == row_id:
                return row
        return None
    
    def findRows(self, condition: Condition) -> List[Row]:
        '''mencari banyak row dengan kondisi tertentu
        
           konsep condition juga perlu dipikir lagi,
           apakah mau niru prinsip storage manager atau gmn'''
        return [row for row in self.rows if row.isRowFullfilngCondition(condition)]

    def findRow(self, condition: Condition) -> Row:
        '''mencari 1 row dengan kondisi tertentu'''
        for row in self.rows:
            if row.isRowFullfilngCondition(condition):
                return row
        return None
    
    def deleteRows(self, condition: Condition) -> None:
        '''menghapus banyak row dengan kondisi tertentu'''
        self.rows = [row for row in self.rows if not row.isRowFullfilngCondition(condition)]
        pass    
    
    def deleteRow(self, condition: Condition) -> None:
        '''menghapus row dengan kondisi tertentu'''
        for row in self.rows:
            if row.isRowFullfilngCondition(condition):
                self.rows.remove(row)
    
    def updateRow(self, condition: str, new_data: List[T]) -> None:
        for row in self.rows:
            if row.isRowFullfilngCondition(condition):
                row.row_data = new_data

    def updateRows(self, condition: str, new_data: List[List[T]]) -> None:
        for row in self.rows:
            if row.isRowFullfilngCondition(condition):
                row.row_data = new_data

    def numRows(self) -> int:
        return self.num_rows
    
    # print repr
    def __str__(self):
        
        print("Table name:", self.table_name)
        print("Header:", self.header)
        
        print("Rows")
        for row in self.rows:
            print(row)
            
        return ""
    
    '''Fungsi CRUD diatas itu gw rada yapping sih perlu dipikir lg implementasinya
    apakah perlu fungsi lain lg atau tidak'''
    
    