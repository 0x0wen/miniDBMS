from typing import List, TypeVar

from StorageManager.objects.Condition import Condition
T = TypeVar('T')

from Row import Row
from Header import Header
from Table import Table

'''
class ini itu untuk nyimpen data tabel, 1 tabel terdiri dari header dan list of rows, 
contohnhya ada di Buffer.py
'''

class Table:
    def __init__(self, table_name: str, header: Header = None, rows: list[Row] = None):
        
        self.table_name = table_name
        self.header = header
        self.rows = rows
    
    def setHeader(self, header: Header) -> None:
        self.header = header
        
    def addRow(self, row: Row) -> None:
        '''ini kaya perlu verifikasi dlu apakah row yang dimasukin itu,
           jumlah kolom serta tipe datanya sesuai header atau gk'''
        if row.isRowValid(self.header):
            self.rows.append(row)
        else:
            raise ValueError("Row data doesn't match header")
        
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

    
    '''Fungsi CRUD diatas itu gw rada yapping sih perlu dipikir lg implementasinya
    apakah perlu fungsi lain lg atau tidak'''
    
    