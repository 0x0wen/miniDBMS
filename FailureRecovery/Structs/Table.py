from typing import List, TypeVar
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
        self.rows.append(row)
        
    def getRowByid(self, row_id: int) -> Row:
        for row in self.rows:
            if row.row_id == row_id:
                return row
        return None
    
    def findRows(self, condition: str) -> List[Row]:
        '''mencari banyak row dengan kondisi tertentu
        
           konsep condition juga perlu dipikir lagi,
           apakah mau niru prinsip storage manager atau gmn'''
        pass    # belum diimplementasi

    def findRow(self, condition: str) -> Row:
        '''mencari 1 row dengan kondisi tertentu'''
        pass    #  belum diimplementasi
    
    def deleteRows(self, condition: str) -> None:
        '''menghapus banyak row dengan kondisi tertentu'''
        pass    # belum diimplementasi
    
    def deleteRow(self, condition: str) -> None:
        '''menghapus row dengan kondisi tertentu'''
        pass    # belum diimplementasi
    
    def updateRow(self, condition: str, new_data: List[T]) -> None:
        pass
    
    def updateRows(self, condition: str, new_data: List[List[T]]) -> None:
        pass
    
    '''Fungsi CRUD diatas itu gw rada yapping sih perlu dipikir lg implementasinya
    apakah perlu fungsi lain lg atau tidak'''
    
    