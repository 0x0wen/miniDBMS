from typing import List, TypeVar
T = TypeVar('T')

'''
row_data itu tuple of data (tapi kita gatau tipe data dan nama atributnya, itu disimpan di header)
contoh:
    self.row_id = 1 ---> ini untuk identifier aja sih, mungkin gk diperlukan
    row_data = [1, 'John', 20]
'''

class Row:
    def __init__(self, row_id, row_data):
        self.row_id = row_id
        self.row_data : List[T]  = row_data
        
    def isRowValid(self, header) -> bool:
        '''ngecek apakah row_data sesuai dengan header'''
        pass    # belum diimplementasi
    
    def isRowFullfilngCondition(self, condition: str) -> bool:
        '''ngecek apakah row_data memenuhi kondisi tertentu'''
        
        '''ini nama methodnya jelek sih bisa diganti, intinya dipakai buat ngecek apakah row ini memenuhi kondisi tertentu'''
        pass    # belum diimplement