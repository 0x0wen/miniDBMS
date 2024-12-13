from typing import List, TypeVar
T = TypeVar('T')

# type data yang bisa dipakai (masih prototype)
class Type:
    INT = 'int'
    STR = 'str'
    FLOAT = 'float'
    
'''
Header itu buat simpan nama data dan tipe data dari data di row, 1 tabel akan punya 1 header

Contoh:
    self.names : [id, name, age]
    self.types : [INT, STR, FLOAT]
'''

class Header:
    def __init__(self):
        self.names : List[str] = None
        self.types : List[Type] = None
        
    def addColumn(self, name: str, type: Type) -> None:
        self.names.append(name)
        self.types.append(type)
        
    def countColumn(self) -> int:
        return len(self.names)
    
    def isColumnExist(self, name: str) -> bool:
        return name in self.names
    
    def typeOfColumn(self, name: str) -> Type:
        return self.types[self.names.index(name)]
    
    def typeOfColumnByIndex(self, index: int) -> Type:
        '''perlu try catch buat invalid index juga atau langsung return None jg bisa biar gk error'''
        return self.types[index]