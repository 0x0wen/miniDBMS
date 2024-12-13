import unittest
import os
from StorageManager.manager.IndexManager import IndexManager, HashIndex
from StorageManager.manager.TableManager import TableManager

# class TestHashIndex(unittest.TestCase):
#     def setUp(self):
#         """Set up instance for testing"""
#         self.table = 'user2'
#         self.column = ''

#         self.index_manager = IndexManager(self.table)
#         self.filepath = "Storage/"
#         self.index_filename = f"{self.table}_index.dat"
    
#     def testWrite(self):
#         """Test writing some data"""
#         self.index_manager.writeIndex(self.table, self.column)
#         self.assertTrue(os.path.exists(self.filepath + self.index_filename))

#     def testRead(self):
#         """Test reading the data"""
        
#         hashIndex = self.index_manager.readIndex(self.table, self.column)
        
#         self.assertTrue(hashIndex.test(), "Check if the length is more than 0")
        

# if __name__ == '__main__':
#     suite = unittest.TestSuite()
#     suite.addTest(TestHashIndex('testRead'))
#     suite.addTest(TestHashIndex('testWrite'))
#     runner = unittest.TextTestRunner(buffer=False)
#     runner.run(suite)

table = "course"
column = 'courseid'
index_manager = IndexManager()
index_manager.writeIndex(table,column)

serializer = TableManager()

def findRecord(table_name: str, column: str, value: str) -> dict:
        """
        Mencari record dari kumpulan blok berdasarkan nilai pada kolom tertentu.
        Args:
            table_name (str): Nama tabel.
            column (str): Nama kolom yang ingin dicari.
            value (str): Nilai yang ingin dicari pada kolom tersebut.
        Returns:
            dict: Record yang ditemukan dalam bentuk dictionary.
        """
        index_manager = IndexManager()
        hashedbucket = index_manager.readIndex(table_name, column)

        block_id = hashedbucket.search(value)
        if block_id is None:
            print(f"Record dengan {column} = {value} tidak ditemukan.")
            return None

        block_data = serializer.readBlockIndex(table_name, block_id)

        for record in block_data:
            column_value = record[column]

            if isinstance(column_value, int):
                value = int(value)  
            elif isinstance(column_value, float):
                value = float(value) 
            else:
                value = str(value) 

            if column_value == value:
                return record  

        print(f"Record dengan {column} = {value} tidak ditemukan di blok {block_id}.")
        return None


#Cari courseid = 23 di table courseid
print(findRecord(table,column,'98'))