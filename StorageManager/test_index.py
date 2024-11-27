import unittest
import os
from StorageManager.manager.IndexManager import IndexManager, HashIndex
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

table = "user2"
column = 'id'
index_manager = IndexManager()
index_manager.writeIndex(table,column)
