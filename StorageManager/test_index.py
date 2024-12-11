import unittest
import time
from StorageManager.manager.IndexManager import IndexManager, HashIndex
from StorageManager.manager.TableManager import TableManager
from StorageManager.objects.Condition import Condition
from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.StorageManager import StorageManager
from StorageManager.serialize_table import TableCreator

class TestSetupIndex(unittest.TestCase):
    def setUp(self):
        """
        Set up storage manager and mock dependencies
        """     
        self.sm = StorageManager()
        self.index_manager = IndexManager()
        self.course_name = 'course'
        self.student_name = 'student'
        self.max_item = 20000
        self.table_create =  TableCreator(self.max_item)
        self.table_create.resetTable()
    
    def test_setup_hash_index(self):
        """
        test indexing on student with student id of 1 and 4000
        """
        data_retrieval = DataRetrieval([self.student_name], [], [
            Condition(self.student_name, "=", 1),
            Condition(self.student_name, "=", self.max_item)
        ])

        # Reset Index
        try:
            self.index_manager.deleteIndex(self.student_name)
        except:
            print(f'Index hasnt been declared')

        print("TEST On Finding a data with index of 1 and "+ self.max_item.__str__() + '\n')
        print("Before Setting Index: ")
        
        start_time = time.time_ns()
        self.sm.readBlock(data_retrieval)
        end_time = time.time_ns()
        times = (end_time - start_time) / 1_000_000
        print(f"Needed Time : {times} ms")
        print()
        self.sm.setIndex(self.student_name,'studentid','hash')
        print("After Setting Index: ")
        start_time = time.time_ns()
        self.sm.readBlock(data_retrieval)
        end_time = time.time_ns()

        times = (end_time - start_time) / 1_000_000
        print(f"Needed Time : {times} ms")

    def test_setup_hash_index_with_lots_of_condition(self):
        f"""
        test indexing on student with student id < {self.max_item} and greater than {self.max_item / 2} and item of id 137
        """
        data_retrieval = DataRetrieval([self.student_name], [], [
            Condition(self.student_name, ">", self.max_item / 2),
            Condition(self.student_name, "<", self.max_item),
            Condition(self.student_name, "=", 137)
        ])

        # Reset Index
        try:
            self.index_manager.deleteIndex(self.student_name)
        except:
            print(f'Index hasnt been declared')

        print("TEST On Finding a data with index of 1 and "+ self.max_item.__str__() + '\n')
        print("Before Setting Index: ")
        
        start_time = time.time_ns()
        self.sm.readBlock(data_retrieval)
        end_time = time.time_ns()
        times = (end_time - start_time) / 1_000_000
        print(f"Needed Time : {times} ms")
        print()
        self.sm.setIndex(self.student_name,'studentid','hash')
        print("After Setting Index: ")
        start_time = time.time_ns()
        self.sm.readBlock(data_retrieval)
        end_time = time.time_ns()

        times = (end_time - start_time) / 1_000_000
        print(f"Needed Time : {times} ms")
        print()

if __name__ == "__main__":
    unittest.main()