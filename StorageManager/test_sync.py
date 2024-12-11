import unittest
from StorageManager.manager.TableManager import TableManager
from FailureRecovery.FailureRecovery import FailureRecovery
from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.Rows import Rows
from FailureRecovery.Structs.Table import Table
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.Condition import Condition
# from FailureRecovery.Structs.Header import Header
from FailureRecovery.Structs.Row import Row


class TestSynchronizeStorage(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment with initial buffer and physical storage data.
        """
        # Initialize TableManager and FailureRecovery
        self.serializer = TableManager(path_name="Storage/")
        self.failure_recovery = FailureRecovery()
        self.table_name = "test_table"

        # Define schema
        self.course_schema = [
            ('courseid', 'int', 4),
            ('year', 'int', 4),
            ('coursename', 'varchar', 50),
            ('coursedesc', 'varchar', 50),
        ]

        # Initial data for physical storage
        self.physical_storage_data = [
            [1, 1000, 'Course Name 1', 'Course Desc 1'],
            [2, 2000, 'Course Name 2', 'Course Desc 2'], 
            [3, 3000, 'Course Name 3', 'Course Desc 3'], 
            [4, 4000, 'Course Name 4', 'Course Desc 4'], 
            [5, 5000, 'Course Name 5', 'Course Desc 5'], 
            [6, 6000, 'Course Name 6', 'Course Desc 6'], 
        ]

        # Initial data for buffer
        self.buffer_data = [
            [1, 1000, 'Course Name 1', 'Course Desc 1'],
            [2, 2000, 'Course Name 2', 'Course Desc 2'], 
            [3, 8888, 'New Course Name 3', 'New Desc 3'],          
            [4, 9999, 'New Course Name BARU', 'New Desc BARU'],
        ]

        # Write initial data to physical storage
        self.serializer.writeTable(self.table_name, self.physical_storage_data, self.course_schema)
   
     
        table = Table(self.table_name)
        for row in self.buffer_data:
            table.addRow(row)

        self.failure_recovery.buffer.addTabble(table)

    def test_synchronize_storage(self):
        """
        Test if synchronize_storage correctly updates physical storage based on buffer.
        """
        # Synchronize storage
        storage_manager = StorageManager()
        storage_manager.synchronize_storage()

        # Read updated data from physical storage
        updated_data = self.serializer.readTable(self.table_name)

        # Expected data after synchronization
        expected_data = [
            {'courseid': 1, 'year': 1000, 'coursename': 'Course Name 1', 'coursedesc': 'Course Desc 1'},
            {'courseid': 2, 'year': 2000, 'coursename': 'Course Name 2', 'coursedesc': 'Course Desc 2'},
            {'courseid': 3, 'year': 8888, 'coursename': 'New Course Name 3', 'coursedesc': 'New Desc 3'},
            {'courseid': 4, 'year': 9999, 'coursename': 'New Course Name BARU', 'coursedesc': 'New Desc BARU'},
            {'courseid': 5, 'year': 5000, 'coursename': 'Course Name 5', 'coursedesc': 'Course Desc 5'},
            {'courseid': 6, 'year': 6000, 'coursename': 'Course Name 6', 'coursedesc': 'Course Desc 6'},
        ]

        # Convert rows to dictionary for comparison
        updated_data_dicts = [
            {col: row[col] for col in row} for row in updated_data
        ]

        # Check if updated data matches the expected data
        self.assertEqual(updated_data_dicts, expected_data)

    def tearDown(self):
        """
        Clean up after the test.
        """
        import os
        # Remove files created during the test
        for suffix in ['_data.dat', '_scheme.dat', '_blocks.dat']:
            file_path = f"Storage/{self.table_name}{suffix}"
            if os.path.exists(file_path):
                os.remove(file_path)

if __name__ == '__main__':
    unittest.main()
