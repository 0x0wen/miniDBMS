import unittest
from StorageManager.manager.TableManager import TableManager
from FailureRecovery.FailureRecovery import FailureRecovery
from StorageManager.objects.Rows import Rows
from FailureRecovery.Structs.Table import Table
from StorageManager.StorageManager import StorageManager
from FailureRecovery.Structs.Row import Row

class TestSynchronizeMultipleTables(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment with initial buffer and physical storage data for two tables.
        """
        # Initialize TableManager and FailureRecovery
        self.serializer = TableManager(path_name="Storage/")
        self.failure_recovery = FailureRecovery()
        
        # Define schemas
        self.table_1_name = "test_table_1"
        self.table_2_name = "test_table_2"

        self.schema_1 = [
            ('courseid', 'int', 4),
            ('year', 'int', 4),
            ('coursename', 'varchar', 50),
            ('coursedesc', 'varchar', 50),
        ]

        self.schema_2 = [
            ('studentid', 'int', 4),
            ('name', 'varchar', 50),
            ('gpa', 'float', 4),
        ]

        # Initial data for physical storage
        self.physical_data_1 = [
            [1, 1000, 'Course Name 1', 'Course Desc 1'],
            [2, 2000, 'Course Name 2', 'Course Desc 2'],
            [3, 3000, 'Course Name 3', 'Course Desc 3'],
            [4, 4000, 'Course Name 4', 'Course Desc 4'],
            [5, 5000, 'Course Name 5', 'Course Desc 5'],
            [6, 6000, 'Course Name 6', 'Course Desc 6'],
        ]

        self.physical_data_2 = [
            [1, 'Alice', 3.5],
            [2, 'Bob', 3.2],
            [3, 'Charlie', 3.8],
            [4, 'Delta', 3.7],
            [5, 'Elia', 3.1],
            [6, 'Florida', 3.0],
        ]

        # Initial data for buffer
        self.buffer_data_1 = [
            Row({'courseid': 1, 'year': 1000, 'coursename': 'Updated Course Name 1', 'coursedesc': 'Updated Desc 1'}),
            Row({'courseid': 2, 'year': 2000, 'coursename': 'Course Name 2', 'coursedesc': 'Course Desc 2'}),
            Row({'courseid': 3, 'year': 3000, 'coursename': 'New Course Name 3', 'coursedesc': 'New Desc 3'}),
            Row({'courseid': 5, 'year': 5000, 'coursename': 'Updated Course Name 5', 'coursedesc': 'Updated Desc 5'}),
        ]

        self.buffer_data_2 = [
            Row({'studentid': 1, 'name': 'Alice', 'gpa': 3.5}),
            Row({'studentid': 2, 'name': 'Updated Bob', 'gpa': 3.6}),
            Row({'studentid': 3, 'name': 'New Charlie', 'gpa': 4.0}),
            Row({'studentid': 5, 'name': 'Elia', 'gpa': 3.1}),
        ]

        # Write initial data to physical storage
        self.serializer.writeTable(self.table_1_name, self.physical_data_1, self.schema_1)
        self.serializer.writeTable(self.table_2_name, self.physical_data_2, self.schema_2)

        # Set up buffer for both tables
        table_1 = Table(self.table_1_name)
        table_2 = Table(self.table_2_name)

        for row in self.buffer_data_1:
            table_1.addRow(row)

        for row in self.buffer_data_2:
            table_2.addRow(row)

        self.failure_recovery.buffer.addTabble(table_1)
        self.failure_recovery.buffer.addTabble(table_2)

    def test_synchronize_storage(self):
        """
        Test if synchronize_storage correctly updates physical storage for multiple tables.
        """
        # Synchronize storage
        storage_manager = StorageManager()
        storage_manager.synchronize_storage()

        # Read updated data from physical storage
        updated_data_1 = self.serializer.readTable(self.table_1_name)
        updated_data_2 = self.serializer.readTable(self.table_2_name)
        
        # Round float values to 2 decimal places
        updated_data_2_rounded = [
            {k: (round(v, 2) if isinstance(v, float) else v) for k, v in row.items()}
            for row in updated_data_2
        ]

        # Expected data after synchronization
        expected_data_1 = [
            {'courseid': 1, 'year': 1000, 'coursename': 'Updated Course Name 1', 'coursedesc': 'Updated Desc 1'},
            {'courseid': 2, 'year': 2000, 'coursename': 'Course Name 2', 'coursedesc': 'Course Desc 2'},
            {'courseid': 3, 'year': 3000, 'coursename': 'New Course Name 3', 'coursedesc': 'New Desc 3'},
            {'courseid': 4, 'year': 4000, 'coursename': 'Course Name 4', 'coursedesc': 'Course Desc 4'},
            {'courseid': 5, 'year': 5000, 'coursename': 'Updated Course Name 5', 'coursedesc': 'Updated Desc 5'},
            {'courseid': 6, 'year': 6000, 'coursename': 'Course Name 6', 'coursedesc': 'Course Desc 6'},
        ]

        expected_data_2 = [
            {'studentid': 1, 'name': 'Alice', 'gpa': 3.5},
            {'studentid': 2, 'name': 'Updated Bob', 'gpa': 3.6},
            {'studentid': 3, 'name': 'New Charlie', 'gpa': 4.0},
            {'studentid': 4, 'name': 'Delta', 'gpa': 3.7},
            {'studentid': 5, 'name': 'Elia', 'gpa': 3.1},
            {'studentid': 6, 'name': 'Florida', 'gpa': 3.0},
        ]
        
        expected_buffer_data = [] # Buffer should be empty after synchronization
        
        # Check if updated data matches the expected data
        print(updated_data_1)
        print(expected_data_1)
        self.assertEqual(updated_data_1, expected_data_1)
        self.assertEqual(updated_data_2_rounded, expected_data_2)
        self.assertEqual(expected_buffer_data, self.failure_recovery.buffer.getTables())
        

    def tearDown(self):
        """
        Clean up after the test.
        """
        import os
        # Remove files created during the test
        for table_name in [self.table_1_name, self.table_2_name]:
            for suffix in ['_data.dat', '_scheme.dat', '_blocks.dat']:
                file_path = f"Storage/{table_name}{suffix}"
                if os.path.exists(file_path):
                    os.remove(file_path)

if __name__ == '__main__':
    unittest.main()
