from StorageManager.objects.DataDeletion import DataDeletion

from StorageManager.objects.Condition import Condition
from StorageManager.StorageManager import StorageManager

cond1 = Condition("courseid", '=', 30)


deleted = DataDeletion(
    table="course",
    conditions = [cond1]
)

sm = StorageManager()
print(sm.deleteBlock(deleted))

# import unittest
# from unittest.mock import MagicMock
# from StorageManager.objects.DataDeletion import DataDeletion
# from StorageManager.objects.Condition import Condition
# from StorageManager.StorageManager import StorageManager
# from StorageManager.objects.DataRetrieval import DataRetrieval

# class TestDeleteBlock(unittest.TestCase):
#     def setUp(self):
#         """"
#         Set up strage manager and mock dependencies
#         """
#         self.sm = StorageManager()

#         cond1 = Condition(column="courseid", operation="=", operand=30, connector=None)

#         data_retrieval = DataRetrieval(['course'] , 'courseid', [cond1],[])

#         self.mock_course_data = self.sm.readBlock(data_retrieval)

#     def test_delete_course(self):
#             """
#             Test that a course record is deleted correctly.
#             """
#             # Create a condition to delete a course with courseid=30.
#             cond1 = Condition(column="courseid", operation="=", operand=30)

#             # Set up a DataDeletion object for the 'course' table.
#             data_deletion = DataDeletion(
#                 table="course",
#                 conditions=[cond1]
#             )

#             # Mock the deleteBlock method to simulate deletion.
#             def mock_delete_block(deletion_request : DataDeletion):
#                 self.mock_table_data = [
#                     record for record in self.mock_table_data
#                     if not (record['courseid'] == deletion_request.conditions[0])
#                 ]
#                 return True  # Simulate successful deletion.

#             # self.sm.deleteBlock = MagicMock(side_effect=mock_delete_block(data_deletion))

#             # Perform deletion.
#             delete_result = self.sm.deleteBlock(data_deletion)

#             # Assert the delete operation was successful.
#             self.assertTrue(delete_result)

#             # Assert the record with courseid=30 is removed.
#             remaining_records = [record for record in self.mock_course_data if record['courseid'] == 30]
#             self.assertEqual(len(remaining_records), 0)

#             # Assert the remaining record is intact.
#             self.assertEqual(len(self.mock_course_data), 1)
#             self.assertEqual(self.mock_course_data[0]['courseid'], 31)

# if __name__ == "__main__":
#     unittest.main()
