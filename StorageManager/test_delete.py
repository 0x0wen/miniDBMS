import unittest
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Condition import Condition
from StorageManager.StorageManager import StorageManager
from StorageManager.serialize_table import TableCreator
from StorageManager.objects.DataRetrieval import DataRetrieval

class TestDeleteBlock(unittest.TestCase):
    def setUp(self):
        """"
        Set up storage manager and mock dependencies
        """
        self.sm = StorageManager()
        self.table_creator = TableCreator(120)

    def test_delete_one_course_data(self):
        """
        Test that a course record is deleted correctly.
        """
        self.table_creator.resetTable()

        # Create a condition to delete a course with courseid=30.
        cond1 = Condition(column="courseid", operation="=", operand=30)

        # Set up a DataDeletion object for the 'course' table.
        data_deletion = DataDeletion(
            table="course",
            conditions=[cond1]
        )

        # Perform deletion.
        delete_result = self.sm.deleteBlock(data_deletion)
        self.assertEqual(delete_result, 1)
    
    def test_delete_fifty_student_data(self):
         """
         Test that a course record is deleted of 50
         """
         self.table_creator.resetTable()

         # Condition that delete 50 first data
         cond1 = Condition(column='studentid', operation="<=", operand='50')

         # Set up data deletion for student table
         data_deletion = DataDeletion(
             table='student',
             conditions=[cond1]
         )

         delete_result = self.sm.deleteBlock(data_deletion)
         self.assertEqual(delete_result, 50)

    def test_delete_multiple_condition(self):
        """
        Test deleting condition of course that has name of Mathematics and year more than 2000 and under 2020
         """
        
        
        
        # Condition of mathematics and 2020 year
        condYear = Condition(column='year',operation='>=', operand='2000')
        condYear2 = Condition(column='year', operation='<', operand='2010')
        condName = Condition(column='coursename', operation='=', operand="Graphic Design")

        data_retrieval_name = DataRetrieval(table=['course'], column=[],conditions=[condName])
        data_retrieval_year = DataRetrieval(['course'], [],[condYear, condYear2])

        nameDeletion = DataDeletion(table='course',conditions=[condName])
        yearDeletion = DataDeletion(table='course', conditions=[condYear, condYear2])

        retrieve_data_name = self.sm.readBlock(data_retrieval_name)
        retrieve_data_year = self.sm.readBlock(data_retrieval_year)

        print("Before Deletion: ")
        print("----------Course Name = Graphic Design----------")
        print(retrieve_data_name.__str__())
        print("----------Year = 2000 - 2010----------")
        print(retrieve_data_year.__str__())

        retrieve_data_name = self.sm.readBlock(data_retrieval_name)
        retrieve_data_year = self.sm.readBlock(data_retrieval_year)

        print("After Deletion: ")
        print("----------Course Name = Graphic Design----------")
        self.sm.deleteBlock(nameDeletion)
        print(retrieve_data_name.__str__())

        print("----------Year = 2000 - 2010----------")
        self.sm.deleteBlock(yearDeletion)
        print(retrieve_data_year.__str__())
         

if __name__ == "__main__":
    unittest.main()
