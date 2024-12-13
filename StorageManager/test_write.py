import unittest
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.Condition import Condition
from StorageManager.serialize_table import TableCreator
from StorageManager.objects.DataRetrieval import DataRetrieval

class TestWriteBlock(unittest.TestCase):
    def setUp(self):
        """"
        Set up storage manager and mock dependencies
        """
        self.sm = StorageManager()
        self.table_creator = TableCreator()
    
    def test_append_one_data(self):
        self.table_creator.resetTable()

        data_append_one = DataWrite(
            overwrite = False,
            selected_table = "course",
            column=[],
            conditions=[],
            new_value= [[200,2000,"Single Insert Test", "Inserting a Single Tuple using WriteBlock"]]
        )

        cond_for_retrieval = Condition(column="courseid", operation=">", operand="195")

        data_retrieval = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[cond_for_retrieval],
        )

        result = self.sm.writeBlock(data_append_one)
        self.sm.readBlock(data_retrieval)
        self.assertEqual(result,1)
    
    def test_append_multiple_data(self):
        self.table_creator.resetTable()

        data_append_three = DataWrite(
            overwrite = False,
            selected_table = "course",
            column=[],
            conditions=[],
            new_value= [
                            [200,2001, "First Insert", "Inserting Multiple Tuples using WriteBlock"],
                            [201,2002, "Second Insert", "Inserting Multiple Tuples using WriteBlock"],
                            [202,2003, "Last Insert", "Inserting Multiple Tuples using WriteBlock"],\
                        ]
        )

        cond_for_retrieval = Condition(column="coursesid", operation=">", operand="195")

        data_retrieval = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[cond_for_retrieval],
        )

        result = self.sm.writeBlock(data_append_three)
        retrieved_data = self.sm.readBlock(data_retrieval)
        print(retrieved_data)
        self.assertEqual(result,3)
    

    def test_overwrite_one_data(self):
        self.table_creator.resetTable()

        cond1 = Condition(column='studentid', operation="=", operand='10')

        data_overwrite_one = DataWrite(
            overwrite = True,
            selected_table = "student",
            column=[],
            conditions = [cond1],
            new_value =  [[10,"Overwrite Testing", 1.0]]
        )
        
        cond_for_retrieval = Condition(column="studentid", operation="<", operand="15")

        data_retrieval = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[cond_for_retrieval],
        )
        result = self.sm.writeBlock(data_overwrite_one)
        self.sm.readBlock(data_retrieval)
        self.assertEqual(result,1)
    
    def test_overwrite_multiple_data(self):
        self.table_creator.resetTable()

        cond1 = Condition(column='studentid', operation=">", operand='9')
        cond2 = Condition(column="studentid", operation="<", operand="13")

        data_overwrite_one = DataWrite(
            overwrite = True,
            selected_table = "student",
            column=[],
            conditions = [cond1,cond2],
            new_value =  [
                            [10,"First Overwrite", 1.0],
                            [11,"Second Overwrite", 2.0],
                            [12,"Third Overwrite", 3.0]
                          ]
        )
        
        cond_for_retrieval = Condition(column="studentid", operation="<", operand="15")

        data_retrieval = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[cond_for_retrieval],
        )
        result = self.sm.writeBlock(data_overwrite_one)
        self.sm.readBlock(data_retrieval)
        self.assertEqual(result,3)

if __name__ == "__main__":
    unittest.main()



# cond1 = Condition("courseid", '<=', 7)
# cond2 = Condition("harga", '>', 60.00)



# sm = StorageManager()

# retrieval_user2 = DataRetrieval(
#     table=["user2"],
#     column=[],
#     conditions=[cond1]
# )

# retrieval_course = DataRetrieval(
#     table=["course"],
#     column=[],
#     conditions=[]
# )

# retrieval_student = DataRetrieval(
#     table=["student"],
#     column=[],
#     conditions=[cond1]
# )

# writerer = DataWrite(
#     overwrite=False,
#     selected_table="course",
#     column=[],
#     conditions=[cond1],
#     new_value=[[5, 5, '55', 'gaming']]
# )

# before_write = sm.readBlock(retrieval_course)
# print(before_write)
# result = sm.writeBlock(writerer)
# result = sm.writeBlock(writerer)
# print(result)
# after_write = sm.readBlock(retrieval_course)    
# print(after_write)

# overwriterer = DataWrite(
#     overwrite=True,
#     selected_table="course",
#     column=[],
#     conditions=[cond1],
#     new_value=
#     [
#         [0, 2000, '55', 'gaming'],
#         [1, 2001, '55', 'gaming'],
#         [2, 2002, '55', 'gaming'],
#         [3, 2003, '55', 'gaming'],
#         [4, 2004, '55', 'gaming'],
#         [5, 2005, '55', 'gaming'],
#         [6, 2006, '55', 'gaming'],
#         [7, 2007, '55', 'gaming'],
#     ]
# )

# result = sm.writeBlock(overwriterer)
# print("RESULT: ",result)