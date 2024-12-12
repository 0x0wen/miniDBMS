from datetime import datetime
from FailureRecovery.FailureRecovery import FailureRecovery
from Interface.ExecutionResult import ExecutionResult
from Interface.Rows import Rows
from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.serialize_table import TableCreator

import unittest



class TestReadBlock(unittest.TestCase):
    def setUp(self):
        self.sm = StorageManager()
        self.totalrecord = 75
        self.table_creator = TableCreator(self.totalrecord)
    
    def test_read_column_index(self):
        self.table_creator.resetTable()
        #Testing read with 'courseid'as condition, which is the index for course table
        data_ret = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[
                Condition(column="courseid", operation="=", operand=30,connector=None)
            ]
        )
        result = self.sm.readBlock(data_ret)
        print("Hasil Test ke-1")
        print(result)
        self.assertEqual(result[0]['courseid'], 30)

    def test_read_multiple_block_indexing(self):
        self.table_creator.resetTable()
        # Testing read with 'courseid' as condition, which is the index for course table
        data_ret = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[
                Condition(column="courseid", operation=">", operand='40', connector=None),
                Condition(column="courseid", operation="<", operand='50', connector="AND"),
            ]
        )

        result = self.sm.readBlock(data_ret)
        print("Hasil Test ke-2")

        print(result)
        # Assert the number of results
        self.assertEqual(len(result), 9)

        # Assert each result has 'courseid' in the range [41, 49]
        for record in result:
            courseid = record.get("courseid")
            self.assertTrue(41 <= courseid <= 49, f"courseid {courseid} out of range")

    def test_read_block_without_index_key(self):
        self.table_creator.resetTable()
        data_ret = DataRetrieval(
            table=["course"],
            column=['year'],
            conditions=[
            ]
        )
        result = self.sm.readBlock(data_ret)
        print("Hasil Test ke-3")
        print(result)
        self.assertEqual(len(result),self.totalrecord)

    def test_read_with_or_condition(self):
        self.table_creator.resetTable()
        data_ret = DataRetrieval(
            table=["course"],
            column=[],
            conditions=[
                Condition(column="courseid", operation="=", operand=10, connector=None),
                Condition(column="courseid", operation="=", operand=20, connector="OR"),
            ]
        )
        result = self.sm.readBlock(data_ret)
        print("Hasil Test ke-4")
        print(result)
        self.assertEqual(len(result), 2)  # Harus ada 2 hasil dengan courseid = 10 dan 20
        self.assertTrue(all(r['courseid'] in [10, 20] for r in result))


    def test_read_specific_columns(self):
        self.table_creator.resetTable()
        data_ret = DataRetrieval(
            table=["course"],
            column=['courseid', 'coursename'],  
            conditions=[]
        )
        result = self.sm.readBlock(data_ret)
        print("Hasil Test ke-5")
        print(result)
        self.assertEqual(len(result), self.totalrecord)
        for record in result:
            self.assertIn('courseid', record)
            self.assertIn('coursename', record)
            self.assertNotIn('year', record)  # Kolom 'year' tidak boleh ada

    def test_year_in_range(self):
            self.table_creator.resetTable()
            # Retrieve all rows and select only the 'year' column
            data_ret = DataRetrieval(
                table=["course"],
                column=['year'],
                conditions=[
                    Condition(column="year", operation=">", operand='2010', connector=None),
                    Condition(column="year", operation="<", operand='2020', connector="AND"),

                ]
            )
            result = self.sm.readBlock(data_ret)

            print("Hasil Test Year in Range")
            print(result)

            # Assert all 'year' values are within the range [2010, 2020]
            for record in result:
                year = record.get('year')
                self.assertIsNotNone(year, "Year value is missing")
                self.assertTrue(2010 <= year <= 2020, f"Year {year} out of range [2010, 2020]")


if __name__ == "__main__":
    unittest.main()














# ret2 = DataRetrieval(table=["course"], column=["year"], conditions=[
#     Condition(column="year", operation="=", operand=2030, connector=None),
#     Condition(column="year", operation="<", operand=2040, connector="AND"),
#     Condition(column="year", operation=">", operand=2070, connector="OR"),
#     Condition(column="year", operation="<>", operand=2080, connector="AND")
# ])


# #pakai index
# ret3 = DataRetrieval(table=["course"], column=['courseid'], conditions=[
#     Condition(column="courseid", operation=">", operand='40', connector=None),
#     Condition(column="courseid", operation="<", operand='50', connector="AND"),
# ])

# ret4 = DataRetrieval(table=["course"], column=[], conditions=[
#     Condition(column="courseid", operation=">", operand='40', connector=None),
#     Condition(column="courseid", operation="<", operand='50', connector="AND"),
# ])
# """
# [{'year': 2030}, {'year': 2031}, {'year': 2032}, {'year': 2033}, {'year': 2034},
#  {'year': 2035}, {'year': 2036}, {'year': 2037}, {'year': 2038}, {'year': 2039},
#   {'year': 2071}, {'year': 2072}, {'year': 2073}, {'year': 2074}, {'year': 2075},
#    {'year': 2076}, {'year': 2077}, {'year': 2078}, {'year': 2079}, {'year': 2081}, 
#    {'year': 2082}, {'year': 2083}, {'year': 2084}, {'year': 2085}, {'year': 2086}, 
#    {'year': 2087}, {'year': 2088}, {'year': 2089}, {'year': 2090}, {'year': 2091}, 
#    {'year': 2092}, {'year': 2093}, {'year': 2094}, {'year': 2095}, {'year': 2096}, 
#    {'year': 2097}, {'year': 2098}, {'year': 2099}]
# Amount :  38
# """
# cond1 = Condition(column="studentid", operation="=", operand='31', connector=None)

# retrieval = DataRetrieval(
#     table=["student"],
#     column=['studentid'],
#     conditions=[]
# )
# sm = StorageManager()

# testdel = DataRetrieval(
#     table=['course'],
#     column=[],
#     conditions=[
#         Condition(column='courseid',operation='=',operand='30',connector=None)
#     ]
# )

# print("---Second read with ret4-----")
# sm.readBlock(testdel)

# transaction_id = 1
# timestamp = datetime.now()
# message = "Test log entry"
# data_before = [{"courseid": 41, "year": 2041, "coursename": "Course Name41", "coursedesc": "Course Deskripsion aaaaa41"}]
# data_after = [{"courseid": 41, "year": 3000, "coursename": "AYAMAYAM", "coursedesc": "Course Description baru"}]
# # query = DataWrite(
# #     overwrite=True,
# #     selected_table="course",
# #     column=["courseid", "year", "coursename", "coursedesc"],
# #     conditions=[Condition(column="courseid", operation="=", operand=41)],
# #     new_value=[{"courseid": 41, "year": 3000, "coursename": "AYAMAYAM", "coursedesc": "Course Description baru"}]
# # )
# table_name = "course"

# execution_result = ExecutionResult(
#     transaction_id=transaction_id,
#     timestamp=timestamp,
#     message=message,
#     data_before=data_before,
#     data_after=data_after,
#     table_name=table_name
# )

# f = FailureRecovery()
# f.write_log(execution_result)


# from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria
# f.recover(RecoverCriteria(transaction_id=1))


# # from FailureRecovery.Structs.Row import Row

# # row1 = Row({"id": "1", "name":"budi", "age":"12"})
# # row2 = Row({"id": "1", "name":"budi", "age":"12"})
# # row3 = Row({"id": "2", "name":"doni", "age":"12"})

# # if (row1.isRowEqual(row2)):
# #     print("equal row1")
# # if (row1.isRowEqual(row3)):
# #     print("equal row2")