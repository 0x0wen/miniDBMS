from datetime import datetime
from FailureRecovery.FailureRecovery import FailureRecovery
from Interface.ExecutionResult import ExecutionResult
from Interface.Rows import Rows
from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.DataWrite import DataWrite
#SELECT year FROM course WHERE year >= 2030 AND year < 2040 OR year > 2070 AND year <> 2080

ret2 = DataRetrieval(table=["course"], column=["year"], conditions=[
    Condition(column="year", operation="=", operand=2030, connector=None),
    Condition(column="year", operation="<", operand=2040, connector="AND"),
    Condition(column="year", operation=">", operand=2070, connector="OR"),
    Condition(column="year", operation="<>", operand=2080, connector="AND")
])


#pakai index
ret3 = DataRetrieval(table=["course"], column=['courseid'], conditions=[
    Condition(column="courseid", operation=">", operand='40', connector=None),
    Condition(column="courseid", operation="<", operand='50', connector="AND"),
])

ret4 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="courseid", operation=">", operand='40', connector=None),
    Condition(column="courseid", operation="<", operand='50', connector="AND"),
])
"""
[{'year': 2030}, {'year': 2031}, {'year': 2032}, {'year': 2033}, {'year': 2034},
 {'year': 2035}, {'year': 2036}, {'year': 2037}, {'year': 2038}, {'year': 2039},
  {'year': 2071}, {'year': 2072}, {'year': 2073}, {'year': 2074}, {'year': 2075},
   {'year': 2076}, {'year': 2077}, {'year': 2078}, {'year': 2079}, {'year': 2081}, 
   {'year': 2082}, {'year': 2083}, {'year': 2084}, {'year': 2085}, {'year': 2086}, 
   {'year': 2087}, {'year': 2088}, {'year': 2089}, {'year': 2090}, {'year': 2091}, 
   {'year': 2092}, {'year': 2093}, {'year': 2094}, {'year': 2095}, {'year': 2096}, 
   {'year': 2097}, {'year': 2098}, {'year': 2099}]
Amount :  38
"""

retrieval = DataRetrieval(
    table=["user2"],
    column=[],
    conditions=[]
)

sm = StorageManager()

print("\n---First read with ret4-----")
sm.readBlock(ret4)

print("---Second read with ret4-----")
sm.readBlock(ret4)

transaction_id = 1
timestamp = datetime.now()
message = "Test log entry"
data_before = [{"courseid": 41, "year": 2041, "coursename": "Course Name41", "coursedesc": "Course Deskripsion aaaaa41"}]
data_after = [{"courseid": 41, "year": 3000, "coursename": "AYAMAYAM", "coursedesc": "Course Description baru"}]
# query = DataWrite(
#     overwrite=True,
#     selected_table="course",
#     column=["courseid", "year", "coursename", "coursedesc"],
#     conditions=[Condition(column="courseid", operation="=", operand=41)],
#     new_value=[{"courseid": 41, "year": 3000, "coursename": "AYAMAYAM", "coursedesc": "Course Description baru"}]
# )
table_name = "course"

execution_result = ExecutionResult(
    transaction_id=transaction_id,
    timestamp=timestamp,
    message=message,
    data_before=data_before,
    data_after=data_after,
    table_name=table_name
)

f = FailureRecovery()
f.write_log(execution_result)


from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria
f.recover(RecoverCriteria(transaction_id=1))


# from FailureRecovery.Structs.Row import Row

# row1 = Row({"id": "1", "name":"budi", "age":"12"})
# row2 = Row({"id": "1", "name":"budi", "age":"12"})
# row3 = Row({"id": "2", "name":"doni", "age":"12"})

# if (row1.isRowEqual(row2)):
#     print("equal row1")
# if (row1.isRowEqual(row3)):
#     print("equal row2")