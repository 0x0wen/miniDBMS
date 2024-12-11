from datetime import datetime

from Interface.ExecutionResult import ExecutionResult

from StorageManager.objects.DataRetrieval import DataRetrieval, Condition
from StorageManager.StorageManager import StorageManager

from FailureRecovery.FailureRecovery import FailureRecovery
from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria

print("--Initializing FailureRecovery, StorageManager:\n")

failureRecovery = FailureRecovery()
storageManager = StorageManager()

print()

print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)


ret_1 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="year", operation=">=", operand=2010, connector=None),
    Condition(column="year", operation="<=", operand=2030, connector="AND"),
])
ret_2 = DataRetrieval(table=["student"], column=[], conditions=[
    Condition(column="gpa", operation="<", operand=3, connector=None),
])
ret_3 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="year", operation=">=", operand=2020, connector=None),
    Condition(column="year", operation="<=", operand=2040, connector="AND"),
])


storageManager.readBlock(ret_1)
storageManager.readBlock(ret_2)
storageManager.readBlock(ret_3)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

# print("--Trying FailureRecovery.write_log() (UPDATE operation):\n")
# print("Query:")
# print(" (transaction_id = 1) UPDATE course SET coursename = 'Sistem Basis Data' WHERE courseid >= 15 AND courseid <= 20;")
# print(" (transaction_id = 2) UPDATE student SET gpa = 2.0 WHERE gpa < 2.0;")
# print(" (transaction_id = 1) UPDATE course SET coursename = 'Intelegensi Buatan' WHERE courseid >= 25 AND courseid <= 30;")
# print()

# '''
# QUERY 1: Update table course: coursename dengan id 15-20 menjadi Sistem Basis Data
# '''
# exec_res_1 = ExecutionResult(
#     transaction_id=1,
#     timestamp=datetime.now(),
#     message="SUCCESS",
#     table_name="course",
#     data_before=[{'courseid': 15, 'year': 2015, 'coursename': 'Course Name15', 'coursedesc': 'Course Deskripsion aaaaa15'},
#                  {'courseid': 16, 'year': 2016, 'coursename': 'Course Name16', 'coursedesc': 'Course Deskripsion aaaaa16'},
#                  {'courseid': 17, 'year': 2017, 'coursename': 'Course Name17', 'coursedesc': 'Course Deskripsion aaaaa17'},
#                  {'courseid': 18, 'year': 2018, 'coursename': 'Course Name18', 'coursedesc': 'Course Deskripsion aaaaa18'},
#                  {'courseid': 19, 'year': 2019, 'coursename': 'Course Name19', 'coursedesc': 'Course Deskripsion aaaaa19'},
#                  {'courseid': 20, 'year': 2020, 'coursename': 'Course Name20', 'coursedesc': 'Course Deskripsion aaaaa20'}],
#     data_after=[{'courseid': 15, 'year': 2015, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa15'},
#                  {'courseid': 16, 'year': 2016, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa16'},
#                  {'courseid': 17, 'year': 2017, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa17'},
#                  {'courseid': 18, 'year': 2018, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa18'},
#                  {'courseid': 19, 'year': 2019, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa19'},
#                  {'courseid': 20, 'year': 2020, 'coursename': 'Sistem Basis Data', 'coursedesc': 'Course Deskripsion aaaaa20'}],
# )

# '''
# QUERY 2: Update table student: mengubah gpa dibawah 2 menjadi 2
# '''
# exec_res_2 = ExecutionResult(
#     transaction_id=2,
#     timestamp=datetime.now(),
#     message="SUCCESS",
#     table_name="student",
#     data_before=[{'studentid': 0, 'fullname': 'Belakang Depan Student0', 'gpa': 0.0},
#                  {'studentid': 1, 'fullname': 'Belakang Depan Student1', 'gpa': 0.25},
#                  {'studentid': 2, 'fullname': 'Belakang Depan Student2', 'gpa': 0.5},
#                  {'studentid': 3, 'fullname': 'Belakang Depan Student3', 'gpa': 0.75},
#                  {'studentid': 4, 'fullname': 'Belakang Depan Student4', 'gpa': 1.0},
#                  {'studentid': 5, 'fullname': 'Belakang Depan Student5', 'gpa': 1.25},
#                  {'studentid': 6, 'fullname': 'Belakang Depan Student6', 'gpa': 1.5},
#                  {'studentid': 7, 'fullname': 'Belakang Depan Student7', 'gpa': 1.75}],
#     data_after=[{'studentid': 0, 'fullname': 'Belakang Depan Student0', 'gpa': 2.0},
#                  {'studentid': 1, 'fullname': 'Belakang Depan Student1', 'gpa': 2.0},
#                  {'studentid': 2, 'fullname': 'Belakang Depan Student2', 'gpa': 2.0},
#                  {'studentid': 3, 'fullname': 'Belakang Depan Student3', 'gpa': 2.0},
#                  {'studentid': 4, 'fullname': 'Belakang Depan Student4', 'gpa': 2.0},
#                  {'studentid': 5, 'fullname': 'Belakang Depan Student5', 'gpa': 2.0},
#                  {'studentid': 6, 'fullname': 'Belakang Depan Student6', 'gpa': 2.0},
#                  {'studentid': 7, 'fullname': 'Belakang Depan Student7', 'gpa': 2.0}],
# )

# '''
# QUERY 3: Update table course: coursename dengan id 25-30 menjadi Intelegensi Buatan
# '''
# exec_res_3= ExecutionResult(
#     transaction_id=1,
#     timestamp=datetime.now(),
#     message="SUCCESS",
#     table_name="course",
#     data_before=[{'courseid': 25, 'year': 2025, 'coursename': 'Course Name25', 'coursedesc': 'Course Deskripsion aaaaa25'},
#                  {'courseid': 26, 'year': 2026, 'coursename': 'Course Name26', 'coursedesc': 'Course Deskripsion aaaaa26'},
#                  {'courseid': 27, 'year': 2027, 'coursename': 'Course Name27', 'coursedesc': 'Course Deskripsion aaaaa27'},
#                  {'courseid': 28, 'year': 2028, 'coursename': 'Course Name28', 'coursedesc': 'Course Deskripsion aaaaa28'},
#                  {'courseid': 29, 'year': 2029, 'coursename': 'Course Name29', 'coursedesc': 'Course Deskripsion aaaaa29'},
#                  {'courseid': 30, 'year': 2030, 'coursename': 'Course Name20', 'coursedesc': 'Course Deskripsion aaaaa30'}],
#     data_after=[{'courseid': 25, 'year': 2025, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa25'},
#                  {'courseid': 26, 'year': 2026, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa26'},
#                  {'courseid': 27, 'year': 2027, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa27'},
#                  {'courseid': 28, 'year': 2028, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa28'},
#                  {'courseid': 29, 'year': 2029, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa29'},
#                  {'courseid': 30, 'year': 2030, 'coursename': 'Intelegensi Buatan', 'coursedesc': 'Course Deskripsion aaaaa30'}],
# )

# failureRecovery.write_log(exec_res_1)
# failureRecovery.write_log(exec_res_2)
# failureRecovery.write_log(exec_res_3)

# print("--FailureRecovery.buffer after write_log():\n")
# print(failureRecovery.buffer)


# print("--Trying FailureRecovery.recover():\n")
# print("rec_criteria = RecoverCriteria(transaction_id=1)")

# failureRecovery.recover(RecoverCriteria(transaction_id=1))

# print("--FailureRecovery.buffer after recover():\n")

# print(failureRecovery.buffer)


# print("--Trying FailureRecovery.save_checkpoint():\n")
# chekpoint = failureRecovery.save_checkpoint()

# print("--Checkpoint entries:\n")
# for entry in chekpoint:
#     print(entry, "\n")

# print("--FailureRecovery.buffer after save_checkpoint():\n")
# print(failureRecovery.buffer)
