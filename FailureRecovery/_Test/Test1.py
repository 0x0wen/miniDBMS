from datetime import datetime

from Interface.ExecutionResult import ExecutionResult

from StorageManager.objects.DataRetrieval import DataRetrieval, Condition
from StorageManager.StorageManager import StorageManager

from FailureRecovery.FailureRecovery import FailureRecovery
from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria

print("--Initializing FailureRecovery, StorageManager:\n")

failureRecovery = FailureRecovery()
storageManager = StorageManager()

'''
QUERY 1: Update table course: coursedesc dengan id 15-20 menjadi Study of crime, criminal behavior, and societal impacts
'''
print("Query 1:\n")
print(" (transaction_id = 1) UPDATE course SET coursedesc = 'Study of crime, criminal behavior, and societal impacts' WHERE courseid >= 15 AND courseid <= 20;")
print()
print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)

ret_1 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="courseid", operation=">=", operand=15, connector=None),
    Condition(column="courseid", operation="<=", operand=20, connector="AND"),
])

data_before = storageManager.readBlock(ret_1)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

data_after = []
for row in data_before:
    new_row = row.copy()
    new_row['coursedesc'] = 'Study of crime, criminal behavior, and societal impacts'
    data_after.append(new_row)


exec_res_1 = ExecutionResult(
    transaction_id=1,
    timestamp=datetime.now(),
    message="SUCCESS",
    table_name="course",
    data_before=data_before,
    data_after=data_after,
)

'''
QUERY 2: Update table student: mengubah gpa dibawah 2 menjadi 2
'''
print("Query 2:\n")
print(" (transaction_id = 2) UPDATE student SET gpa = 2.0 WHERE gpa < 2.0;")
print()
print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)

ret_2 = DataRetrieval(table=["student"], column=[], conditions=[
    Condition(column="gpa", operation="<", operand=2.0, connector=None),
])

data_before = storageManager.readBlock(ret_2)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

data_after = []
for row in data_before:
    new_row = row.copy()
    new_row['gpa'] = 2.0
    data_after.append(new_row)

exec_res_2 = ExecutionResult(
    transaction_id=2,
    timestamp=datetime.now(),
    message="SUCCESS",
    table_name="student",
    data_before= data_before,
    data_after=data_after,
)

'''
QUERY 3: Update table course: coursename dengan id 25-30 menjadi Intelegensi Buatan
'''

print("Query 3:\n")
print(" (transaction_id = 1) UPDATE course SET coursename = 'Intelegensi Buatan' WHERE courseid >= 25 AND courseid <= 30;")
print()
print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)

ret_3 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="courseid", operation=">=", operand=25, connector=None),
    Condition(column="courseid", operation="<=", operand=30, connector="AND"),
])

data_before = storageManager.readBlock(ret_3)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

data_after = []
for row in data_before:
    new_row = row.copy()
    new_row['coursename'] = 'Intelegensi Buatan'
    data_after.append(new_row)

exec_res_3= ExecutionResult(
    transaction_id=1,
    timestamp=datetime.now(),
    message="SUCCESS",
    table_name="course",
    data_before=data_before,
    data_after=data_after,
)

print("--FailureRecovery.buffer before write_log():\n")
print(failureRecovery.buffer)

failureRecovery.write_log(exec_res_1)
failureRecovery.write_log(exec_res_2)
failureRecovery.write_log(exec_res_3)

print("--FailureRecovery.buffer after write_log():\n")
print(failureRecovery.buffer)


print("--Trying FailureRecovery.recover():\n")
print("rec_criteria = RecoverCriteria(transaction_id=1)\n")

print("--FailureRecovery.buffer before recover():\n")

print(failureRecovery.buffer)

failureRecovery.recover(RecoverCriteria(transaction_id=1))

print("--FailureRecovery.buffer after recover():\n")

print(failureRecovery.buffer)


print("--Trying FailureRecovery.save_checkpoint():\n")
chekpoint = failureRecovery.save_checkpoint()

print("--Checkpoint entries:\n")
for entry in chekpoint:
    print(entry, "\n")

print("--FailureRecovery.buffer after save_checkpoint():\n")
print(failureRecovery.buffer)