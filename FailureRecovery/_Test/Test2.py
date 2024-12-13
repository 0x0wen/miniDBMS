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






print("--Trying FailureRecovery.write_log() (UPDATE operation):\n")
print("Query:")
print(" (transaction_id = 1) UPDATE course SET coursename = 'Sistem Basis Data' WHERE courseid >= 15 AND courseid <= 20;")

print("\n--Need to read the data first before updating it\n")

print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)

ret_1 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="year", operation=">=", operand=2015, connector=None),
    Condition(column="year", operation="<=", operand=2020, connector="AND"),
])
data_before = storageManager.readBlock(ret_1)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

data_after = []
for row in data_before:
    new_row = row.copy()
    new_row['coursename'] = 'Sistem Basis Data'
    data_after.append(new_row)

exec_res_1 = ExecutionResult(
    transaction_id=1,
    timestamp=datetime.now(),
    message="SUCCESS",
    table_name="course",
    data_before=data_before,
    data_after=data_after,
)

failureRecovery.write_log(exec_res_1)

print("--FailureRecovery.buffer after write_log():\n")
print(failureRecovery.buffer)







print("--Trying FailureRecovery.write_log() (UPDATE operation):\n")
print("Query:")
print(" (transaction_id = 1) UPDATE course SET coursename = 'Pemrograman Berbasis Objek' WHERE courseid >= 18 AND courseid <= 23;")

print("\n--Need to read the data first before updating it\n")
print("--FailureRecovery.buffer before read:\n")
print(failureRecovery.buffer)

ret_2 = DataRetrieval(table=["course"], column=[], conditions=[
    Condition(column="year", operation=">=", operand=2018, connector=None),
    Condition(column="year", operation="<=", operand=2023, connector="AND"),
])
data_before = storageManager.readBlock(ret_2)

print("--FailureRecovery.buffer after read:\n")
print(failureRecovery.buffer)

data_after = []
for row in data_before:
    new_row = row.copy()
    new_row['coursename'] = 'Intelegensi Buatan'
    data_after.append(new_row)

exec_res_2 = ExecutionResult(
    transaction_id=1,
    timestamp=datetime.now(),
    message="SUCCESS",
    table_name="course",
    data_before=data_before,
    data_after=data_after,
)

failureRecovery.write_log(exec_res_2)

print("--FailureRecovery.buffer after write_log():\n")
print(failureRecovery.buffer)