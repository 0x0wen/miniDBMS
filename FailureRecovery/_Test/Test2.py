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