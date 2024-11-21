from StorageManager.StorageManager import StorageManager
from StorageManager.objects.Condition import Condition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataRetrieval import DataRetrieval

cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)



sm = StorageManager()

retrieval_user2 = DataRetrieval(
    table=["user2"],
    column=[],
    conditions=[cond1]
)

retrieval_course = DataRetrieval(
    table=["course"],
    column=[],
    conditions=[]
)

retrieval_student = DataRetrieval(
    table=["student"],
    column=[],
    conditions=[cond1]
)

writerer = DataWrite(
    overwrite=False,
    selected_table="course",
    column=[],
    conditions=[cond1],
    new_value=[[5, 5, '55', 'gaming']]
)


before_write = sm.readBlock(retrieval_course)
print(before_write)
result = sm.writeBlock(writerer)
result = sm.writeBlock(writerer)
print(result)
after_write = sm.readBlock(retrieval_course)    
print(after_write)