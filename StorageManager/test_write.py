from StorageManager.StorageManager import StorageManager
from StorageManager.objects.Condition import Condition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataRetrieval import DataRetrieval

cond1 = Condition("courseid", '<=', 7)
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

# before_write = sm.readBlock(retrieval_course)
# print(before_write)
# result = sm.writeBlock(writerer)
# result = sm.writeBlock(writerer)
# print(result)
# after_write = sm.readBlock(retrieval_course)    
# print(after_write)

overwriterer = DataWrite(
    overwrite=True,
    selected_table="course",
    column=[],
    conditions=[cond1],
    new_value=
    [
        [0, 2000, '55', 'gaming'],
        [1, 2001, '55', 'gaming'],
        [2, 2002, '55', 'gaming'],
        [3, 2003, '55', 'gaming'],
        [4, 2004, '55', 'gaming'],
        [5, 2005, '55', 'gaming'],
        [6, 2006, '55', 'gaming'],
        [7, 2007, '55', 'gaming'],
    ]
)

result = sm.writeBlock(overwriterer)
print("RESULT: ",result)