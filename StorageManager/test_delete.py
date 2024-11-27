from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.Condition import Condition
from StorageManager.StorageManager import StorageManager

cond1 = Condition("courseid", '=', 30)


deleted = DataDeletion(
    table="course",
    conditions = [cond1]
)

sm = StorageManager()
print(sm.deleteBlock(deleted))