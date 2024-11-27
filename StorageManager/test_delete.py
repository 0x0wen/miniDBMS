from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.Condition import Condition
from StorageManager.StorageManager import StorageManager

cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)


deleted = DataDeletion(
    table="user2",
    conditions = [cond1]
)

sm = StorageManager()
print(sm.deleteBlock(deleted))