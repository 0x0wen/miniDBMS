from objects.DataDeletion import DataDeletion
from objects.DataRetrieval import DataRetrieval
from objects.Condition import Condition
from StorageManager import StorageManager

cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)

retrieval = DataRetrieval(
    table=["user2"],
    conditions=[]
)

deleted = DataDeletion(
    table="user2",
    conditions = [cond1]
)

sm = StorageManager()
sm.readBlock(retrieval)
print()
print(sm.deleteBlock(deleted))