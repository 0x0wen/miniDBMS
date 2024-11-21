from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.StorageManager import StorageManager

cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)

retrieval = DataRetrieval(
    table=["user2"],
    column=[],
    conditions=[cond1]
)

sm = StorageManager()
sm.readBlock(retrieval)