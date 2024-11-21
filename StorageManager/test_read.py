from StorageManager import StorageManager
from objects.Condition import Condition
from objects.DataRetrieval import DataRetrieval
    

#SELECT umur,desk FROM user2 WHERE id <= 7 AND harga > 60.00
#ini masih baca semua block
cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)

sm = StorageManager()

retrieval_course = DataRetrieval(
    table=["course"],
    column=[],
    conditions=[]
)

result_course = sm.readBlock(retrieval_course)
print(result_course)