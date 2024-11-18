from Serializer import Serializer

schema = [
    ('id', 'int', 4),
    ('umur', 'varchar', 10),
    ('harga', 'float', 4),
    ('desk', 'char', 10),
]

data2 = [
    [5, 'datadasdadd1', 12.34, 'row1'],
    [6, 'data2', 56.78, 'row2'],
    [7, 'data3', 90.12, 'row3'],
    [8, 'data4', 30000, 'row4'],
]

serializer = Serializer()
table_name = "user2"
serializer.writeTable(table_name, data2,schema)
print(serializer._readSchema(table_name))
data_with_schema = serializer.readTable(table_name)
print(data_with_schema)

# baris1 = data_with_schema[0]
# idb1 = baris1['id']
# print(idb1)

