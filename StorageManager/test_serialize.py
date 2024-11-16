
from Serializer import Serializer

schema = [
    ('col1', 'int', 4),
    ('col2', 'varchar', 10),
    ('col3', 'float', 4),
    ('col4', 'char', 10),
]




data2 = [
    [5, 'data1', 12.34, 'row1'],
    [6, 'data2', 56.78, 'row2'],
    [7, 'data3', 90.12, 'row3'],
    [8, 'data4', 30000, 'row4'],

]
serializer = Serializer(schema)
table_name = "user2"
serializer.write_table(table_name, data2)

read_data = serializer.read_table(table_name)

print('Data:', read_data)
