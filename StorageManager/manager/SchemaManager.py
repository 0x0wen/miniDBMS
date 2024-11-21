import struct
from StorageManager.manager.Serializer import Serializer
class SchemaManager(Serializer):
    def __init__(self, path_name="Storage/") -> None:
        super().__init__(path_name)

    def readSchema(self, table_name: str) -> list[tuple]:
        """
        Reads the schema from binary file named file_name.dat and returns a list of tuples representing the column schema

        Args : 
            table_name (str) : the table name (without .dat suffix) to read the schema

        Returns:
            list[tuple] : A list of tuples, each representing a column in the schema
            Each tuple contains:
            - column_name (str) : The column name
            - data_type (str) : the data type of column (e.g., 'int', 'char', 'float')
            - size (int) : the size (or byte length) of the value
        Example : 
            schema = readSchema('my_table')
            #### schema will be a list of tuples like:
            #### [
            ####     ('id', 'int', 4),
            ####     ('umur', 'char', 32),
            ####     ('harga', 'float', 4),
            ####     ('desk', 'char', 32),
            ####     ...
            #### ]
        """
        schema_list = []
        if(not table_name):
            raise ValueError("Table name aren't set yet")
        
        schema_file_name = table_name + '_scheme.dat'
        with open(self.path_name + schema_file_name, 'rb') as schema_file:
            num_columns = struct.unpack('i', schema_file.read(4))[0]
            for _ in range(num_columns):
                col_name_len = struct.unpack('i', schema_file.read(4))[0]
                column_name = schema_file.read(col_name_len).decode('utf-8')
                data_type_len = struct.unpack('i', schema_file.read(4))[0]
                data_type = schema_file.read(data_type_len).decode('utf-8')
                size = struct.unpack('i', schema_file.read(4))[0]
                schema_list.append((column_name, data_type, size))
        return schema_list