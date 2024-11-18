import struct
import os

class Serializer:
    def __init__(self, path_name='Storage/'):
        self.path_name = path_name

    def serializeRow(self, row, schema):
        """ Serialize a single row based on the schema. """
        row_binary = b''
        for value, (_, data_type, size) in zip(row, schema):
            if data_type == 'int':
                row_binary += struct.pack('i', value)
            elif data_type == 'float':
                row_binary += struct.pack('f', value)
            elif data_type == 'char':
                row_binary += struct.pack(f'{size}s', value.encode())
            elif data_type == 'varchar':
                row_binary += struct.pack(f'{size}s', value.encode().ljust(size, b'\x00'))
        return row_binary

    def writeTable(self, file_name, data, schema):
        """ Write data and schema to separate files. format : table_data dan tabel_scheme """
        schema_file_name = file_name + '_scheme'
        with open(self.path_name + schema_file_name, 'wb') as schema_file:
            schema_file.write(struct.pack('i', len(schema)))  
            for column_name, data_type, size in schema:
                # tulis metadata: name, data type, size
                column_name_bytes = column_name.encode('utf-8')
                schema_file.write(struct.pack('i', len(column_name_bytes)))  
                schema_file.write(column_name_bytes)  
                schema_file.write(struct.pack('i', len(data_type)))  
                schema_file.write(data_type.encode('utf-8'))  
                schema_file.write(struct.pack('i', size)) 

        data_file_name = file_name + '_data'
        with open(self.path_name + data_file_name, 'wb') as data_file:
            for row in data:
                data_file.write(self.serializeRow(row, schema))

    def readSchema(self, file_name):
        """ Read schema from the file. """
        schema = []
        schema_file_name = file_name + '_scheme'
        with open(self.path_name + schema_file_name, 'rb') as schema_file:
            num_columns = struct.unpack('i', schema_file.read(4))[0]
            for _ in range(num_columns):
                col_name_len = struct.unpack('i', schema_file.read(4))[0]
                column_name = schema_file.read(col_name_len).decode('utf-8')
                data_type_len = struct.unpack('i', schema_file.read(4))[0]
                data_type = schema_file.read(data_type_len).decode('utf-8')
                size = struct.unpack('i', schema_file.read(4))[0]
                schema.append((column_name, data_type, size))
        return schema

    def readData(self, file_name, schema):
        """ Read data based on the schema. """
        data = []
        row_size = sum(size for _, _, size in schema)  
        data_file_name = file_name + '_data'
        with open(self.path_name + data_file_name, 'rb') as data_file:
            while row := data_file.read(row_size):
                row_data = []
                offset = 0
                for _, data_type, size in schema:
                    if data_type == 'int':
                        value = struct.unpack_from('i', row, offset)[0]
                        offset += 4
                    elif data_type == 'float':
                        value = struct.unpack_from('f', row, offset)[0]
                        offset += 4
                    elif data_type == 'char' or data_type == 'varchar':
                        value = struct.unpack_from(f'{size}s', row, offset)[0].decode('utf-8').strip('\x00')
                        offset += size
                    row_data.append(value)
                data.append(row_data)
        return data

    def readTable(self, file_name):
        """ Read both schema and data from files. """
        schema = self.readSchema(file_name) 
        data = self.readData(file_name, schema)  
        column_names = [column_name for column_name, _, _ in schema]
        data_with_schema = [
            {column_name: value for column_name, value in zip(column_names, row)}
            for row in data
        ]
        return data_with_schema




    #pake 'Condition' kalo gk eror, python aneh
    def applyConditions(self,rows: list[dict], conditions: list['Condition']) -> list[dict]:
        def satisfies(row: dict, condition: 'Condition') -> bool:
            value = row.get(condition.column)
            if value is None:
                return False
            if condition.operation == "=":
                return value == condition.operand
            elif condition.operation == "<>":
                return value != condition.operand
            elif condition.operation == ">":
                return value > condition.operand
            elif condition.operation == "<":
                return value < condition.operand
            elif condition.operation == ">=":
                return value >= condition.operand
            elif condition.operation == "<=":
                return value <= condition.operand
            return False
        
        filtered_rows = []
        for row in rows:
            if all(satisfies(row, cond) for cond in conditions):
                filtered_rows.append(row)
        return filtered_rows


    def filterColumns(self,rows: list[dict], columns: list[str]) -> list[dict]:
        if columns:
            return [{col: row[col] for col in columns if col in row} for row in rows]
        return rows