import struct
import os
from objects.Condition import Condition

"""
Format file yang dibuat 
{tabel}_blocks.dat --metadata block
{tabel}_data.dat --data
{tabel}_scheme.dat --metadata info atribut

"""
class Serializer:
    def __init__(self, path_name='Storage/', block_size=256):
        self.path_name = path_name
        self.block_size = block_size 

    def serializeRow(self, row, schema):
        """ Serialize a single row based on the schema. """
        row_binary = b''
        for value, (_, data_type, size) in zip(row, schema):
            if data_type == 'int':
                row_binary += struct.pack('i', value)
            elif data_type == 'float':
                row_binary += struct.pack('f', value)
            elif data_type == 'char':
                row_binary += struct.pack(f'{size}s', value.encode()[:size])
            elif data_type == 'varchar':
                row_binary += struct.pack(f'{size}s', value.encode().ljust(size, b'\x00')[:size])
        return row_binary


    
    def readBlocks(self, file_name):
        """Read block metadata from the separate blocks file."""
        blocks_file_name = file_name + '_blocks.dat'  
        blocks = []
        with open(self.path_name + blocks_file_name, 'rb') as blocks_file:
            num_blocks = struct.unpack('i', blocks_file.read(4))[0]
            for _ in range(num_blocks):
                offset, num_rows = struct.unpack('ii', blocks_file.read(8))
                blocks.append((offset, num_rows))
        return blocks


    def readBlock(self, file_name, block_index):
        """ Read data from a specific block based on the block index and schema. """
        schema = self.readSchema(file_name)
        row_size = sum(size for _, _, size in schema)
        data_file_name = file_name + '_data.dat'
        blocks = self.readBlocks(file_name)  
        if block_index < 0 or block_index >= len(blocks):
            raise IndexError(f"Block index {block_index} out of range.")
        
        offset, num_rows = blocks[block_index]
        print(f"Membaca blok ke-{block_index + 1}, Offset: {offset}, Jumlah baris: {num_rows}")

        data = []
        with open(self.path_name + data_file_name, 'rb') as data_file:
            data_file.seek(offset)
            
            for _ in range(num_rows):
                row = data_file.read(row_size)
                if len(row) < row_size:
                    print(f"error: data row yang dibaca lebih pendek dari {row_size} bytes!")
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
                        raw_value = struct.unpack_from(f'{size}s', row, offset)[0]
                        value = raw_value.split(b'\x00', 1)[0].decode('utf-8')  # Hanya bagian sebelum padding
                        offset += size
                    row_data.append(value)
                data.append(row_data)
        
        return data


    def writeTable(self, file_name, data, schema):
        """ Write data and schema to files with metadata blocks. """
        schema_file_name = file_name + '_scheme.dat'
        with open(self.path_name + schema_file_name, 'wb') as schema_file:
            schema_file.write(struct.pack('i', len(schema)))
            for column_name, data_type, size in schema:
                column_name_bytes = column_name.encode('utf-8')
                schema_file.write(struct.pack('i', len(column_name_bytes)))
                schema_file.write(column_name_bytes)
                schema_file.write(struct.pack('i', len(data_type)))
                schema_file.write(data_type.encode('utf-8'))
                schema_file.write(struct.pack('i', size))

        data_file_name = file_name + '_data.dat'
        blocks_file_name = file_name + '_blocks.dat'  
        blocks = []  # (offset, jumlah row)
        with open(self.path_name + data_file_name, 'wb') as data_file, open(self.path_name + blocks_file_name, 'wb') as blocks_file:
            current_offset = 0
            row_size = sum(size for _, _, size in schema)
            block_data = []
            num_rows = 0

            for i, row in enumerate(data):
                block_data.append(self.serializeRow(row, schema))
                num_rows += 1

                if sum(len(block) for block in block_data) >= self.block_size or i == len(data) - 1:
                    for block in block_data:
                        data_file.write(block)
                    blocks.append((current_offset, num_rows))
                    current_offset += sum(len(block) for block in block_data)
                    block_data = []  # Reset blok
                    num_rows = 0

            blocks_file.write(struct.pack('i', len(blocks)))  
            for offset, num_rows in blocks:
                blocks_file.write(struct.pack('ii', offset, num_rows))




    def readSchema(self, file_name):
        """ Read schema from the file. """
        schema = []
        schema_file_name = file_name + '_scheme.dat'
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
        """ Read data based on the schema, using block allocation from a separate blocks file. """
        data = []
        row_size = sum(size for _, _, size in schema)
        data_file_name = file_name + '_data.dat'
        blocks = self.readBlocks(file_name) 
        print("Blocks metadata read:", blocks)  
        
        with open(self.path_name + data_file_name, 'rb') as data_file:
            for block_index, (offset, num_rows) in enumerate(blocks):
                print(f"Membaca blok ke-{block_index + 1}, Offset: {offset}, Jumlah baris: {num_rows}")
                data_file.seek(offset)  
                for _ in range(num_rows):
                    row = data_file.read(row_size)
                    if len(row) < row_size:
                        print(f"Warning: Data row yang dibaca lebih pendek dari {row_size} bytes!")
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
                            raw_value = struct.unpack_from(f'{size}s', row, offset)[0]
                            value = raw_value.split(b'\x00', 1)[0].decode('utf-8')  # Hanya bagian sebelum padding
                            offset += size
                        row_data.append(value)
                    data.append(row_data)
        
        return data





    def readTable(self, file_name):
        """ Read both schema and data from files using block allocation. """
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