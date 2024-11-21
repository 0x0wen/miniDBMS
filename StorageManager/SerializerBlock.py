import struct
from objects.Condition import Condition
import os
from typing import Union
from objects.Condition import Condition
from objects.Rows import Rows
"""
Format file yang dibuat 
{tabel}_blocks.dat --metadata block
{tabel}_data.dat --data
{tabel}_scheme.dat --metadata info atribut

"""
class Serializer:
    def __init__(self, path_name='Storage/', block_size=720):
        self.path_name = path_name
        self.block_size = block_size 

    def serializeRow(self, row, schema):
        print(row)
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
    

    def overwriteData(self, file_name, data) -> int:
        
        return 0

    def appendData(self, file_name, data) -> int:
        schema = self.readSchema(file_name)
        data_file_name = file_name + '_data.dat'
        blocks_file_name = file_name + '_blocks.dat'  
        blocks = []
        with open(self.path_name + data_file_name, 'rb+') as data_file, open(self.path_name + blocks_file_name, 'rb+') as blocks_file:
            blocks_file.seek(0)
            num_existing_blocks = struct.unpack('i', blocks_file.read(4))[0]

            existing_blocks = []
            for _ in range(num_existing_blocks):
                offset, num_rows = struct.unpack('ii', blocks_file.read(8))
                existing_blocks.append((offset, num_rows))

            if existing_blocks:
                last_block_offset, rows_in_last_block = existing_blocks[-1]
                data_file.seek(last_block_offset)
                last_block_data = data_file.read(self.block_size)
                used_space_in_last_block = len(last_block_data)
            else:
                last_block_offset = 0
                rows_in_last_block = 0
                used_space_in_last_block = 0

            remaining_space = self.block_size - used_space_in_last_block
            current_offset = last_block_offset + used_space_in_last_block

            block_data = []
            num_rows = 0
            new_blocks = []

            for i, row in enumerate(data):
                serialized_row = self.serializeRow(row, schema)
                row_size = len(serialized_row)

                print(row_size)
                print(remaining_space)
                if row_size <= remaining_space:
                    data_file.seek(last_block_offset + used_space_in_last_block)
                    data_file.write(serialized_row)
                    rows_in_last_block += 1
                    used_space_in_last_block += row_size
                    remaining_space -= row_size
                else:
                    if block_data:
                        for block in block_data:
                            data_file.seek(current_offset)
                            data_file.write(block)
                            current_offset += len(block)
                        new_blocks.append((current_offset - len(block_data[0]), num_rows))
                        block_data = []
                        num_rows = 0

                    block_data.append(serialized_row)
                    num_rows += 1

                    if sum(len(block) for block in block_data) >= self.block_size or i == len(data) - 1:
                        for block in block_data:
                            data_file.seek(current_offset)
                            data_file.write(block)
                            current_offset += len(block)
                        new_blocks.append((current_offset - len(block_data[0]), num_rows))
                        block_data = []
                        num_rows = 0
                        remaining_space = self.block_size

            updated_blocks = existing_blocks[:-1] + [(last_block_offset, rows_in_last_block)] + new_blocks
            blocks_file.seek(0)
            blocks_file.write(struct.pack('i', len(updated_blocks)))
            for offset, num_rows in updated_blocks:
                blocks_file.write(struct.pack('ii', offset, num_rows))

        return rows_in_last_block + num_rows
    
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
            remaining_space = self.block_size
            block_data = []
            num_rows = 0

            for i, row in enumerate(data):
                block_data.append(self.serializeRow(row, schema))
                remaining_space-=row_size
                num_rows += 1

                if row_size > remaining_space  or i == len(data) - 1:
                    for block in block_data:
                        data_file.write(block)
                    blocks.append((current_offset, num_rows))
                    current_offset += sum(len(block) for block in block_data)
                    remaining_space = self.block_size
                    block_data = []  # Reset blok
                    num_rows = 0

            blocks_file.write(struct.pack('i', len(blocks)))  
            for offset, num_rows in blocks:
                blocks_file.write(struct.pack('ii', offset, num_rows))

    def readSchema(self, file_name : str) -> list[tuple]:
        """
        Reads the schema from binary file named file_name.dat and returns a list of tuples representing the column schema

        Args : 
            file_name (str) : the table name (without .dat suffix) to read the schema

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
        schema_file_name = file_name + '_scheme.dat'
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

    def readData(self, file_name : str, schema : list[tuple]) -> list[list]:
        """
        Reads data from a binary file based on the provided schema (which can be obtained from the readSchema function),
        and returns a list of tuples representing all of the values of the columns in the table.

        Args: 
            file_name (str): The base name of the table file (without the '_data.dat' suffix) to read the data from.
            schema (list[tuple]): A list of tuples, each representing a column in the schema. 
                                Each tuple contains the column name, its data type (e.g., 'int', 'char', 'float'), 
                                and the size (in bytes) of the column value. 
                                Example schema:
                                [
                                    ('id', 'int', 4),
                                    ('umur', 'char', 32),
                                    ('harga', 'float', 4),
                                    ('desk', 'char', 32),
                                    ...
                                ]

        Returns:
            Rows : a List of dictionary containing the value of each data
            
            For example, given a schema:
            [
                ('id', 'int', 4),
                ('name', 'char', 32),
                ('salary', 'float', 4),
                ('description', 'char', 64)
            ]
            
            The returned data might look like:
            [
                {1, 'John', 1200.50, 'Some description'},
                {2, 'Jane', 1500.75, 'Another description'},
                ...
            ]
            
            Where:
                - The first element in each row is an `int` (e.g., `1`).
                - The second element is a `str` (e.g., `'John'`).
                - The third element is a `float` (e.g., `1200.50`).
                - The fourth element is a `str` (e.g., `'Some description'`).

        Example:
            schema = readSchema('my_table')
            data = readData('my_table', schema)
            # data will be a list of rows, with each row being a list of column values:
            # [
            #     [1, 'John', 1200.50, 'Some description'],
            #     [2, 'Jane', 1500.75, 'Another description'],
            #     ...
            # ]
        """
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

    def readTable(self, file_name) -> Rows:
        """ Read both schema and data from files using block allocation. """
        schema = self.readSchema(file_name) 
        data = self.readData(file_name, schema)  
        column_names = [column_name for column_name, _, _ in schema]
        data_with_schema: Rows = Rows([
            {column_name: value for column_name, value in zip(column_names, row)}
            for row in data
        ])
        return data_with_schema

    #pake 'Condition' kalo gk eror, python aneh
    def applyConditions(self,rows: list[dict], conditions: list[Condition]) -> Rows:
        """
        Return the D
        """
        def satisfies(row: dict, condition: Condition) -> bool:
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
        return Rows(filtered_rows)

    def filterColumns(self,rows: list[dict], columns: list[str]) -> list[dict]:
        if columns:
            return [{col: row[col] for col in columns if col in row} for row in rows]
        return rows