from StorageManager.manager.BlocksManager import BlocksManager
import struct
import os
class DataManager(BlocksManager):
    def __init__(self, path_name: str, block_size : int) -> None:
        super().__init__(path_name, block_size)
    
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
    
    def overwriteData(self, file_name, data) -> int:        
        return 0

    def readBlockByOffset(self, table_name : str , block_id : int, schema : list[tuple], offset : int, num_rows) -> tuple[list, int]:
        """
        Reads data from a specific block

        Args: 
            file_name (str): The base name of the table file.

            schema (list[tuple]): A list of tuples representing the schema

            block_index (int): The index of the block to read

            offset (int): The offset in the file where the block starts

            num_rows (int): The number of rows in the block

        Returns: 
            List of rows for the specific block and its block id
        """
        row_size = sum(size for _, _ , size in schema)
        block_data = []

        data_file_name = table_name + "_data.dat"
        fullpath = self.path_name + data_file_name

        if not os.path.exists(fullpath):
            raise ValueError(f"Table name doesn't exist {table_name}")
        
        with open(fullpath, 'rb') as data_file:
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

                    elif data_type in ['char', 'varchar']:
                        raw_value = struct.unpack_from(f'{size}s', row, offset)[0]
                        value = raw_value.split(b'\x00', 1)[0].decode('utf-8')
                        offset += size
                    row_data.append(value)
                block_data.append(row_data)
        return block_data, block_id

    def readData(self, table_name : str, schema : list[tuple]) -> list[list]:
        """
        Reads data from a binary file based on the provided schema (which can be obtained from the readSchema function),
        and returns a list of tuples representing all of the values of the columns in the table.

        Args: 
            file_name (str): The base name of the 
            
            table_name (str) : The name of the table that will be read

            schema (list[tuple]): A list of tuples, each representing a column in the schema. 
            Each tuple contains the column name, its data type (e.g., 'int', 'char', 'float'), 
            and the size (in bytes) of the column value. 

        Returns:
            Rows : a List of dictionary containing the value of each data
        
        """
        data = []
        blocks = self.readBlocks(table_name) 
        print("Blocks metadata read:", blocks)  
        
        try:
            for block_index, (offset, num_rows) in enumerate(blocks):
                print(f"Membaca blok ke-{block_index + 1}, Offset: {offset}, Jumlah baris: {num_rows}")

                block_data, _ = self.readBlockByOffset(table_name, block_index, schema, offset, num_rows)

                data.extend(block_data)
        except ValueError as e:
            print("Error : ", e)

        return data
    
    def readBlockList(self, table_name: str) -> list[tuple[int, list]]:
        """
        Retrieves a list of blocks for the specified table.

        Args:
            table_name (str): The name of the table to retrieve blocks from.

        Returns:
            list[tuple[int, list]]: A list of tuples where each tuple contains:
                - An integer representing the block ID.
                - A list of keys (or rows) associated with that block.
        """
        schema = self.readSchema(table_name)

        blocks = self.readBlocks(table_name) 

        blocks_file_name = table_name + '_blocks.dat'

        blocks_fullpath = self.path_name + blocks_file_name

        if not os.path.exists(blocks_fullpath):
            raise ValueError(f"Blocks file does not exist for table: {table_name}")
        
        final_data : list[tuple[int, list]]= []
        # Read the block data
        for block_index, (offset, num_rows) in enumerate(blocks):
            print(f"Membaca blok ke-{block_index + 1}, Offset: {offset}, Jumlah baris: {num_rows}")
            block_data, amount = self.readBlockByOffset(table_name, block_index, schema, offset, num_rows)
            final_data.append((amount, block_data))

        return final_data

