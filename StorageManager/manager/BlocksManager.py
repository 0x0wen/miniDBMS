import struct
import os
from StorageManager.manager.SchemaManager import SchemaManager

class BlocksManager(SchemaManager):
    def __init__(self, path_name='Storage/', block_size=720):
        super().__init__(path_name)
        self.block_size = block_size

    def readBlockIndex(self, table_name: str, block_index : int):
        """
        Read data from a specific block based on the block index and schema.
        Args : 
            table_name (str): the table name 
        Returns:
            list[]
        """
        if not table_name:
            raise ValueError("File name cannot be empty")
        print(table_name)
        schema = self.readSchema(table_name)
        row_size = sum(size for _, _, size in schema)
        data_path = table_name + "_data.dat"
        full_path = os.path.join(self.path_name, data_path)
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Data file not found: {full_path}")

        blocks = self.readBlocks(table_name)
        offset, num_rows = blocks[block_index]
        print(f"Membaca blok ke-{block_index + 1}, Offset: {offset}, Jumlah baris: {num_rows}")

        block_data = []
        try:
            with open(full_path, 'rb') as data_file:
                data_file.seek(offset)

                for _ in range(num_rows):
                    row = data_file.read(row_size)
                    
                    if(len(row) < row_size):
                        raise ValueError(f"Data row yang dibaca lebih pendek dari {row_size} bytes!")
                    
                    row_data = self._parseRowData(row, schema, row_size)
                    block_data.append(row_data)
            return block_data

        except Exception as e:
                raise IOError(f"Unexpected error reading blocks file:  {e}")



    def readBlocks(self, table_name: str) -> list[tuple[int, int]]:
        """
        Read block metadata from blocks file
        Args:
            table_name (str): The table name
        Returns:
            list[tuple[int, int]]: A list of block metadata (offset, num_rows)
        Raises:
            FileNotFoundError: If the blocks file does not exist
            IOError: If there's an issue reading the file
            ValueError: If the file format is invalid
        """
        if not table_name:
            raise ValueError("File name cannot be empty")

        blocks_file_name = table_name + '_blocks.dat'  
        full_path = os.path.join(self.path_name, blocks_file_name)

        # Check if file exist
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Blocks file not found: {full_path}")
        
        try:
            blocks = []
            with open(full_path, "rb") as blocks_file:
                num_blocks = struct.unpack('i', blocks_file.read(4))[0]
                if(num_blocks < 0):
                    raise ValueError(f"Invalid number of blocks: {num_blocks}")
                for _ in range(num_blocks):
                    offset, num_rows = struct.unpack('ii', blocks_file.read(8))
                    blocks.append((offset, num_rows))
            return blocks
        except Exception as e:
            raise IOError(f"Unexpected error reading blocks file : {e}")

    def _parseRowData(self,row: bytes, schema: list, row_size: int) -> list:
        """
        Parse a single row of binary data based on the given schema.
        """
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

            else:
                raise ValueError(f"Unsupported data type: {data_type}")
        
            row_data.append(value)
    
        return row_data 
    