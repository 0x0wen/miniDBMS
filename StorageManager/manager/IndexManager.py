from StorageManager.index.HashIndex import HashIndex
from StorageManager.manager.DataManager import DataManager
import os
import struct
class IndexManager(DataManager):
    def __init__(self, path_name = 'Storage/') -> None:
        self.path_name = path_name
    
    def readIndex(self, table_name : str, column : str) -> HashIndex:
        """
        Read the index for specific table name and column Returning an instance of HashIndex

        Args:
            table_name : name of the table
            column : name of the column
        
        Returns:
            HashIndex : a hash list of (key, block_id) 
        """
        index_filename = table_name + "_index.dat"
        hash_group = HashIndex(column)

        full_path = self.path_name  + index_filename
        if not os.path.exists(full_path) or not self.isIndexed(table_name, column):
            return None  # Return None if index is not available
        
        if not self.isIndexed(table_name, column):
            return None

        with open(full_path, 'rb') as index_file:
            while True:
                column_length_data = index_file.read(4)
                if not column_length_data:
                    break
                column_length = struct.unpack('i', column_length_data)[0]

                # Read column name
                column_name_data = index_file.read(column_length).decode()

                # Read Key Length
                key_length_data = index_file.read(4)
                key_length = struct.unpack('i', key_length_data)[0]

                # Read key Data
                key_data = index_file.read(key_length).decode()

                # Read block ID
                block_id_data = index_file.read(4)
                block_id = struct.unpack('i', block_id_data)[0]

                if(column_name_data == column):
                    hash_group.insert(key_data, block_id)
                
        return hash_group

    def writeIndex(self, table_name : str, column : str):
        """
        Write an index on a table_name_index.dat

        Note :
            This Function will overwrite the whole file
            Todo: Make a way that it only overwrite the index with same 
        
        """
        index_filename = table_name + "_index.dat"
        full_path = self.path_name  + index_filename
        
        if self.isIndexed(table_name, column):
            print(f"Warning : Already defined index")

        block_list = self.readBlockList(table_name)

        column_idx = self.getColumnIndexByTable(column,table_name)

        # Convert Pair of bloc_id and list blocks into a Hash Index
        hash_index : HashIndex = HashIndex.fromBlocks(column,column_idx,block_list)

        with open(full_path, 'wb') as index_file:
            for key, block_id in hash_index:
                # Write Column name
                column_name_data = column.encode()
                index_file.write(struct.pack('i', len(column_name_data)))
                index_file.write(column_name_data)

                # Write key
                key_data = key.encode()
                index_file.write(struct.pack('i', len(key_data)))
                index_file.write(key_data)

                # Write block ID
                index_file.write(struct.pack('i', block_id))

    def isIndexed(self, table_name: str,column : str):
        """Check if the specified column is indexed in the table"""
        index_filename = table_name + "_index.dat"
        full_path = self.path_name + index_filename

        # Check is index file already definie
        if not os.path.exists(full_path):
            return False
        
        # Check if the current column in the index file is already define or not
        with open(full_path, 'rb') as index_file:
            while True:
                column_length_data = index_file.read(4)
                if not column_length_data:
                    return False

                column_length = struct.unpack('i', column_length_data)[0]

                # Read column name
                column_name_data = index_file.read(column_length)
                try:
                    column_name = column_name_data.decode('utf-8')
                except UnicodeDecodeError:
                    continue

                # Check if column matches
                if column == column_name_data.decode('utf-8'):
                    # print(f"Match! antara {column} dengan {column_name_data}")
                    return True
                                
        # No matching column is found
        return False
    def deleteIndex(self, table_name):
        """
        Delete a already defined index, raise error if try delete a not defined index
        """
        index_filename  = table_name + "_index.dat"
        full_path = self.path_name + index_filename
        
        if not os.path.exists(full_path):
            raise ValueError("Index is not given")
        os.remove(full_path)
        