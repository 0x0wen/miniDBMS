
class HashIndex:
    def __init__(self,column, bucket_amount = 16) -> None:
        self.bucket_amount = bucket_amount
        self.column = column
        self.hash_table : list[list[tuple[str,int]]] = [[] for _ in range(bucket_amount)]

    @classmethod
    def fromBlocks(cls, column : str, column_idx : int,pairs_of_blockID_blockList : list[tuple[int,list]]):
        """
        Create a HashIndex instance from a list of blocks.

        Args:
            column (str): The column name for the hash index.

            column_idx (int) : index of column on the schema

            pairs_of_blockID_blockList (list[tuple[int, list[str]]]): A list of tuples, 
                where each tuple contains a block ID and a list of keys associated with that block.

        Returns:
            HashIndex: An instance of HashIndex populated with the blocks
        """   
        instance = cls(column)
        for block_id, block_list in pairs_of_blockID_blockList:
            for block in block_list:
                key = str(block[column_idx])
                instance.insert(key, block_id)
        return instance

    def __hash__(self, key) -> int:
        """
        Computes the hash value for a given key.
        
        Args: 
            key: The key to hash.

        Returns:
            index in the hash table.
        """
        
        return hash(key) % self.bucket_amount
    
    def insert(self, key: str, block_id : int) -> None:
        """
        Insert pair of key, block_id into hash table

        Args: 
            key : key that will be hashed
            block_id : the block where the key can be found
        """
        index = self.__hash__(key)
        for item in self.hash_table[index]:
            if item[0] == key:
                item[1] = block_id
                return
        self.hash_table[index].append([key, block_id])
    
    def search(self, key: str) -> int:
        """
        Search by key and return the block id.

        Args: 
            key: The key of the item.
        
        Returns:
            block_id (int): block_id where the key is found.
            -1: Return -1 if no key is found.
        """
        # Pastikan tipe data key sama dengan yang ada di hash_table
        for bucket in self.hash_table:
            for item in bucket:
                try:
                    stored_key = item[0]
                    # Convert key to the same type as the stored key
                    converted_key = type(stored_key)(key)  
                    if stored_key == converted_key:
                        # Jika cocok, hash dan cek di indeks yang sesuai
                        index = self.__hash__(converted_key)
                        for pair in self.hash_table[index]:
                            if pair[0] == stored_key:
                                return pair[1]
                except (ValueError, TypeError):
                    continue

        # Key Not Found
        return -1


    def delete(self, key: str) -> bool:
        """
        Delete a key from the hash table
        """
        index = self.__hash__(key)
        for i, item in enumerate(self.hash_table[index]):
            if item[0] == key:
                del self.hash_table[index][i]
                return True
        return False
    
    def __len__(self):
        """
        Return the number of items in the hash table
        """
        return self.hash_table.__len__()
    
    def __iter__(self):
        """
        Iterate over the key and block id
        """
        for bucket in self.hash_table:
            for key, block_id in bucket:
                yield key, block_id

    def __repr__(self) -> str:
        """
        String representation of the hash table
        """
        items = []
        for key, block_id in self:

            items.append(f"{key}: {block_id}")

        return f"HashTable({self.column}): " + ", ".join(items)
    
    def test(self):
        return self.hash_table.__len__() > 0