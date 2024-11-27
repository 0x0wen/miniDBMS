class HashIndexer:
    def __init__(self, table_size = 10) -> None:
        self.table_size = table_size
        self.hash_table = [[] for _ in range(table_size)]

    def _hashFunction(self, key):
        """
        Computes the hash value for a given key.
        
        Args: 
            key: The key to hash.

        Returns:
            index in the hash table.
        """
        return hash(key) % self.table_size
    
    def insert(self, key, block_id) -> None:
        """
        Inserts a key-to-block mapping into the hash index.

        Args:
            key: the key to index by
            block_id: the block ID or reference associated with the key
        """
        index = self._hashFunction(key)
        for i, (key_item, _) in enumerate(self.hash_table[index]):
            if key_item == key:
                self.hash_table[index][i] = (key, block_id)
                return
        self.hash_table[index].append((key, block_id))
    
    def search(self, key) -> int:
        """
        Searches for a block ID in the hash index by key.
        
        Args: 
            key: The key to search for.

        Returns:
            The block ID if found, or None.
        """
        index = self._hashFunction(key)
        for i, (k, _) in enumerate(self.hash_table[index]):
            if k == key:
                del self.hash_table[index][i]
                return True
        return False

    def __repr__(self) -> str:
        """
        Displays the hash table for debugging purposes.
        """
        for i, bucket in enumerate(self.hash_table):
            print(f"Bucket {i}: {bucket}")