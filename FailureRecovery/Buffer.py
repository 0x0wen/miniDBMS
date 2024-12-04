from typing import Generic, TypeVar, List, Dict
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.DataRetrieval import DataRetrieval

T = TypeVar('T')

class Buffer(Generic[T]):
    def __init__(self, size: int = 1024):
        self.size = size
        self.buffer: Dict[str, List[Dict]] = {}  
        self.current_size = 0
        self.storage_manager = StorageManager()
        self.modified_tables = set()

    def read_data(self, table: str) -> List[Dict]:
        """Read data from storage and cache in buffer"""
        if table not in self.buffer:
            data_retrieval = DataRetrieval([table])
            data = self.storage_manager.readBlock(data_retrieval)
            if self.current_size < self.size and data and data.data:
                self.buffer[table] = data.data
                self.current_size += len(data.data)
        return self.buffer.get(table, [])

    def write_data(self, table: str, data: Dict) -> bool:
        """Cache write operations in buffer"""
        if self.current_size >= self.size:
            return False
        if table not in self.buffer:
            self.buffer[table] = []
        self.buffer[table].append(data)
        self.current_size += 1
        self.modified_tables.add(table)
        return True

    def empty_buffer(self) -> None:
        """Empty buffer during checkpoint"""
        self.buffer.clear()
        self.current_size = 0
        self.modified_tables.clear()

    def get_modified_data(self) -> Dict[str, List[Dict]]:
        """Get all modified data by table"""
        return {table: self.buffer[table] for table in self.modified_tables}