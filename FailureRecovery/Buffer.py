from typing import Generic, TypeVar, List

T = TypeVar('T')

class Buffer(Generic[T]):
    """Generic buffer implementation for temporary data storage"""

    def __init__(self, size: int = 1024):
        """
        Initialize buffer with fixed size
        """
        if size <= 0:
            raise ValueError("Buffer size must be positive")
        self.size = size
        self.buffer: List[T] = [] # Actual storage
        self.current_size = 0  # Current number of entries

    def is_almost_full(self) -> bool:
        """Check if buffer is at 80% capacity"""
        return self.current_size >= (self.size * 0.8)

    def add(self, data: T) -> bool:
        """
        Add new entry to buffer
        """
        try:
            if self.current_size >= self.size:
                return False
            self.buffer.append(data)
            self.current_size += 1
            return True
        except Exception as e:
            print(f"Error adding to buffer: {e}")
            return False

    def flush(self) -> List[T]:
        """
        Empty buffer and return all entries
        """
        try:
            data = self.buffer.copy()
            self.buffer.clear()
            self.current_size = 0
            return data
        except Exception as e:
            print(f"Error flushing buffer: {e}")
            return []
        
    def __str__(self) -> str:
        return f"Buffer({self.current_size}/{self.size} entries)"