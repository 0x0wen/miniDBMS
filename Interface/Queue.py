from typing import Generic, TypeVar, List

T = TypeVar('T')

class Queue(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.queue_length = len(data)
    
    def isEmpty(self) -> bool:
        return self.queue_length == 0
    
    def head(self) -> T:
        if not (self.isEmpty()):
            return self.data[0]
        else:
            raise ValueError("Queue is empty")

    def enqueue(self, element: T) -> None:
        self.data.append(element)
        self.queue_length += 1
    
    def enqueueFront(self, element: T) -> None:
        self.data.insert(0, element)
        self.queue_length += 1
    
    def dequeue(self) -> T:
        if self.queue_length == 0:
            raise ValueError("Queue is empty")
        else:
            self.queue_length -= 1
            return self.data.pop(0)
    
    def printQueue(self) -> None:
        for i in range(len(self.data)):
            print(self.data[i])