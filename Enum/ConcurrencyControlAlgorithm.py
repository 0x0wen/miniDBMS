from enum import Enum


class ConcurrencyControlAlgorithm(Enum):
    LOCK = 1
    TIMESTAMP = 2
    MVCC = 3
