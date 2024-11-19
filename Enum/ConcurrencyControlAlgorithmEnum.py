from enum import Enum


class ConcurrencyControlAlgorithmEnum(Enum):
    LOCK = 1
    TIMESTAMP = 2
    MVCC = 3
