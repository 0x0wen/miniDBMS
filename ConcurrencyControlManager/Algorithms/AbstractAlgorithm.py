from abc import ABC, abstractmethod
from Interface.Action import Action
from Interface.Rows import Rows
from Interface.Response import Response


class AbstractAlgorithm(ABC):
    @abstractmethod
    def logObject(self, db_object: Rows, transaction_id: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        raise NotImplementedError

    @abstractmethod
    def end(self, transaction_id: int) -> bool:
        raise NotImplementedError