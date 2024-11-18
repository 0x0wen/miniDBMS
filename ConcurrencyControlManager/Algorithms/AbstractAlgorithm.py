from abc import ABC, abstractmethod
from Interface import Action
from Interface.Response import Response


class AbstractAlgorithm(ABC):
    @abstractmethod
    def run(self, object: int, transaction_id: int):
        raise NotImplementedError

    @abstractmethod
    def validate(self, object: int, transaction_id: int, action: Action) -> Response:
        raise NotImplementedError

    @abstractmethod
    def end(self, transaction_id: int) -> bool:
        raise NotImplementedError