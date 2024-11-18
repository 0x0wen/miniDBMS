from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm
from Interface import Action, Response

class TwoPhaseLock(AbstractAlgorithm):
    def run(self, object: int, transaction_id: int):
        pass

    def validate(self, object: int, transaction_id: int, action: Action) -> Response:
        pass
