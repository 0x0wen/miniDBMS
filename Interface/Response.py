class Response:
    def __init__(self, status: bool, message: int):
        self.allowed = status
        self.transaction_id = message

    def __str__(self):
        return f"{self.allowed}: {self.transaction_id}"

    def __call__(self):
        return self.allowed, self.transaction_id
