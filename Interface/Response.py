class Response:
    def __init__(self, status: bool, message: str):
        self.allowed = status
        self.message = message

    def __str__(self):
        return f"{self.allowed}: {self.message}"

    def __call__(self):
        return self.allowed, self.message
