class CustomException(Exception):
    """
    A base class for custom application errors.
    """
    def __init__(self, message: str, code: int = None, context: dict = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.context = context or {}

    def __str__(self):
        """
        Customize the string representation of the exception.
        """
        base_message = f"[Error {self.code}] {self.message}" if self.code else self.message
        if self.context:
            context_info = ", ".join(f"{key}={value}" for key, value in self.context.items())
            return f"{base_message} | Context: {context_info}"
        return base_message

    def to_dict(self):
        """
        Return a dictionary representation of the error.
        """
        return {
            "message": self.message,
            "code": self.code,
            "context": self.context,
        }


# Example usage
# try:
#     raise CustomError(
#         message="Something went wrong.",
#         code=500,
#         context={"operation": "SELECT * FROM non_existing_table"}
#     )
# except CustomError as e:
#     print(e)              # Human-readable error message
#     print(e.to_dict())    # Dictionary representation for logs or APIs