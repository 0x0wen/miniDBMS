class Response:
    def __init__(self, response_action: str, current_t_id: int, related_t_id: int):
        """
        Ex:
        Response(response_action="ALLOW", current_t_id=1, related_t_id=1) => transaction 1 is allowed to continue
        Response(response_action="WOUND", current_t_id=1, related_t_id=2) => transaction 1 is allowed, but transaction 2 is wounded
        Response(response_action="WAIT", current_t_id=2, related_t_id=1) => transaction 2 is waiting for transaction 1
        """
        self.response_action = response_action
        self.current_t_id = current_t_id
        self.related_t_id = related_t_id

    def __str__(self):
        return f"{self.response_action}, {self.current_t_id}, {self.related_t_id}"

    def __call__(self):
        return self.response_action, self.current_t_id, self.related_t_id
    
    def __eq__(self, other):
        if isinstance(other, Response):
            return (self.response_action == other.response_action) and (self.current_t_id == other.current_t_id) and (self.related_t_id == other.related_t_id)
        
        return False