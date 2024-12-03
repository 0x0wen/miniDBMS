from Enum.ActionEnum import ActionEnum


class Action:
    def __init__(self, action: [str]):
        self.action: [ActionEnum] = []
        for a in action:
            if a not in ["read", "write"]:
                raise ValueError(f"Invalid action: {a}")
            else:
                self.action.append(ActionEnum.READ if a == "read" else ActionEnum.WRITE)

    def __str__(self):
        response = f"Action: "
        for action in self.action:
            response += f"{action.name} "
        return response

    def __eq__(self, other):
        return self.action == other.action

    def __call__(self, *args, **kwargs):
        return self.action
