from Enum.ActionEnum import ActionEnum


class Action:
    def __init__(self, action: [str]):
        try:
            self.action: [ActionEnum] = []
            for a in action:
                if a not in ["read", "write", "commit"]:
                    raise ValueError(f"Invalid action: {a}")
                else:
                    self.action.append(ActionEnum[a.upper()])
        except KeyError:
            raise ValueError(f"Invalid action: {action}")

    def __str__(self):
        response = f"Action: "
        for action in self.action:
            response += f"{action.name} "
        return response

    def __eq__(self, other):
        return self.action == other.action

    def __call__(self, *args, **kwargs):
        return self.action
