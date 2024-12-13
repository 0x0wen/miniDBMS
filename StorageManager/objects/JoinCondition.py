from typing import List, Union

class JoinCondition:
    def __init__(self, join_type: str, condition: Union[List[str], None] = None) -> None:
        """
        Menangani berbagai jenis join: CROSS JOIN, NATURAL JOIN, JOIN ON
        :param join_type: Tipe join ('CROSS', 'NATURAL', 'ON')
        :param condition: Kondisi untuk 'JOIN ON' (misalnya, ["a.id", "=", "b.id"])
        """
        self.join_type = join_type
        self.condition = condition if condition else []

    def __repr__(self) -> str:
        if self.join_type == "CROSS":
            return "CROSS JOIN"
        elif self.join_type == "NATURAL":
            return "NATURAL JOIN"
        elif self.join_type == "ON" and self.condition:
            condition_str = " AND ".join(self.condition)
            return f"JOIN ON {condition_str}"
        return f"JOIN {self.join_type}"
