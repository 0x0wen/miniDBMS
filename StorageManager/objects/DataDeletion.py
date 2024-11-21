from StorageManager.objects.Condition import Condition
class DataDeletion:
    def __init__(self, table : str, conditions : list[Condition] = []) -> None:
        self.table = table
        self.conditions = conditions
    
    def __repr__(self) -> str:
        return(f"DataDeletion Object"
               f"Table = {self.table}"
               f"Conditions = {self.conditions}")
    