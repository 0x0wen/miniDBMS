from typing import List
from StorageManager.objects.JoinCondition import JoinCondition

class JoinOperation:
    def __init__(self, tables: List[str], join_condition: JoinCondition) -> None:
        """
        Mengelola operasi join antara tabel-tabel.
        :param tables: Daftar tabel yang terlibat dalam join
        :param join_condition: Objek JoinCondition yang menentukan jenis join
        """
        self.tables = tables
        self.join_condition = join_condition

    def __repr__(self) -> str:
        tables_str = " JOIN ".join(self.tables)
        return f"{tables_str} {self.join_condition}"
