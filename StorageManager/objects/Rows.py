class Rows:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
    

    #print 
    def __repr__(self) -> str:
        return f"Rows(rows={self.rows})"