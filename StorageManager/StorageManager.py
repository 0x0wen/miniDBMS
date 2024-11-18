from objects.DataRetrieval import DataRetrieval,Condition
from objects.DataWrite import DataWrite
from objects.DataDeletion import DataDeletion
from objects.Statistics import Statistics
from Serializer import *
from objects.Rows import Rows  

class StorageManager:
    
    def __init__(self) -> None:
        pass

    
    def readBlock(self, data_retrieval : DataRetrieval) -> Rows:
        """
        Returns Rows of data from harddisk
        
        Args:
            data_retrieval : objects contains data to help determine which data to be retrieved from hard disk
        
        """
        serializer = Serializer()
        all_filtered_data = []
        for table_name in data_retrieval.table: #harusnya bisa join tabel karena list[str] ????
            data = serializer.readTable(table_name)
            cond_filtered_data = serializer.applyConditions(data,data_retrieval.conditions)
            column_filtered_data  = serializer.filterColumns(cond_filtered_data,data_retrieval.column)
            all_filtered_data.extend(column_filtered_data)
        
        print(all_filtered_data) 
        return Rows(all_filtered_data)



    
        

    def writeBlock(self ,data_write: DataWrite) -> int:
        """
        Returns the number of affected rows
        
        Args:
            data_write : objects contains data to help determine which data to be retrieved from hard disk, contain modified data for modification operation, and new data for adddition operation
        
        """
        pass

    def deleteBlock(self, data_deletion : DataDeletion) -> int:
        """
        Returns the number of removed rows

        Args: 
            data_deletion : objects contains data to help storage manager determince which data to be deleted.
        """
        pass

    def setIndex(self, table : str, column : str, index_type : str) -> None:
        """
        Handle creation of index in a given table

        Args : 
            table : the table to be given index
            column : certain column to be given index
            index_type: type of index (B+ Tree or Hash)
        """
        pass

    def getStats() -> Statistics:
        """
        Returns Statistics object that has number of tuples, number of blocks, size of tuple, blocking factor, and number of distinct values appear in r
        """
        pass

    

#SELECT umur,desk FROM user2 WHERE id <= 7 AND harga > 60.00
cond1 = Condition("id", '<=', 7)
cond2 = Condition("harga", '>', 60.00)

retrieval = DataRetrieval(
    table=["user2"],
    column=["umur","desk"],
    conditions=[cond1,cond2]
)

sm = StorageManager()
sm.readBlock(retrieval)

