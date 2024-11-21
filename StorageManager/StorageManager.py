from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Statistics import Statistics
# from Serializer import *
from StorageManager.SerializerBlock import Serializer
from StorageManager.objects.Rows import Rows  

class StorageManager:
    
    def __init__(self) -> None:
        pass

    
    def readBlock(self, data_retrieval : DataRetrieval) -> Rows:
        """
        Returns Rows of data from harddisk
        
        Args:
            data_retrieval : objects contains data to help determine which data to be retrieved from hard disk
        
        """
        #ini masih baca semua block
        #ini masih baca semua block
        serializer = Serializer()
        all_filtered_data : Rows = []
        for table_name in data_retrieval.table: #harusnya bisa join tabel karena list[str] ????
            data = serializer.readTable(table_name)
            cond_filtered_data = serializer.applyConditions(data,data_retrieval.conditions)
            column_filtered_data  = serializer.filterColumns(cond_filtered_data,data_retrieval.column)
            all_filtered_data.extend(column_filtered_data)
        print(all_filtered_data)
        print("Amount : ", all_filtered_data.__len__())
        return all_filtered_data


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

        serializer = Serializer()

        # Filtereed table based on condition
        data : Rows = serializer.readTable(data_deletion.table)
        filtered_Table = serializer.applyConditions(data, data_deletion.conditions) 

        schema = serializer.readSchema(data_deletion.table)

        # Create new data that doesn't contain filtered table
        newData = []
        rows_set = filtered_Table.to_set()
        for row in data:
            if frozenset(row.items()) not in rows_set:
                newData.append(list(row.values()))
        
        print(newData)
        serializer.writeTable(data_deletion.table, newData ,schema)
        return newData.__len__()

    def setIndex(self, table : str, column : str, index_type : str) -> None:
        """
        Handle creation of index in a given table

        Args : 
            table : the table to be given index
            column : certain column to be given index
            index_type: type of index (B+ Tree or Hash)
        """
        
        

    def getStats() -> Statistics:
        """
        Returns Statistics object that has number of tuples, number of blocks, size of tuple, blocking factor, and number of distinct values appear in r
        """
        pass

    

#SELECT umur,desk FROM user2 WHERE id <= 7 AND harga > 60.00
#ini masih baca semua block
