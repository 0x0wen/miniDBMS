from objects.DataRetrieval import DataRetrieval,Condition
from objects.DataWrite import DataWrite
from objects.DataDeletion import DataDeletion
from objects.Statistics import Statistics
# from Serializer import *
from SerializerBlock import Serializer
from objects.Rows import Rows  
from objects.QueryTree import QueryTree

class StorageManager:
    
    def __init__(self) -> None:
        pass

    
    def readBlock(self, query_tree) -> Rows:
        """
        Returns Rows of data from harddisk
        
        Args:
            data_retrieval : objects contains data to help determine which data to be retrieved from hard disk
        
        """
        #ini masih baca semua block
        #ini masih baca semua block
        serializer = Serializer()
        all_filtered_data = []
        data_retrieval = self.__query_tree_to_data_retrieval(query_tree)
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
    
    def __query_tree_to_data_retrieval(self,query_tree):
        tables = []
        columns = []
        conditions = []

        stack = [query_tree] 
        while stack:
            node = stack.pop()

            # Proses node berdasarkan tipe
            if node.node_type == "FROM":
                tables.extend(node.val)  # Ambil tabel
            elif node.node_type == "SELECT":
                if(node.val[0] == '*'):
                    columns.extend([])
                else:
                    columns.extend(node.val)  # Ambil kolom
            elif node.node_type == "WHERE":
                if isinstance(node.val[0], list):  # AND (array terpisah)
                    for idx, condition in enumerate(node.val):
                        column, operation, operand = condition
                        # Kondisi terakhir mendapatkan connector=None
                        connector = "AND" if idx < len(node.val) - 1 else None
                        conditions.append(Condition(column, operation, operand, connector=connector))
                else:  # OR (linear array)
                    i = 0
                    while i < len(node.val):
                        if node.val[i] in ["AND", "OR"]:
                            i += 1  # Abaikan operator, digunakan untuk kondisi berikutnya
                            continue
                        # Ambil kondisi dalam format [kolom, operasi, nilai]
                        column, operation, operand = node.val[i:i+3]
                        # Default connector None untuk kondisi tunggal, atau gunakan OR/AND untuk lainnya
                        connector = None if i + 3 >= len(node.val) else node.val[i + 3]
                        conditions.append(Condition(column, operation, operand, connector=connector))
                        i += 3  # Lompat ke kondisi berikutnya

            # Tambahkan anak ke stack
            stack.extend(node.children)

        # Buat DataRetrieval
        return DataRetrieval(tables, columns, conditions)

    

#SELECT umur,desk FROM user2 WHERE id <= 7 AND harga > 60.00
#ini masih baca semua block
# cond1 = Condition("id", '<=', 7)
# cond2 = Condition("harga", '>', 60.00)

# retrieval = DataRetrieval(
#     table=["user2"],
#     column=[],
#     conditions=[cond1]
# )

# sm = StorageManager()
# sm.readBlock(retrieval)

# QueryTree 
root =  QueryTree("SELECT", ["*"])  # Node root untuk FROM
from_node = QueryTree("FROM", ["user2"], parent=root)  # Node untuk SELECT

where_node = QueryTree("WHERE", ["id", "<=", 7], parent=root)  # 1 kondisi

# ini format and sama or kel owen jadi kalo mau coba coba formatnya gini ya
# where_node = QueryTree("WHERE", ['id', '<=', 7, 'OR', 'harga', '>', 60.0], parent=root) # or
# where_node = QueryTree("WHERE", [['id', '<=', 7], ['harga', '>', 60.0]], parent=root) # and

# Menghubungkan children ke root

root.children = [from_node, where_node]
# data_retrieval = query_tree_to_data_retrieval(root)
# print(data_retrieval)


sm = StorageManager()
sm.readBlock(root)