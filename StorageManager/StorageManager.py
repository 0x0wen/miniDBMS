from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Statistics import Statistics
# from Serializer import *
from SerializerBlock import Serializer
from objects.Rows import Rows  
import sys
import os
current_file_dir = os.path.dirname(os.path.abspath(__file__))
folder_saya_path = os.path.join(current_file_dir, "../QueryOptimizer")
sys.path.append(folder_saya_path)

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
        # TODO: Put this change query_tree outside of fucntion
        # data_retrieval = self.__query_tree_to_data_retrieval(query_tree)
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
        serializer = Serializer()
        table_name = data_write.selected_table
        new_data = data_write.new_value

        if (data_write.overwrite):
            print('overwrite')
        else:
            return serializer.appendData(table_name, new_data)
        
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
        
        

    def getStats(self) -> dict:
        """
        Return dictionary of statistics for all tables in the database
        """
        
        current_dir = os.path.dirname(os.path.abspath(__file__)) 
        storage_dir = os.path.join(current_dir, "../Storage") 
        storage_dir = os.path.abspath(storage_dir)  

        all_stats = {}

        # Check all tables in the directory
        for file_name in os.listdir(storage_dir):
            if file_name.endswith("_scheme.dat"):
                table_name = file_name.replace("_scheme.dat", "")
                try:
                    stats = self.getStatsOneTable(table_name)
                    all_stats[table_name] = stats
                except Exception as e:
                    print(f"Error saat membaca statistik untuk tabel {table_name}: {e}")
        
        return all_stats

    
    def getStatsOneTable(self,table_name) -> Statistics:
        """
        Returns Statistics object that has number of tuples, number of blocks, size of tuple, blocking factor, and number of distinct values appear in r
        """
        serializer = Serializer()
        
        schema = serializer.readSchema(table_name)
        
        l_r = sum(size for _, _, size in schema)  
        
        # Ambil metadata blok
        blocks = serializer.readBlocks(table_name)
        n_r = sum(num_rows for _, num_rows in blocks)  
        b_r = len(blocks)  
        
        # Hitung faktor blocking (f_r)
        block_size = serializer.block_size
        f_r = block_size // l_r  
        
        # Hitung jumlah nilai unik (V) untuk setiap kolom
        data = serializer.readData(table_name, schema)
        column_names = [col[0] for col in schema]
        col_indices = {col_name: idx for idx, col_name in enumerate(column_names)}  # Mapping nama kolom ke indeks
        V_a_r = {
            col: len(set(row[col_indices[col]] for row in data)) 
            for col in column_names
        }
        
        # Buat dan return objek Statistik
        return Statistics(n_r=n_r, b_r=b_r, l_r=l_r, f_r=f_r, V_a_r=V_a_r)
    
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
