from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Statistics import Statistics
from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager
# from Serializer import *
from StorageManager.objects.Rows import Rows  
import os

class StorageManager:

    def __init__(self) -> None:
        pass
    
    def readBlock(self, data_retrieval: DataRetrieval) -> Rows:
        """
        Returns Rows of data from hard disk based on DataRetrieval.

        Args:
            data_retrieval: Object containing data to help determine which data to be retrieved from hard disk.
        """
        serializer = TableManager()
        index_manager = IndexManager()
        all_filtered_data: Rows = []

        for table_name in data_retrieval.table:  # (support for join in the future)
            indexed_rows = []
            use_index = False

            indexable_conditions = [
                condition for condition in data_retrieval.conditions if condition.operation  in ["=", ">","<"]
            ]

            for condition in indexable_conditions:
                index = index_manager.readIndex(table_name, condition.column)
                if index:  # Use index if available
                    block_id = index.search(condition.operand)
                    if block_id is not None:
                        block_data = serializer.readBlockIndex(table_name, block_id)
                        indexed_rows.extend(block_data)
                        use_index = True

            if use_index and indexed_rows: 
                print("Pencarian menggunakan index")
                cond_filtered_data = serializer.applyConditions(indexed_rows, data_retrieval)
            else: 
                print("Pencarian tidak menggunakan index,baca semua blok")
                data = serializer.readTable(table_name)
                cond_filtered_data = serializer.applyConditions(data, data_retrieval)

           
            column_filtered_data = serializer.filterColumns(cond_filtered_data, data_retrieval.column)

            
            all_filtered_data.extend(column_filtered_data)

        print(all_filtered_data)
        print("Amount: ", len(all_filtered_data))
        return all_filtered_data



    def writeBlock(self ,data_write: DataWrite) -> int:
        """
        Returns the number of affected rows
        
        Args:
            data_write : objects contains data to help determine which data to be retrieved from hard disk, contain modified data for modification operation, and new data for adddition operation
        
        """
        serializer = TableManager()
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

        serializer = TableManager()

        # Filtereed table based on condition
        data : Rows = serializer.readTable(data_deletion.table)
        filtered_Table = serializer.applyConditions(data, data_deletion.conditions) 

        schema = serializer.readSchema(data_deletion.table)

        # Create new data that doesn't contain filtered table
        newData = data.getRowsNotMatching(filtered_Table)
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
        serializer = TableManager()
        indexManager = IndexManager()

        


    def getStats(self, test = False) -> dict:
        """
        Return dictionary of statistics for all tables in the database
        """
        path_name = "../Storage" if not test else "TestStatistics/"
        
        current_dir = os.path.dirname(os.path.abspath(__file__)) 
        storage_dir = os.path.join(current_dir, path_name) 
        storage_dir = os.path.abspath(storage_dir)  
        print(storage_dir)
        all_stats = {}

        # Check all tables in the directory
        for file_name in os.listdir(storage_dir):
            if file_name.endswith("_scheme.dat"):
                table_name = file_name.replace("_scheme.dat", "")
                try:
                    stats = self.getStatsOneTable(table_name) if not test else self.getStatsOneTable(table_name, storage_dir + "/")
                    all_stats[table_name] = stats
                except Exception as e:
                    print(f"Error saat membaca statistik untuk tabel {table_name}: {e}")
        
        return all_stats

    
    def getStatsOneTable(self,table_name,path_folder = None) -> Statistics:
        """
        Returns Statistics object that has number of tuples, number of blocks, size of tuple, blocking factor, and number of distinct values appear in r
        """
        serializer = TableManager() if path_folder is None else TableManager(path_name=path_folder)
        
        schema = serializer.readSchema(table_name) if path_folder is None else serializer.readSchema(table_name)
        
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
