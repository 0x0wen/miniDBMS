from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Statistics import Statistics
from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager
from StorageManager.objects.Rows import Rows
from functools import reduce
from FailureRecovery.FailureRecovery import FailureRecovery 
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
        #Fungsi bantu index ranging
        def process_ranges_with_column(conditions): 

            def generate_range(start, end, operator):
                """Menghasilkan range berdasarkan operator."""
                if operator == '>' or operator == '>=':
                    return list(range(start , end + 1)) 
                elif operator == '<' or operator == '<=':
                    return list(range(0, start + 1))  
                else:
                    raise ValueError(f"Operator {operator} tidak valid")

            column_groups = {}
            for start, end, operator, column in conditions:
                if column not in column_groups:
                    column_groups[column] = []
                column_groups[column].append(generate_range(start, end, operator))

            result = {}
            for column, ranges in column_groups.items():
                if ranges:
                    intersect_result = sorted(reduce(lambda x, y: list(set(x) & set(y)), ranges))
                    if intersect_result:
                        result[column] = intersect_result  
                    else:
                        result[column] = sorted(list(set().union(*ranges)))

            return result        
        
        def retrieve_indexed_data(data_retrieval : DataRetrieval, table_name: str, index_manager : IndexManager, serializer : TableManager) -> Rows:
            """
            Helper method to retrieve data using indexes.

            Args:
                data_retrieval: Object containing the conditions and other data to retrieve.
                table_name: The name of the table being queried.

            Returns:
                A list of rows retrieved using indexes.
            """
            indexed_rows = []
            ranged_indexed_id = []

            # Filter indexable conditions
            indexable_conditions = [
                condition for condition in data_retrieval.conditions if condition.operation in ["=", ">", "<", "<=", ">="]
            ]

            
            for i, condition in enumerate(indexable_conditions):
                # print(f"-=-=-=--=-=-=Condition-{i} yaitu {condition}--=-=-=-=-=-=-=-=--==")
                index = index_manager.readIndex(table_name, condition.column)
                if not index:
                    continue

                if condition.operation == "=":
                    # print("Operasi EQUAL ditemukan")
                    block_id = index.search(condition.operand)
                    if block_id is not None:
                        block_data = serializer.readBlockIndex(table_name, block_id)
                        indexed_rows.extend(block_data)
                elif condition.operation in ['>', '<', '>=', '<=']:
                    # print("Operasi RANGE ditemukan")
                    block_id = index.search(condition.operand)
                    if(block_id != -1): # Kalo gk ada
                        ranged_indexed_id.append([block_id, len(index), condition.operation, condition.column])

            # Process ranges if applicable
            ranged_indexed_id = process_ranges_with_column(ranged_indexed_id)
            for column in ranged_indexed_id:
                if ranged_indexed_id[column]:
                    for i in ranged_indexed_id[column]:
                        block_data = serializer.readBlockIndex(table_name, i)
                        indexed_rows.extend(block_data)

            return Rows(indexed_rows)
    
    
        serializer = TableManager()
        index_manager = IndexManager()
        all_filtered_data = Rows([])
        table_name = data_retrieval.table[0] 
        indexed_rows : Rows = []

        #Kalo condisi kosong, return semua data sesuai colomn
        if(not len(data_retrieval.conditions)):
            #     data_retrieval= DataRetrieval(
            #     table=['course'],
            #     column=[],
            #     conditions=[]
            # 
            data = serializer.readTable(table_name)
            column_filtered_data = serializer.filterColumns(data, data_retrieval.column)
            failureRecovery = FailureRecovery()
            buffered_row = failureRecovery.buffer.retrieveData(data_retrieval)
            if(buffered_row):
                return buffered_row
            else:
                PK = serializer.getPrimaryKey(table_name)
                failureRecovery.buffer.writeData(rows=column_filtered_data, dataRetrieval=data_retrieval,primaryKey=PK)
                rows = failureRecovery.buffer.retrieveData(data_retrieval)
                return Rows(rows)

            

        #Cek dulu, ada gk condisi yang pake column yang gk ada index
        for cond in data_retrieval.conditions:
            
            index = index_manager.readIndex(table_name, cond.column)
            
            if(not index):
                all_filtered_data.setIndex(None)
                continue
                
            all_filtered_data.setIndex(cond.column)
        

        # Read indexed rows
        if(all_filtered_data.isIndexed()):
            indexed_rows = retrieve_indexed_data(data_retrieval, table_name, index_manager, serializer)

        if  indexed_rows:  #cek (indexed_rows) harus ada hasil, antisipasi bener bener index digunakan pada column tidak cocok
            cond_filtered_data = serializer.applyConditions(indexed_rows,data_retrieval)

        else: 
            data = serializer.readTable(table_name)
            cond_filtered_data = serializer.applyConditions(data, data_retrieval)

        
        column_filtered_data = serializer.filterColumns(cond_filtered_data, data_retrieval.column)
        all_filtered_data.extend(column_filtered_data)
        if(all_filtered_data.__len__() == 0):
           all_filtered_data = []

        # write to buffer in failureRecovery
        failureRecovery = FailureRecovery()
        PK = serializer.getPrimaryKey(table_name)
        failureRecovery.buffer.writeData(rows=all_filtered_data, dataRetrieval=data_retrieval,primaryKey=PK)
        rows = failureRecovery.buffer.retrieveData(data_retrieval)
        return rows    
        # return all_filtered_data


    def writeBlock(self ,data_write: DataWrite) -> int:
        """
        Returns the number of affected rows
        
        Args:
            data_write : objects contains data to help determine which data to be retrieved from hard disk, contain modified data for modification operation, and new data for adddition operation
        
        """
        table_manager = TableManager()
        index_manager = IndexManager()

        pk = table_manager.getPrimaryKey(data_write.selected_table)

        table_name = data_write.selected_table
        new_data = data_write.new_value

        def replace_data(old_data, new_data, filtered_old_data):
            if len(new_data) != len(filtered_old_data):
                raise ValueError("new_data and filtered_old_data must have the same length.")

            indices_to_replace = [old_data.index(row) for row in filtered_old_data]

            updated_data = old_data.copy()
            for new_row, idx in zip(new_data, indices_to_replace):
                updated_data[idx] = new_row

            return updated_data

        if (data_write.overwrite):
            schema = table_manager.readSchema(table_name)
            old_data = table_manager.readData(table_name, schema)
            column_names = [column_name for column_name, _, _ in schema]

            old_data_with_schema: Rows = Rows([
                {column_name: value for column_name, value in zip(column_names, row)}
                for row in old_data
            ])

            filtered_old_data_with_schema = table_manager.applyConditions(old_data_with_schema, data_write)
            filtered_old_data = [list(d.values()) for d in filtered_old_data_with_schema]

            
            rows_to_write = replace_data(old_data, new_data, filtered_old_data)
            table_manager.writeTable(table_name, rows_to_write, schema)
            if(pk.__len__() > 0):
                for idx in pk:
                    index_manager.writeIndex(data_write.selected_table, idx)
            return new_data.__len__()
        else:
            result =  table_manager.appendData(table_name, new_data)
            if(pk.__len__() > 0):
                for idx in pk:
                    index_manager.writeIndex(data_write.selected_table, idx)
            return result
        
    def deleteBlock(self, data_deletion : DataDeletion) -> int:
        """
        Returns the number of removed rows

        Args: 
            data_deletion : objects contains data to help storage manager determince which data to be deleted.
        """

        serializer = TableManager()
        index_manager = IndexManager()

        cond_filtered_data : Rows= ""
        
        data = serializer.readTable(data_deletion.table)
        
        pk = serializer.getPrimaryKey(data_deletion.table)

        data_retrieval = DataRetrieval([data_deletion.table],[],data_deletion.conditions)
        cond_filtered_data = self.readBlock(data_retrieval)

        # Filtereed table based on condition
        schema = serializer.readSchema(data_deletion.table)

        
        # Create new data that doesn't contain filtered table
        newData = data.getRowsNotMatching(cond_filtered_data)
        
        serializer.writeTable(data_deletion.table, newData, schema)
        if(pk.__len__() > 0):
            for idx in pk:
                index_manager.writeIndex(data_deletion.table, idx)
        
        # get the amount of dat that is deleted
        return cond_filtered_data.__len__()


    def setIndex(self, table : str, column : str, index_type : str) -> None:
        """
        Handle creation of index in a given table

        Args : 
            table : the table to be given index
            column : certain column to be given index
            index_type: type of index (B+ Tree or Hash)
        """
        if(index_type.lower() == "hash"):
            indexManager = IndexManager()
            indexManager.writeIndex(table, column)
        else:
            print("Blm dibikin bang")
        
    def getStats(self, test = False) -> dict:
        """
        Return dictionary of statistics for all tables in the database
        """
        path_name = "../Storage" if not test else "TestStatistics/"
        
        current_dir = os.path.dirname(os.path.abspath(__file__)) 
        storage_dir = os.path.join(current_dir, path_name) 
        storage_dir = os.path.abspath(storage_dir)  

        
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
    
    def apply_transactions(self, physical_storage, transactions):
        """
        Update data in physical storage based on transaction logs.

        Args:
            physical_storage (list[dict]): Data yang ada di storage fisik.
            transactions (list[dict]): Log transaksi yang berisi perubahan.

        Returns:
            list[dict]: Data terbaru di storage fisik setelah transaksi diterapkan.
        """
        # Urutkan transaksi berdasarkan timestamp
        transactions.sort(key=lambda x: x['timestamp'])

        # Terapkan setiap transaksi
        for transaction in transactions:
            for before, after in zip(transaction['data_before'], transaction['data_after']):
                # Cari data di physical_storage yang cocok dengan data_before
                for idx, row in enumerate(physical_storage):
                    if row == before:  # Jika row ditemukan
                        physical_storage[idx] = after  # Ganti dengan data_after
                        break
                else:
                    # Jika tidak ditemukan, tambahkan data_after ke physical_storage
                    physical_storage.append(after)

        # Konversi ke array of arrays
        results = [list(row.values()) for row in physical_storage]
        return results
        
    def synchronize_storage(self):
        """
        Sinkronisasi antara buffer, log transaksi, dan pyhsical storage.
        """
        # Dapatkan buffer dan log transaksi
        failureRecovery = FailureRecovery()
        tables = failureRecovery.buffer.getTables()
        transaction_logs = failureRecovery.save_checkpoint()

        # Proses setiap tabel di buffer
        serializer = TableManager()
        for table in tables:
            # Ambil data dari penyimpanan fisik
            schema = serializer.readSchema(table.table_name)
            physical_storage = serializer.readTable(table.table_name)
            
            # Round float values to 2 decimal places
            physical_storage = [
                    {k: (round(v, 2) if isinstance(v, float) else v) for k, v in row.items()}
                    for row in physical_storage
                ]

            # Filter log transaksi untuk tabel yang sedang diproses
            table_logs = [log for log in transaction_logs if log['table'] == table.table_name]

            # Update data di penyimpanan fisik berdasarkan log transaksi 
            new_data = self.apply_transactions(physical_storage, table_logs)

            # Tulis data baru ke penyimpanan fisik
            serializer.writeTable(table.table_name, new_data, schema)

        return None
