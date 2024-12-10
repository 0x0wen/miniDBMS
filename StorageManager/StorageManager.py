from StorageManager.objects.DataRetrieval import DataRetrieval,Condition
from StorageManager.objects.JoinOperation import JoinOperation, JoinCondition
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.Statistics import Statistics
from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager
from QueryOptimizer.QueryTree import QueryTree
# from Serializer import *
from StorageManager.objects.Rows import Rows

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
    
        
        """
        tolong di adjust sama mekanisme kalian -rafi
        """
        failureRecovery = FailureRecovery()
        rows = failureRecovery.buffer.retrieveData(data_retrieval)

        print("Inside StorageManager.readblock()")
        
        if rows is not None:
            print("     Rows from buffer: ", rows)
            return rows
        else:
            print("     Rows from buffer is empty\n")
    
        
                
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
            # print("\n\nFrom StorageManager: column_filtered_data", column_filtered_data)
            all_filtered_data.extend(column_filtered_data)
            
        print("all filtered data", all_filtered_data)
            
        
        # write to buffer in failureRecovery
        failureRecovery.buffer.writeData(rows=cond_filtered_data, dataRetrieval=data_retrieval)
        
        return all_filtered_data



    def writeBlock(self ,data_write: DataWrite) -> int:
        """
        Returns the number of affected rows
        
        Args:
            data_write : objects contains data to help determine which data to be retrieved from hard disk, contain modified data for modification operation, and new data for adddition operation
        
        """
        table_manager = TableManager()
        table_name = data_write.selected_table
        new_data = data_write.new_value

        def replace_data(self, old_data, new_data, filtered_old_data):
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
            
            return new_data.__len__()
        else:
            return table_manager.appendData(table_name, new_data)
        
    def deleteDataOnStorage(self, dataToDelete : Rows) -> int:  
        """
        Delete the dataToDelete from physical storage
        
        Args: 
            data_deletion : objects contains the rows to delet in the storage manager
        """

    def writeDataOnStorage(self, serializer : TableManager, data_after_delete  : Rows, schema : list[tuple], written_table : str) -> int:
        """
        Delete data on Physical Storage
        """
        serializer.writeTable(written_table,data_after_delete, schema)

        return data_after_delete.__len__()

        
    def deleteBlock(self, data_deletion : DataDeletion) -> int:
        """
        Returns the number of removed rows

        Args: 
            data_deletion : objects contains data to help storage manager determince which data to be deleted.
        """

        serializer = TableManager()
        index_manager = IndexManager()

        use_index = False
        
        indexed_rows = []
        indexable_conditions = [
            condition for condition in data_deletion.conditions if condition.operation in ["=", ">", "<"]
        ]

        for condition in indexable_conditions:
            index = index_manager.readIndex(table_name=data_deletion.table, column=condition.column)

            if index:
                block_id = index.search(condition.operand)
                if block_id is not None:
                    block_data = serializer.readBlockIndex(table_name=data_deletion.table,block_index= block_id)
                    indexed_rows.extend(block_data)
                    use_index = True
        cond_filtered_data = ""
        data = ""
        data = serializer.readTable(data_deletion.table)
        if use_index and indexed_rows: 
            print("Pencarian menggunakan index")
            cond_filtered_data = serializer.applyConditions(indexed_rows, data_deletion)
        else: 
            cond_filtered_data = serializer.applyConditions(data, data_deletion)

        
        # Filtereed table based on condition
        schema = serializer.readSchema(data_deletion.table)
        
        # Create new data that doesn't contain filtered table
        newData = data.getRowsNotMatching(cond_filtered_data)

        FailureRecovery._instance.buffer.deleteData(newData)

        #NOTE - Use this to delete from physical data
        # delete_num =  self.deleteDataOnStorage(serializer,newData, schema)
        # index_manager.writeIndex(data_deletion.table, data_deletion.table)
        # return delete_num

        return newData.__len__()


    def setIndex(self, table : str, column : str, index_type : str) -> None:
        """
        Handle creation of index in a given table

        Args : 
            table : the table to be given index
            column : certain column to be given index
            index_type: type of index (B+ Tree or Hash)
        """
        if(index_type == "Hash"):
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
    
    def synchronize_stroage(self):
        #STUB - Panggil checkpoint dari Error recovery
        #STUB - untuk tiap table di buffer, cek ada datanya atau ga
        #STUB - baca data table dari physical
        #STUB - ambil data physical yang alive
        #STUB - Untuk tiap table ada isinya kita panggil writeTable
        return None
    
    def query_tree_to_data_retrieval(self, query_tree: QueryTree) -> DataRetrieval:
        """
        Convert QueryTree object to DataRetrieval object with proper handling of joins, where conditions, and tables.
        """
        tables = []
        columns = []
        where_conditions = []  # To store WHERE conditions
        join_conditions = []   # To store JOIN conditions
        processed_tables = set()  # To track processed tables 

        def traverse_tree(node: QueryTree, connector: str = None):
            """
            Recursively traverse the QueryTree object to extract tables, columns, and conditions.
            """
            nonlocal tables, columns, where_conditions, join_conditions, processed_tables

            if node.node_type == "SELECT":
                columns.extend(node.val) if node.val[0] != "*" else columns.extend([])
            elif node.node_type == "FROM":
                # Add tables from the FROM clause
                for table in node.val:
                    if table not in processed_tables:
                        tables.append(table)
                        processed_tables.add(table)

            elif node.node_type == "WHERE":
                if len(node.val) == 3:
                    # Simple WHERE condition
                    column, operation, operand = node.val
                    where_conditions.append(Condition(column=column, operation=operation, operand=operand, connector=connector))
                elif len(node.val) > 3:
                    # Complex WHERE condition (OR)
                    for i in range(0, len(node.val), 4):
                        column, operation, operand = node.val[i:i+3]
                        where_conditions.append(Condition(column=column, operation=operation, operand=operand, connector=connector))
                        connector = "OR"

            elif node.node_type == "JOIN":
                # Handle Cross Join (no condition)
                if len(node.val) == 0:
                    if tuple(tables) not in processed_tables:  # Ensure CROSS JOIN is added only once
                        join_conditions.append(JoinCondition(join_type="CROSS"))
                        processed_tables.add(tuple(tables))
                else:
                    # Don't add a generic JOIN, only process ON conditions for JOIN
                    pass

            elif node.node_type == "TJOIN":
                # Handle Natural Join or Join On with conditions
                if len(node.val) == 0:
                    # Natural Join (no condition)
                    join_conditions.append(JoinCondition(join_type="NATURAL"))
                else:
                    # JOIN ON
                    join_conditions.append(JoinCondition(join_type="ON", condition=node.val))

                # Add tables involved in the JOIN
                if node.children:
                    for child in node.children:
                        table_name = child.val[0]
                        if table_name not in processed_tables:
                            tables.append(table_name)
                            processed_tables.add(table_name)

            # Recursively process the child nodes
            for child in node.children:
                if child.node_type.startswith("Value"):  # Ensure only table nodes are added
                    table_name = child.val[0]
                    if table_name not in processed_tables:
                        tables.append(table_name)
                        processed_tables.add(table_name)

                traverse_tree(child, connector="AND")

        # Start traversing from the root of the QueryTree
        traverse_tree(query_tree)
        return DataRetrieval(table=tables, column=columns, conditions=where_conditions, join_conditions=join_conditions)