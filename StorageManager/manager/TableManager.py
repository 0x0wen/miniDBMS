from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.DataDeletion import DataDeletion
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.Condition import Condition

from StorageManager.objects.Rows import Rows
from StorageManager.manager.DataManager import DataManager
from StorageManager.manager.IndexManager import IndexManager
import struct

class TableManager(DataManager):
    def __init__(self,  path_name='Storage/', block_size=720) -> None:
        super().__init__(path_name, block_size)

    def writeTable(self, file_name, data, schema):
        """ Write data and schema to files with metadata blocks. """
        schema_file_name = file_name + '_scheme.dat'
        with open(self.path_name + schema_file_name, 'wb') as schema_file:
            schema_file.write(struct.pack('i', len(schema)))
            for column_name, data_type, size in schema:
                column_name_bytes = column_name.encode('utf-8')
                schema_file.write(struct.pack('i', len(column_name_bytes)))
                schema_file.write(column_name_bytes)
                schema_file.write(struct.pack('i', len(data_type)))
                schema_file.write(data_type.encode('utf-8'))
                schema_file.write(struct.pack('i', size))
        
        data_file_name = file_name + '_data.dat'
        blocks_file_name = file_name + '_blocks.dat'  
        blocks = []  # (offset, jumlah row)
        with open(self.path_name + data_file_name, 'wb') as data_file, open(self.path_name + blocks_file_name, 'wb') as blocks_file:
            current_offset = 0
            row_size = sum(size for _, _, size in schema)
            remaining_space = self.block_size
            block_data = []
            num_rows = 0

            for i, row in enumerate(data):
                block_data.append(self.serializeRow(row, schema))
                remaining_space-=row_size
                num_rows += 1

                if row_size > remaining_space  or i == len(data) - 1:
                    for block in block_data:
                        data_file.write(block)
                    blocks.append((current_offset, num_rows))
                    current_offset += sum(len(block) for block in block_data)
                    remaining_space = self.block_size
                    block_data = []  # Reset blok
                    num_rows = 0

            blocks_file.write(struct.pack('i', len(blocks)))  
            for offset, num_rows in blocks:
                blocks_file.write(struct.pack('ii', offset, num_rows))

    

    def group_conditions(self,conditions: list[Condition]) -> list[list[Condition]]:
        """
        Group conditions based on their connectors, by shifting connectors logic.
        """
        connectors = [condition.connector for condition in conditions]
        print(" Sebelum Connector: ",connectors)

        connectors = connectors[1:] + [None]  # shift kiri 1
        grouped_conditions = []
        current_group = []
        print(" Sesudah Connector: ",connectors)

        for i, condition in enumerate(conditions):
            current_group.append(condition)

            if connectors[i] == "OR" or i == len(conditions) - 1:
                grouped_conditions.append(current_group)
                current_group = []

        for group in grouped_conditions:
            print("Group: " , group)
        return grouped_conditions

    def applyConditions(self, rows: list[dict], action_object) -> Rows:
        """
        Apply conditions to filter rows based on DataRetrieval, DataDeletion, or DataWrite.
        Utilizes indexing if relevant conditions are found.
        """
        # Ambil kondisi berdasarkan tipe objek yang diberikan
        if isinstance(action_object, DataRetrieval):
            conditions = action_object.conditions
        elif isinstance(action_object, DataWrite):
            conditions = action_object.conditions
        elif isinstance(action_object, DataDeletion):
            conditions = action_object.conditions
        else:
            raise ValueError("Unsupported action object type")

        # Kelompokkan kondisi (misalnya, untuk menangani AND/OR)
        grouped_conditions = self.group_conditions(conditions)

        def satisfies(row: dict, condition: Condition) -> bool:
            value = row.get(condition.column)
            if value is None:
                return False
            
            # Normalisasi tipe data
            try:
                operand = type(value)(condition.operand) 
            except (ValueError, TypeError):
                return False  # Jika tidak bisa dikonversi, kondisi tidak terpenuhi
            
            # Mengecek kondisi operasi
            if condition.operation == "=":
                return value == operand
            elif condition.operation == "<>":
                return value != operand
            elif condition.operation == ">":
                return value > operand
            elif condition.operation == "<":
                return value < operand
            elif condition.operation == ">=":
                return value >= operand
            elif condition.operation == "<=":
                return value <= operand
            return False

        # Fungsi untuk mencari dengan menggunakan indeks jika kondisi sesuai
        def searchWithIndex(action_object) -> list[dict]:
            indexed_conditions = [cond for cond in action_object.conditions if cond.operation == "="]
            filtered_rows = []

            for condition in indexed_conditions:
                index_manager = IndexManager()
                try:
                    hashedbucket = index_manager.readIndex(action_object.table[0], condition.column)
                    if hashedbucket:
                        # Mencari blok yang sesuai dengan nilai operand dalam kondisi
                        block_id = hashedbucket.search(condition.operand)
                        if block_id is not None:
                            # Mengambil data dari blok yang ditemukan
                            block_data = self.readBlockIndex(action_object.table[0], block_id)
                            filtered_rows.extend(block_data)
                except ValueError:
                    # Tidak ada indeks untuk kolom tertentu
                    continue

            return filtered_rows

        # Mencari dengan menggunakan indeks terlebih dahulu jika memungkinkan
        indexed_rows = searchWithIndex(action_object)

        # Cek semua kondisi pada hasil dari indexed_rows jika tersedia
        filtered_rows = []
        rows_to_filter = indexed_rows if indexed_rows else rows

        for row in rows_to_filter:
            if any(all(satisfies(row, cond) for cond in group) for group in grouped_conditions):
                filtered_rows.append(row)

        return Rows(filtered_rows)





    def filterColumns(self,rows: list[dict], columns: list[str]) -> list[dict]:
        
        if columns:
            return [{col: row[col] for col in columns if col in row} for row in rows]
        return rows
    
    def readTable(self, file_name) -> Rows:
        """ Read both schema and data from files """
        schema = self.readSchema(file_name) 
        data = self.readData(file_name, schema)  
        column_names = [column_name for column_name, _, _ in schema]
        data_with_schema: Rows = Rows([
            {column_name: value for column_name, value in zip(column_names, row)}
            for row in data
        ])
        return data_with_schema