from StorageManager.objects.Condition import Condition
from StorageManager.objects.Rows import Rows
from StorageManager.manager.DataManager import DataManager
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

    #pake 'Condition' kalo gk eror, python aneh
    def applyConditions(self,rows: list[dict], conditions: list[Condition]) -> Rows:
        """
        Return the D
        """
        def satisfies(row: dict, condition: Condition) -> bool:
            value = row.get(condition.column)
            if value is None:
                return False
            if condition.operation == "=":
                return value == condition.operand
            elif condition.operation == "<>":
                return value != condition.operand
            elif condition.operation == ">":
                return value > condition.operand
            elif condition.operation == "<":
                return value < condition.operand
            elif condition.operation == ">=":
                return value >= condition.operand
            elif condition.operation == "<=":
                return value <= condition.operand
            return False
        
        filtered_rows = []
        for row in rows:
            if all(satisfies(row, cond) for cond in conditions):
                filtered_rows.append(row)
        return Rows(filtered_rows)

    def filterColumns(self,rows: list[dict], columns: list[str]) -> list[dict]:
        
        if columns:
            return [{col: row[col] for col in columns if col in row} for row in rows]
        return rows
    
    def readTable(self, file_name) -> Rows:
        """ Read both schema and data from files using block allocation. """
        schema = self.readSchema(file_name) 
        data = self.readData(file_name, schema)  
        column_names = [column_name for column_name, _, _ in schema]
        data_with_schema: Rows = Rows([
            {column_name: value for column_name, value in zip(column_names, row)}
            for row in data
        ])
        return data_with_schema