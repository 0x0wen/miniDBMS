import struct

class Serializer:

    def __init__(self, schema):
        """
        schema: List of tuples (column_name, data_type, size)
        e.g., [('col1', 'int', 4), ('col2', 'char', 10), ...]
        """
        self.schema = schema
        self.path_name = 'Storage/'

    def serialize_row(self, row):
        """
        Serialize a row based on the schema.
        """
        row_binary = b''
        for value, (_, data_type, size) in zip(row, self.schema):
            if data_type == 'int':
                row_binary += struct.pack('i', value)
            elif data_type == 'float':
                row_binary += struct.pack('f', value)
            elif data_type == 'char':
                row_binary += struct.pack(f'{size}s', value.encode())
            elif data_type == 'varchar':
                # Pad varchar to the specified size
                row_binary += struct.pack(f'{size}s', value.encode().ljust(size, b'\x00'))
        return row_binary

    def write_table(self, file_name, data):
        """
        Write rows to a binary file.
        """
        with open(self.path_name + file_name, 'wb') as file:
            for row in data:
                file.write(self.serialize_row(row))

    def read_table(self, file_name):
        """
        Read rows from a binary file.
        """
        data = []
        row_size = sum(size for _, _, size in self.schema)
        with open(self.path_name + file_name, 'rb') as file:
            while row := file.read(row_size):
                row_data = []
                offset = 0
                for _, data_type, size in self.schema:
                    if data_type == 'int':
                        value = struct.unpack_from('i', row, offset)[0]
                        offset += 4
                    elif data_type == 'float':
                        value = struct.unpack_from('f', row, offset)[0]
                        offset += 4
                    elif data_type in ('char', 'varchar'):
                        value = struct.unpack_from(f'{size}s', row, offset)[0].decode().strip('\x00')
                        offset += size
                    row_data.append(value)
                data.append(row_data)
        return data

    
