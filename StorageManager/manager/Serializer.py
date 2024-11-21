from StorageManager.objects.Condition import Condition
import struct

"""
Format file yang dibuat 
{tabel}_blocks.dat --metadata block
{tabel}_data.dat --data
{tabel}_scheme.dat --metadata info atribut

"""
class Serializer:
    def __init__(self, path_name):
        self.path_name = path_name
    def serializeRow(self, row, schema):
        print(row)
        """ Serialize a single row based on the schema. """
        row_binary = b''
        for value, (_, data_type, size) in zip(row, schema):
            if data_type == 'int':            
                row_binary += struct.pack('i', value)
            elif data_type == 'float':
                row_binary += struct.pack('f', value)
            elif data_type == 'char':
                row_binary += struct.pack(f'{size}s', value.encode()[:size])
            elif data_type == 'varchar':
                row_binary += struct.pack(f'{size}s', value.encode().ljust(size, b'\x00')[:size])
        return row_binary