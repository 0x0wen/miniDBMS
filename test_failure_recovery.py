# # test_buffer_write.py
# from datetime import datetime
# import os
# import sys

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from StorageManager.StorageManager import StorageManager
# from StorageManager.objects.DataWrite import DataWrite
# from StorageManager.objects.Condition import Condition
# from StorageManager.objects.Rows import Rows

# def test_buffer_write():
#     print("Starting Buffer Write Test...")
    
#     sm = StorageManager()
    
#     # Test 1: Write new entries
#     print("\nTest 1: Write New Entries")
#     write_data1 = DataWrite(
#         overwrite=True,
#         selected_table="course",
#         column=["courseid", "year", "name"],
#         conditions=[],
#         new_value=[
#             {"courseid": 41, "year": 2041, "name": "Course Name41"},
#             {"courseid": 42, "year": 2042, "name": "Course Name42"}
#         ]
#     )
    
#     try:
#         success = sm.writeBuffer(write_data1)
#         print("✅ First write successful") if success else print("❌ First write failed")
#         print(f"Written data: {write_data1.new_value}")
#     except Exception as e:
#         print(f"❌ First write failed: {e}")

#     # Test 2: Write different data
#     print("\nTest 2: Write More Entries")
#     write_data2 = DataWrite(
#         overwrite=True,
#         selected_table="course",
#         column=["courseid", "year", "name"],
#         conditions=[],
#         new_value=[
#             {"courseid": 43, "year": 2043, "name": "Course Name43"},
#             {"courseid": 44, "year": 2044, "name": "Course Name44"}
#         ]
#     )
    
#     try:
#         success = sm.writeBuffer(write_data2)
#         print("✅ Second write successful") if success else print("❌ Second write failed")
#         print(f"Written data: {write_data2.new_value}")
#     except Exception as e:
#         print(f"❌ Second write failed: {e}")

# if __name__ == "__main__":
#     test_buffer_write()