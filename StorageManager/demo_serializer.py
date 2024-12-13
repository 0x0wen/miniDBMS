from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager


student_schema = [
    ('studentid', 'int', 4),
    ('fullname', 'varchar', 50),
    ('gpa', 'float', 4),
]

course_schema = [
    ('courseid', 'int', 4),
    ('year', 'int', 4),
    ('coursename', 'varchar', 50),
    ('coursedesc', 'varchar', 600),
]

attends_schema = [
    ('studentid', 'int', 4),
    ('courseid', 'int', 4),
]

student_data = [
    [1, 'Alice Johnson', 3.8],
    [2, 'Bob Smith', 3.4],
    [3, 'Charlie Brown', 2.9],
]

course_data = [
    [101, 2024, 'Introduction to Databases', 'A foundational course on database systems and SQL.'],
    [102, 2024, 'Data Structures', 'An in-depth course on algorithms and data structures.'],
    [103, 2023, 'Operating Systems', 'A course on operating system concepts.']
]

attends_data = [
    [1, 101],
    [1, 102],
    [2, 101],
    [3, 103],
]

serializer = TableManager()
index_manager = IndexManager()
serializer.writeTable('student', student_data, student_schema)
index_manager.writeIndex('student', 'studentid')
serializer.writeTable('course', course_data, course_schema)
index_manager.writeIndex('course', 'courseid')
serializer.writeTable('attends', attends_data, attends_schema)

