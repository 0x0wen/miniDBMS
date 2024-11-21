from StorageManager.manager.TableManager import TableManager

schema = [
    ('id', 'int', 4),
    ('umur', 'char', 32),
    ('harga', 'float', 4),
    ('desk', 'char', 32),
]

student =[
    ('studentid', 'int', 4),
    ('fullname', 'varchar', 50),
    ('gpa', 'float', 4),
]
course =[
    ('courseid', 'int', 4),
    ('year', 'int', 4),
    ('coursename', 'varchar', 50),
    ('coursedesc', 'varchar', 50),

]

n_data = 100
user2 = [[i,f'data{i}',10 + i*0.25, f'desk{i}' ]for i in range(n_data)]
data_student = [[i,f'Belakang Depan Student{i}',0.25 * i ]for i in range(n_data)]
data_course = [[i,2000 + i,f'Course Name{i}',f'Course Deskripsion aaaaa{i}' ]for i in range(n_data)]

append_data_course = [[111, 11, '11', '11']]


serializer = TableManager()
student_table = "student"
course_table = "course"

serializer.writeTable('user2',user2 ,schema)
serializer.writeTable(student_table,data_student ,student)
serializer.writeTable(course_table,data_course ,course)

# cnt_modified_data = serializer.appendData(course_table,append_data_course)
# cnt_modified_data = serializer.appendData(course_table,append_data_course)
# cnt_modified_data = serializer.appendData(course_table,append_data_course)
# cnt_modified_data = serializer.appendData(course_table,append_data_course)
# cnt_modified_data = serializer.appendData(course_table,append_data_course)

# print(cnt_modified_data)
data_with_schema = serializer.readTable(course_table)
for row in data_with_schema:
    print(row)
    

#baca block index ke-2 dari tabel student
print(serializer.readBlockIndex(student_table,2))
#baca block index ke-10 dari tabel course
print(serializer.readBlockIndex(course_table,10))


