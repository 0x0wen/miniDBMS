import os
"""
As we didn't create any functionality of create table, this is the main entry
of creating a table as given in specs which is student and course
"""

import random
from faker import Faker
from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager


fake = Faker()

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

data_student = [
    [i, fake.name(), round(random.uniform(2.0, 4.0))]
    for i in range(1, n_data + 1)
]

course_names = [
    "Mathematics", "Physics", "Chemistry", "Biology", "English Literature",
    "History", "Computer Science", "Philosophy", "Economics", "Art"
]

course_descriptions = [
    "Introduction to fundamental concepts and applications.",
    "An in-depth study of theoretical and practical principles.",
    "Advanced topics with a focus on real-world applications.",
    "Basic principles and foundational theories.",
    "Exploring classical and contemporary literature.",
]

data_course = [
    [
        i, 
        random.randint(2000, 2023),  
        random.choice(course_names), 
        random.choice(course_descriptions)
    ]
    for i in range(1, 101)
]

serializer = TableManager()
indexmanager = IndexManager()
student_table = "student"
course_table = "course"

serializer.writeTable(student_table,data_student ,student)
serializer.writeTable(course_table,data_course ,course)
indexmanager.writeIndex(course_table,'courseid')
indexmanager.writeIndex(student_table,'studentid')



data_with_schema = serializer.readTable(course_table)
for row in data_with_schema:
    print(row)
