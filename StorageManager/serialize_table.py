import os
"""
As we didn't create any functionality of create table, this is the main entry
of creating a table as given in specs which is student and course
"""

import random
from faker import Faker
from StorageManager.manager.TableManager import TableManager
from StorageManager.manager.IndexManager import IndexManager


class TableCreator:
    def __init__(self, amount = 100):
        self.fake = Faker()
        self.amount_of_data = amount
        self.serializer = TableManager()
        self.indexManager = IndexManager()
        self.setupTable()
        
    def setupTable(self):
        self.student_schema = [
            ('studentid', 'int', 4),
            ('fullname', 'varchar', 50),
            ('gpa', 'float', 4),
        ]

        self.course_schema = [
            ('courseid', 'int', 4),
            ('year', 'int', 4),
            ('coursename', 'varchar', 50),
            ('coursedesc', 'varchar', 50),
        ]

        self.course_name = [
    "Mathematics", "Physics", "Chemistry", "Biology", "English Literature",
    "History", "Computer Science", "Philosophy", "Economics", "Art",
    "Psychology", "Sociology", "Political Science", "Law", "Engineering",
    "Medicine", "Nursing", "Architecture", "Music", "Theater Studies",
    "Anthropology", "Geography", "Linguistics", "Statistics", "Business Administration",
    "Marketing", "Finance", "Accounting", "Management", "Data Science",
    "Cybersecurity", "Software Engineering", "Environmental Science", "Agriculture",
    "Astronomy", "Astronomical Engineering", "Geology", "Marine Biology", "Veterinary Medicine",
    "Public Health", "Education", "Journalism", "Graphic Design", "Digital Media",
    "Artificial Intelligence", "Robotics", "Game Design", "International Relations",
    "Supply Chain Management", "Sports Science", "Human Resources", "Criminology",
    "Tourism Management", "Event Management", "Urban Planning", "Interior Design", "Fashion Design",
    "Culinary Arts", "Hospitality Management", "Pharmaceutical Sciences", "Nutrition",
    "Biotechnology", "Genetics", "Neuroscience", "Biochemistry", "Mathematical Economics",
    "Sustainable Development", "Renewable Energy", "Marine Engineering", "Nanotechnology",
    "Materials Science", "Data Analytics", "Blockchain Technology", "Internet of Things", "Cloud Computing"
]   
        self.course_description = [
    "Introduction to fundamental concepts and applications.",
    "An in-depth study of theoretical and practical principles.",
    "Advanced topics with a focus on real-world applications.",
    "Basic principles and foundational theories.",
    "Exploring classical and contemporary literature.",
    "Study of the mind and behavior, focusing on cognitive and social processes.",
    "Examination of human society, including institutions, cultures, and interactions.",
    "In-depth analysis of political structures, governance, and policies.",
    "Comprehensive study of legal systems, laws, and ethical frameworks.",
    "Introduction to engineering disciplines and principles.",
    "Prepares students for a career in healthcare, covering human biology and treatments.",
    "Fundamental principles of building design, construction, and urban planning.",
    "Creative exploration of music theory, composition, and performance.",
    "Study of theater history, performance, and stage production.",
    "Study of human societies, culture, and history through anthropology.",
    "Analysis of Earth's physical features, weather patterns, and geographic phenomena.",
    "Study of language structure, syntax, semantics, and phonetics.",
    "Quantitative analysis and application of data to support decision-making.",
    "Principles and theories behind global business management and practices.",
    "In-depth study of financial markets, investment strategies, and corporate finance.",
    "Study of financial records, accounting principles, and auditing processes.",
    "Fundamentals of organizational management, leadership, and strategic planning.",
    "Introduction to data manipulation, analysis, and machine learning techniques.",
    "Security protocols, encryption methods, and defending against cyber threats.",
    "Study of software development lifecycle, coding, and testing practices.",
    "Analysis of the environmental impact of human activity and sustainability practices.",
    "Study of agricultural systems, food production, and sustainable practices.",
    "Introduction to the universe, celestial bodies, and cosmic phenomena.",
    "Specialized knowledge in designing and developing space-related technologies.",
    "Study of Earth's structure, materials, and geophysical processes.",
    "Study of aquatic ecosystems, marine life, and environmental conservation efforts.",
    "Principles of diagnosing, treating, and preventing diseases in animals.",
    "Study of healthcare systems, policies, and health management practices.",
    "Research in educational methods, theories, and pedagogy.",
    "Study of the practices and ethics of journalism and media production.",
    "Introduction to visual communication, digital media production, and design principles.",
    "Principles of artificial intelligence, machine learning, and intelligent systems.",
    "Development and design of autonomous machines, including industrial robots.",
    "Designing and developing interactive video games and entertainment software.",
    "Study of global relations, diplomacy, and international conflicts.",
    "Analysis of the management and flow of goods, services, and information.",
    "Examination of human resource practices, talent acquisition, and organizational behavior.",
    "Study of crime, criminal behavior, law enforcement, and justice systems.",
    "Exploration of tourism industry trends, management, and travel planning.",
    "Managing events, including logistics, marketing, and client relationships.",
    "Study of urban development, zoning laws, and sustainable city planning.",
    "Creative exploration of interior space, architecture, and design principles.",
    "Design and development of fashion collections, textiles, and trends.",
    "Preparation of culinary arts, including cooking techniques and food presentation.",
    "Introduction to hospitality industry management and customer service.",
    "Study of pharmaceutical science, drug development, and healthcare systems.",
    "Principles of nutrition, dietetics, and food science for better health.",
    "Biological sciences focusing on genetics, molecular biology, and biotechnology.",
    "Study of the brain and nervous system, including behavior and cognitive functions.",
    "In-depth research into the biochemical processes occurring in living organisms.",
    "Theoretical and applied study of economics, including mathematical modeling techniques.",
    "Study of the balance between environmental, economic, and social factors for sustainability.",
    "Principles of clean energy, renewable resources, and environmental impact reduction.",
    "Research and application of nanomaterials and technologies for various industries.",
    "Study of new materials and their applications in technology and manufacturing.",
    "Advanced data interpretation techniques, predictive modeling, and decision-making support.",
    "Study of emerging technologies, including blockchain, decentralized finance, and cryptocurrencies.",
    "Internet-connected devices and their integration into daily life and industries.",
    "Cloud computing and its applications for data storage, computing power, and business solutions."
]
        self.student_data = [
            [i, self.fake.name(), round(random.uniform(1.0, 4.0))]
            for i in range(1, self.amount_of_data + 1)
        ]

        self.course_data = [
    [
        i, 
        random.randint(1998, 2024),  
        random.choice(self.course_name), 
        random.choice(self.course_description)
    ]
    for i in range(1, self.amount_of_data + 1)
]
    
    def resetTable(self):
        self.student_table = "student"
        self.course_table = "course"
        self.serializer.writeTable(self.student_table,self.student_data ,self.student_schema)
        self.serializer.writeTable(self.course_table,self.course_data ,self.course_schema)
        self.indexManager.writeIndex(self.course_table,'courseid')
        self.indexManager.writeIndex(self.student_table,'studentid')

    def displayTable(self, table : str):
        data_with_schema = self.serializer.readTable(table)
        for row in data_with_schema:
            print(row)
    
if __name__ == 'main':
    table_creator = TableCreator(100)
    table_creator.resetTable()
    table_creator.displayTable('course')

    
