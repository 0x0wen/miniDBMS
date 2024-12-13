from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.CustomException import CustomException
from QueryOptimizer.whereOptimize import optimizeWhere
from QueryOptimizer.sortLimitOptimize import optimizeSortLimit
from StorageManager.objects.Statistics import Statistics
from StorageManager.StorageManager import StorageManager
storage_manager = StorageManager()

# Example SQL query
queries =   [
                "SELECT * FROM student",
                "SELECT student.fullname from student where student.gpa = 4.0", 
                "SELECT course.coursename from student where course.year = 2020",
                "SELECT * from attends where attends.studentid = 2",
                "SELECT student.name, course.coursename from attends JOIN student ON attends.studentID = student.studentID JOIN course ON attends.courseID = course.courseID where student.gpa = 3.5",
                "SELECT * FROM attends, student JOIN course ON attends.CourseID = course.CourseID WHERE attends.studentID = student.studentID AND student.gpa = 3.5",
            ]

# Initialize the optimization engine
engine = OptimizationEngine()
statistic = {
    'Student': Statistics(n_r=100, b_r=10, l_r=72, f_r=10, V_a_r={'StudentID': 100, 'FullName': 100, "GPA": 100}),
    'Course': Statistics(n_r=200, b_r=10, l_r=72, f_r=10, V_a_r={'CourseID': 200, 'Year': 200, 'CourseName': 200, 'CourseDescription': 200}),
    'Attends': Statistics(n_r=500, b_r=15, l_r=80, f_r=12, V_a_r={'StudentID': 500, 'CourseID': 500}),
}

# Parse the query
try:
    for query in queries:
        parsed_query = engine.parseQuery(query, statistic)
        # print(f"{parsed_query.query_tree}")
        print('-----------------------')
        optimized_query = engine.optimizeQuery(parsed_query,statistic)
        print("Optimized Query:", optimized_query)
        print("✅ Successfully parsed the query.")
    # parsed_query = optimizeWhere(parsed_query)
    # parsed_query = optimizeSortLimit(parsed_query)
except CustomException as e:
    print("✅ Successfully parsed the query.")
    print(e)
    exit(1)
except Exception as e:
    print("❌ Failed to parse and optimize the query.")
    print(f"Exception: {e}")
    exit(1)