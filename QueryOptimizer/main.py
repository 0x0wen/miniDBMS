from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.CustomException import CustomException
from QueryOptimizer.whereOptimize import optimizeWhere
from QueryOptimizer.sortLimitOptimize import optimizeSortLimit
from StorageManager.objects.Statistics import Statistics
from StorageManager.StorageManager import StorageManager
storage_manager = StorageManager()
test = {
    "users": {"row": 100, "cols": ["user_id", "name", "age", "sibling", "office_id"]},
    "office": {"row": 80, "cols": ["office_id", "name", "location"]},
    "address": {"row": 80, "cols": ["address_id", "address", "city", "state", "zip"]},
    "salary": {"row": 80, "cols": ["salary_id", "user_id", "salary", "date"]},
}
# Example SQL query
query_str = "SELECT users.name, users.age FROM users JOIN salary ON users.salary_id = salary.salary_id JOIN office ON users.office_id = office.office_id JOIN houses ON houses.house_id = office.office_id WHERE users.age > 18 AND office.name = 'Off_1' AND office.name = users.name ORDER BY users.age DESC LIMIT 10"
# query_str = "UPDATE users SET age = 20, name = 'ahmed' WHERE age > 20 OR name = 'John'"
# query_str = "UPDATE users SET age = 20 WHERE name = 'John'"
# query_str = "users AS u, abc AS a, temp as t" # Testing AS
# query_str = "ORDER BY name, age LIMIT 1"
# query_str = "DELETE FROM table WHERE age = 20 AND name = 'John' OR age = 30"
# query_str = "BEGIN TRANSACTION"
query_str = "SELECT users.name, users.age FROM users, salary NATURAL JOIN office JOIN houses ON office.house = house.office WHERE users.age > 18 AND office.name = 'Off_1' AND salary.salary >= 1000"
# Initialize the optimization engine
engine = OptimizationEngine()
statistic = {
    'users': Statistics(n_r=100, b_r=10, l_r=72, f_r=10, V_a_r={'id': 100, 'name': 100, 'age': 100, 'salary_id': 100, 'office_id': 100}),
    'salary': Statistics(n_r=200, b_r=15, l_r=80, f_r=12, V_a_r={'salary_id': 200, 'amount': 200, 'type': 200}),
    'office': Statistics(n_r=300, b_r=20, l_r=90, f_r=15, V_a_r={'office_id': 300, 'name': 300, 'location': 300}),
    'houses': Statistics(n_r=400, b_r=25, l_r=100, f_r=16, V_a_r={'house_id': 400, 'address': 400, 'value': 400})
}

# Parse the query
try:
    parsed_query = engine.parseQuery(query_str, statistic)
    # parsed_query = optimizeWhere(parsed_query)
    # parsed_query = optimizeSortLimit(parsed_query)
except CustomException as e:
    print(e)
    exit(1)
except Exception as e:
    print(f"Exception: {e}")
    exit(1)

print(f"{parsed_query.query_tree}")
print('-----------------------')

# is_valid = engine.validateParsedQuery(parsed_query.query_tree)
# if is_valid:
#     print("Query is valid and ready for optimize.")
# else:
#     print("Query contains syntax errors.")

# # Optimize the query
# print(f"Statistics: {statistic}")
optimized_query = engine.optimizeQuery(parsed_query,statistic)
print("Optimized Query:", optimized_query)
# print(f"Optimized Query Estimated Cost: {optimized_query.estimated_cost}")
