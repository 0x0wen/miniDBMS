from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.CustomException import CustomException
from QueryOptimizer.whereOptimize import optimizeWhere
from QueryOptimizer.sortLimitOptimize import optimizeSortLimit
from QueryOptimizer.rule8Optimize import rule8



test = {
    "users": {"row": 100, "cols": ["user_id", "name", "age", "sibling", "office_id"]},
    "office": {"row": 80, "cols": ["office_id", "name", "location"]},
    "address": {"row": 80, "cols": ["address_id", "address", "city", "state", "zip"]},
    "salary": {"row": 80, "cols": ["salary_id", "user_id", "salary", "date"]},
}
# Example SQL query
# query_str = "SELECT users.name, users.age FROM users JOIN office ON users.office_id = office.office_id WHERE users.age > 18 AND office.name = 'Off_1' AND office.name = users.name ORDER BY users.age DESC LIMIT 10"
# query_str = "UPDATE users SET age = 20, name = 'ahmed' WHERE age > 20 OR name = 'John'"
# query_str = "UPDATE users SET age = 20 WHERE name = 'John'"
# query_str = "users AS u, abc AS a, temp as t" # Testing AS
# query_str = "ORDER BY name, age LIMIT 1"
# query_str = "DELETE FROM table WHERE age = 20 AND name = 'John' OR age = 30"
# query_str = "BEGIN TRANSACTION"
query_str = "SELECT users.name, users.age FROM users, salary NATURAL JOIN office JOIN houses ON office.house = house.office WHERE users.age > 18 AND office.name = 'Off_1' AND salary.salary >= 1000"
# Initialize the optimization engine
engine = OptimizationEngine()

# Parse the query
try:
    parsed_query = engine.parseQuery(query_str)
    parsed_query = rule8(parsed_query)
    # parsed_query = optimizeWhere(parsed_query)
    # parsed_query = optimizeSortLimit(parsed_query)
except CustomException as e:
    print(e)
    exit(1)
except Exception as e:
    print(f"Exception: {e}")
    exit(1)

print(f"{parsed_query.query_tree}")

# is_valid = engine.validateParsedQuery(parsed_query.query_tree)
# if is_valid:
#     print("Query is valid and ready for optimize.")
# else:
#     print("Query contains syntax errors.")

# # Optimize the query
# optimized_query = engine.optimizeQuery(parsed_query)
# print(f"Optimized Query Estimated Cost: {optimized_query.estimated_cost}")
