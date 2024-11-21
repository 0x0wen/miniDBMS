from OptimizationEngine import OptimizationEngine
from QueryTree import QueryTree
from ParsedQuery import ParsedQuery
from CustomException import CustomException

# Example SQL query
query_str = "UPDATE users SET age = 20, name = 'ahmed' WHERE age > 20 OR name = 'John'"
# query_str = "UPDATE users SET age = 20 WHERE name = 'John'"
# query_str = "BEGIN TRANSACTION"
# Initialize the optimization engine
engine = OptimizationEngine()

# Parse the query
try:
    parsed_query = engine.parseQuery(query_str)
except CustomException as e:
    print(e)
    exit(1)
except Exception as e:
    print(f"Exception: {e}")
    exit(1)
    
print(f"{parsed_query.query_tree}")

is_valid = engine.validateParsedQuery(parsed_query.query_tree)
if is_valid:
    print("Query is valid and ready for optimize.")
else:
    print("Query contains syntax errors.")
    
# Optimize the query
optimized_query = engine.optimizeQuery(parsed_query)
print(f"Optimized Query Estimated Cost: {optimized_query.estimated_cost}")