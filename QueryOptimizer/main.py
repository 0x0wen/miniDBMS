from OptimizationEngine import OptimizationEngine
from QueryTree import QueryTree
from ParsedQuery import ParsedQuery


# Example SQL query
query_str = "SELECT name, age FROM users WHERE age > 18 ORDER BY name LIMIT 10"

# Initialize the optimization engine
engine = OptimizationEngine()

# Parse the query
parsed_query = engine.parseQuery(query_str)
print(f"Parsed Query Tree: {parsed_query.query_tree}")

# Optimize the query
optimized_query = engine.optimizeQuery(parsed_query)
print(f"Optimized Query Estimated Cost: {optimized_query.estimated_cost}")