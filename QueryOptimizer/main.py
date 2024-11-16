from OptimizationEngine import OptimizationEngine
from QueryTree import QueryTree
from ParsedQuery import ParsedQuery


def main():
    # Input query
    query = "SELECT name, age FROM employees WHERE age > 30 JOIN departments ON employees.dept_id = departments.id"

    # Initialize the query optimizer
    optimizer = OptimizationEngine()

    # Parse the input query
    parsed_query = optimizer.parseQuery(query)

    # Optimize the parsed query
    optimizer.optimizeQuery(parsed_query)
    print('asyo')
    # Display the optimized query
    print(parsed_query)

if __name__ == "__main__":
    main()