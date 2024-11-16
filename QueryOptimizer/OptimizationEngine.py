from ParsedQuery import ParsedQuery
from QueryTree import QueryTree

class OptimizationEngine:
    def parseQuery(self, query: str) -> ParsedQuery:
        """Parse the input query string."""
        parsed_query = ParsedQuery(query)
        parsed_query.parseQuery()
        return parsed_query
    
    def optimizeQuery(self, parsed_query: ParsedQuery):
        """Optimize the parsed query using heuristic rules."""
        self.applyHeuristicRules(parsed_query)
        parsed_query.estimated_cost = self.__getCost(parsed_query)

    def applyHeuristicRules(self, parsed_query: ParsedQuery):
        """
        Applies heuristic-based optimizations to the query.
        """
        # Example: Push down WHERE conditions
        self.pushDownSelections(parsed_query)

        # Example: Reorder joins (if applicable)
        self.reorderJoins(parsed_query)

        # Generate the execution order based on the optimized query tree
        parsed_query.execution_order = self.generateExecutionOrder(parsed_query.query_tree)

    def pushDownSelections(self, parsed_query: ParsedQuery):
        """Move WHERE conditions closer to data sources."""
        # Modify the query tree to reflect selection pushdown
        pass  # Implement the logic here

    def reorderJoins(self, parsed_query: ParsedQuery):
        """Reorder joins for better performance."""
        # Modify the query tree to reflect join reordering
        pass  # Implement the logic here

    def generateExecutionOrder(self, query_tree: QueryTree) -> list:
        """Generate execution order from the query tree."""
        # Traverse the query tree and generate the logical execution order
        order = []
        self.__traverseQueryTree(query_tree, order)
        return order

    def __traverseQueryTree(self, node: QueryTree, order: list):
        """Helper to traverse the query tree in execution order."""
        if node is None:
            return
        for child in node.children:
            self.__traverseQueryTree(child, order)
        order.append(f"{node.type}: {node.val}")

    def __getCost(self, parsed_query: ParsedQuery) -> int:
        """
        Estimate the cost of executing the parsed query based on its execution order.
        """
        cost = 0
        for operation in parsed_query.execution_order:
            if "SCAN" in operation:
                cost += 100  # Placeholder value for scanning
            elif "FILTER" in operation:
                cost += 50   # Placeholder value for filtering
            elif "SORT" in operation:
                cost += 70   # Placeholder value for sorting
            elif "LIMIT" in operation:
                cost += 10   # Placeholder value for limiting results
        return cost
