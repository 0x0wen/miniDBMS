from ParsedQuery import ParsedQuery
from QueryTree import QueryTree

class OptimizationEngine:
    def __init__(self):
        self.statistics = {}  # Example: Holds table statistics for cost estimation

    def parseQuery(self, query: str) -> ParsedQuery:
        """
        Parses the given SQL query string into a ParsedQuery object.
        """
        # Tokenize and construct a basic QueryTree for demonstration purposes
        tokens = query.split()
        print('token:',tokens)
        root = self.__createQueryTree(tokens)

        # Return a ParsedQuery object
        return ParsedQuery(query=query, query_tree=root)

    def optimizeQuery(self, query: ParsedQuery) -> ParsedQuery:
        """
        Optimizes the parsed query and returns the optimized query.
        """
        # Apply heuristic optimizations (pushdowns, reorderings, etc.)
        self.__applyHeuristicRules(query)

        # Calculate and assign the cost to the query
        cost = self.__getCost(query)
        query.estimated_cost = cost  # Example of adding optimization-specific attributes

        return query

    def __getCost(self, query: ParsedQuery) -> int:
        """
        Calculates the estimated cost of executing the parsed query.
        """
        cost = 0
        if query.query_tree:
            # Traverse the QueryTree and sum up estimated costs
            cost = self.__traverseAndEstimateCost(query.query_tree)
        return cost

    # Auxiliary Helper Methods

    def __createQueryTree(self, tokens: list) -> QueryTree:
        """
        Helper method to create a complete QueryTree from tokens.
        Handles the full SQL query structure with proper parent-child relationships.
        """
        if not tokens:
            return None

        token = tokens.pop(0).upper()
        root = None

        if token == "SELECT":
            # Create SELECT node
            root = QueryTree(node_type="SELECT", val=[])
            
            # Get all columns until we hit FROM
            while tokens and tokens[0].upper() != "FROM":
                col = tokens.pop(0).rstrip(',')  # Remove trailing comma
                root.val.append(col)
                
                # Skip any commas
                if tokens and tokens[0] == ',':
                    tokens.pop(0)

            # Process FROM clause if it exists
            if tokens and tokens[0].upper() == "FROM":
                from_node = self.__createQueryTree(tokens)  # Recursive call for FROM
                if from_node:
                    from_node.parent = root
                    root.children.append(from_node)

            # Process WHERE clause if it exists
            if tokens and tokens[0].upper() == "WHERE":
                where_node = self.__createQueryTree(tokens)  # Recursive call for WHERE
                if where_node:
                    where_node.parent = root
                    root.children.append(where_node)

        elif token == "FROM":
            root = QueryTree(node_type="FROM", val=[])
            
            # Get all tables until we hit WHERE or end of tokens
            while tokens and tokens[0].upper() not in ["WHERE", "ORDER", "GROUP", "HAVING", "LIMIT"]:
                table = tokens.pop(0).rstrip(',')
                root.val.append(table)
                
                # Skip any commas
                if tokens and tokens[0] == ',':
                    tokens.pop(0)

        elif token == "WHERE":
            root = QueryTree(node_type="WHERE", val=[])
            
            # Get the condition
            condition = []
            while tokens and tokens[0].upper() not in ["ORDER", "GROUP", "HAVING", "LIMIT"]:
                condition.append(tokens.pop(0))
            root.val = condition

        elif token == "ORDER":
            if tokens and tokens.pop(0).upper() == "BY":
                root = QueryTree(node_type="ORDER_BY", val=[])
                
                while tokens and tokens[0].upper() not in ["LIMIT"]:
                    col = tokens.pop(0).rstrip(',')
                    root.val.append(col)
                    
                    # Skip any commas
                    if tokens and tokens[0] == ',':
                        tokens.pop(0)

        elif token == "LIMIT":
            root = QueryTree(node_type="LIMIT", val=[tokens.pop(0)] if tokens else [])

        else:
            root = QueryTree(node_type="UNKNOWN", val=[token])

        return root

    def __applyHeuristicRules(self, query: ParsedQuery):
        """
        Applies heuristic-based optimizations to the query.
        """
        # Example of optimizations like selection pushdown, join reordering, etc.
        if query.query_tree and query.query_tree.node_type == "SELECT":
            # Apply optimizations to the query tree here
            pass

    def __traverseAndEstimateCost(self, node: QueryTree) -> int:
        """
        Traverses the QueryTree recursively to calculate cost.
        """
        if not node:
            return 0

        # Example cost logic based on node type
        base_cost = {
            "SELECT": 50,
            "FROM": 100,
            "WHERE": 30,
            "ORDER_BY": 70,
            "LIMIT": 10,
        }.get(node.node_type, 0)

        # Sum the cost of this node and its children
        return base_cost + sum(self.__traverseAndEstimateCost(child) for child in node.children)
