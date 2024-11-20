from ParsedQuery import ParsedQuery
from QueryTree import QueryTree
import re
class OptimizationEngine:
    def __init__(self):
        self.statistics = {}  # Example: Holds table statistics for cost estimation

    def parseQuery(self, query: str) -> ParsedQuery:
        """
        Parses the given SQL query string into a ParsedQuery object.
        """
        # Tokenize and construct a basic QueryTree for demonstration purposes
        tokens = re.findall(r'[^\s,]+|,', query)
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
    
    def validateParsedQuery(self, query_tree: QueryTree) -> bool:
        """
        Validates the QueryTree structure for SQL syntax correctness.
        """
        if query_tree.node_type not in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "BEGIN_TRANSACTION", "COMMIT"]:
            print("Error: Query must start with a valid statement (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, BEGIN_TRANSACTION, COMMIT).")
            return False
        
        if query_tree.node_type == "SELECT":
            if not query_tree.children or query_tree.children[0].node_type != "FROM": # SELECT must have a FROM clause
                print("Error: SELECT query must contain a FROM clause.")
                return False
            
        if query_tree.node_type == "UPDATE":
            has_set= any(child.node_type == "SET" for child in query_tree.children)
            if not has_set: # UPDATE must have a SET clause
                print("Error: Query must contain a SET clause.")
                return False

        has_unknown = any(child.node_type == "UNKNOWN" for child in query_tree.children)
        if has_unknown:
            print("Error: Unknown syntax found in query.")
            return False

        print("QueryTree validation passed.")
        return True
    
    def validateDoubleComma(self, token, next_token) -> bool:
        if token == "," and next_token == ",":
            print("Error: Double comma found in query.")
            return False
        return True

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

        try:
            root = None
            current_node = None


            """
            General one token only
            """
            # If there is not one-token-only constraint, add it here 
            # If one-token-only read and it already exists, return error
            one_node_constraint = [] 

            """
            General first appear token
            """
            if tokens[0] not in ["SELECT", "UPDATE", "DELETE", "INSERT", "BEGIN", "COMMIT", "CREATE", "DROP"]:
                return Exception("First token must be either SELECT, UPDATE, DELETE, INSERT, BEGIN, COMMIT, CREATE, or DROP")


            """
            NI HUL YANG AS
            """            
            # rename = {}
            # temp = ""
            # while len(tokens) > 0:
            #     token = tokens.pop(0)
            #     if token.upper() == "AS":
            #         token = tokens.pop(0)
            #         if len(tokens) == 0 or token.upper() in [",", "LIMIT", "ORDER", "BY", "WHERE"]:
            #             return Exception("AS not followed by alias")
                    
            #         rename[token] = temp
            #         temp = ""
            #     elif token == ",":
            #         if len(tokens) > 0:
            #             if (not self.validateDoubleComma(token, tokens[0])):
            #                 return Exception("Double Comma")
            #         else:
            #             return Exception("Trailing Comma")
                    
            #         # TODO: Do What ',' does here
            #     else:
            #         if temp != "":
            #             return Exception("double value separated by space")
            #         temp = token


            """
            NI HUL YANG ORDER BY
            """
            token = tokens.pop(0)
            if token.upper() == "ORDER":
                token = tokens.pop(0)
                if token != "BY":
                    return Exception("ORDER not followed by ID")
                current_node = QueryTree(node_type="SORT", val=[]) # Create ORDER BY node

                token = tokens.pop(0) # Value of ORDER BY
                if token.upper() in ["LIMIT"]:
                    return Exception("ORDER BY not followed by value")
                    
                while len(tokens) > 0:
                    current_node.val.append(token)
                    token = tokens.pop(0)

                    if token == ",":
                        if (not self.validateDoubleComma(token, tokens[0])):
                            return Exception("Double Comma")
                        
                        token = tokens.pop(0)
                        if token.upper() in ["LIMIT"]:
                            return Exception("Trailing Comma")
                    elif token == "LIMIT":
                        break
                    else:
                        return Exception("each value must be separated by comma")
                        
                if token not in ["LIMIT"]: 
                    current_node.val.append(token)

                root = current_node
            

            """
            NI HUL YANG LIMIT
            """
            if token.upper() == "LIMIT":
                current_node = QueryTree(node_type="LIMIT", val=[])

                if len(tokens) == 0:
                    return Exception("LIMIT not followed by value")
                
                token = tokens.pop(0) # Value of LIMIT
                try:
                    int(token)
                except:
                    return Exception("LIMIT must be a number")
                
                current_node.val.append(token)
                root.children.append(current_node)  


            """
            NI HUL YANG DELETE
            """
            if token.upper() == "DELETE":
                
                # Next token must be FROM
                if len(tokens) == 0:
                    return Exception("DELETE not followed by FROM")
                
                token = tokens.pop(0)
                if token.upper() != "FROM":
                    return Exception("DELETE not followed by FROM")
                current_node = QueryTree(node_type="DELETE", val=[]) # Create DELETE node

                # Get Table
                if len(tokens) == 0:
                    return Exception("Table not specified")
                
                token = tokens.pop(0)
                if token.upper() in ["WHERE"]:
                    return Exception("Table not specified")
                current_node.val.append(token)

                # Check for WHERE clause
                if len(tokens) > 0:
                    token = tokens.pop(0)
                    if token.upper() != "WHERE":
                        return Exception("WHERE clause not found")
                    if len(tokens) == 0:
                        return Exception("WHERE clause not followed by condition")
                    if len(tokens) < 3:
                        return Exception("WHERE clause not followed by valid condition")
                    while len(tokens) > 0:
                        current_node.val.append(tokens.pop(0))

                        # TODO: VALIDATE CONDITION
                else:
                    return Exception("WHERE clause not found")
                
                root = current_node


        # if token == "SELECT":
        #     # Create SELECT node
        #     root = QueryTree(node_type="SELECT", val=[])
            
        #     # Get all columns until we hit FROM
        #     while tokens and tokens[0].upper() != "FROM":
        #         token = tokens.pop(0)
        #         if not token.endswith(',') and (tokens[0] != ','):
        #             root.val.append(token.rstrip(','))  # Remove trailing commas
        #             break
                
        #         root.val.append(token.rstrip(','))  # Remove trailing commas


        #     # Process FROM clause if it exists
        #     if tokens and tokens[0].upper() == "FROM":
        #         from_node = self.__createQueryTree(tokens)  # Recursive call for FROM
        #         if from_node:
        #             from_node.parent = root
        #             root.children.append(from_node)
        #             root = from_node  # Update root to FROM node

        #     # Process WHERE clause if it exists
        #     if tokens and tokens[0].upper() == "WHERE":
        #         where_node = self.__createQueryTree(tokens)  # Recursive call for WHERE
        #         if where_node:
        #             where_node.parent = root
        #             root.children.append(where_node)
        #             root = where_node  # Update root to WHERE node
                    
        #     # Process ORDER BY clause if it exists
        #     if tokens and tokens[0].upper() == "ORDER":
        #         order_node = self.__createQueryTree(tokens)  # Recursive call for ORDER BY
        #         if where_node:
        #             where_node.parent = root
        #             root.children.append(order_node)
        #             root = order_node  # Update root to ORDER BY node
            
        #     # Process LIMIT clause if it exists
        #     if tokens and tokens[0].upper() == "LIMIT":
        #         limit_node = self.__createQueryTree(tokens)  # Recursive call for LIMIT
        #         if where_node:
        #             where_node.parent = root
        #             root.children.append(limit_node)
        #             root = limit_node  # Update root to LIMIT node
                    
        #     if tokens : # if there is any token, add it as unknown
        #         root.children.append(QueryTree(node_type="UNKNOWN", val=[tokens]))

        # elif token == "FROM":
        #     root = QueryTree(node_type="FROM", val=[])
            
        #     # Get all tables until we hit WHERE or end of tokens
        #     while tokens and tokens[0].upper() not in ["WHERE", "ORDER", "GROUP", "HAVING", "LIMIT"]:
        #         table = tokens.pop(0).rstrip(',')
        #         root.val.append(table)
                
        #         # Skip any commas
        #         if tokens and tokens[0] == ',':
        #             tokens.pop(0)

        # elif token == "WHERE":
        #     root = QueryTree(node_type="WHERE", val=[])
            
        #     # Get the condition
        #     condition = []
        #     while tokens and tokens[0].upper() not in ["ORDER", "GROUP", "HAVING", "LIMIT"]:
        #         condition.append(tokens.pop(0))
        #     root.val = condition

        # elif token == "ORDER":
        #     if tokens and tokens.pop(0).upper() == "BY":
        #         root = QueryTree(node_type="ORDER_BY", val=[])
                
        #         while tokens and tokens[0].upper() not in ["LIMIT"]:
        #             col = tokens.pop(0).rstrip(',')
        #             root.val.append(col)
                    

        # elif token == "LIMIT":
        #     root = QueryTree(node_type="LIMIT", val=[tokens.pop(0)] if tokens else [])
            
        # elif token == "UPDATE":
        #     root = QueryTree(node_type="UPDATE", val=[tokens.pop(0).rstrip(',')] if tokens else [])
            
        #     # if tokens and tokens[0].upper() == "JOIN":
            
        #     if tokens and tokens[0].upper() == "SET":
        #         tokens.pop(0)
        #         from_node = QueryTree(node_type="SET", val=[])
        #         while tokens and tokens[0].upper() != "WHERE":
        #             table = tokens.pop(0).rstrip(',')
        #             from_node.val.append(table)
                    
        #             if tokens and tokens[0] == ',':
        #                 tokens.pop(0)
        #         root.children.append(from_node)
                
        #         if tokens and tokens[0].upper() == "WHERE":
        #             where_node = self.__createQueryTree(tokens)  # Recursive call for WHERE
        #         if where_node:
        #             where_node.parent = root
        #             root.children.append(where_node)  
                
        # elif token == "BEGIN":
        #     if tokens and tokens.pop(0).upper() == "TRANSACTION":
        #         root = QueryTree(node_type="BEGIN_TRANSACTION", val=[])
            
        #     if tokens : # if there is any token, add it as unknown
        #         root.children.append(QueryTree(node_type="UNKNOWN", val=[tokens]))
                
        # elif token == "COMMIT":
        #     root = QueryTree(node_type="COMMIT", val=[])
            
        #     if tokens : # if there is any token, add it as unknown
        #         root.children.append(QueryTree(node_type="UNKNOWN", val=[tokens]))
                
        # else:
        #     root = QueryTree(node_type="UNKNOWN", val=[token])

            return root
        except:
            return Exception("Oops! Something went wrong.")

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
