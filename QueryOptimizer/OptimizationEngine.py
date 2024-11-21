from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.QueryTree import QueryTree
import re
from QueryOptimizer.constants import LEGAL_COMMANDS_AFTER_WHERE,LEGAL_COMPARATORS, LEGAL_COMMANDS_AFTER_UPDATE, LEGAL_COMMANDS_AFTER_SET
from QueryOptimizer.helpers import isAlphanumericWithQuotes
from QueryOptimizer.CustomException import CustomException
class OptimizationEngine:
    def __init__(self):
        self.statistics = {}  # Example: Holds table statistics for cost estimation

    def parseQuery(self, query: str) -> ParsedQuery:
        """
        Parses the given SQL query string into a ParsedQuery object.
        """
        # Tokenize and construct a basic QueryTree for demonstration purposes
        tokens = re.findall(r'[^\s,]+|,', query)
        print('tokens:',tokens)
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
                token = tokens.pop(0)
                if not token.endswith(',') and (tokens[0] != ','):
                    root.val.append(token.rstrip(','))  # Remove trailing commas
                    break
                
                root.val.append(token.rstrip(','))  # Remove trailing commas
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "FROM":
            root = QueryTree(node_type="FROM", val=[])
            
            # Get all tables until we hit WHERE or end of tokens
            while tokens and tokens[0].upper() not in ["WHERE", "ORDER", "GROUP", "HAVING", "LIMIT"]:
                table = tokens.pop(0).rstrip(',')
                root.val.append(table)
                
                # Skip any commas
                if tokens and tokens[0] == ',':
                    tokens.pop(0)
            
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "AND":
            root = QueryTree(node_type="WHERE", val=[])
            condition = []
            if(not tokens):
                raise CustomException("Incomplete syntax for WHERE clause", code=400)
            
            if(not isAlphanumericWithQuotes(tokens[0].strip("'")) or not isAlphanumericWithQuotes(tokens[2].strip("'"))):
                raise CustomException("Invalid syntax for WHERE clause", code=400)
            
            if(not tokens[1] in LEGAL_COMPARATORS):
                raise CustomException("Invalid comparator in WHERE clause", code=400)
            
            while True:
                condition.append(tokens.pop(0))
                condition.append(tokens.pop(0))
                condition.append(tokens.pop(0))
                if(not tokens or tokens[0] != "OR"):
                    break
                else:
                    condition.append(tokens.pop(0))
            root.val = condition
            if(tokens and tokens[0] not in LEGAL_COMMANDS_AFTER_WHERE):
                raise CustomException("Invalid command after WHERE clause", code=400)
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  
            
        elif token == "WHERE":
            root = QueryTree(node_type="WHERE", val=[])
            condition = []

            if(not tokens):
                raise CustomException("Incomplete syntax for WHERE clause", code=400)
            
            if(not isAlphanumericWithQuotes(tokens[0].strip("'")) or not isAlphanumericWithQuotes(tokens[2].strip("'"))):
                raise CustomException("Invalid syntax for WHERE clause", code=400)
            
            if(not tokens[1] in LEGAL_COMPARATORS):
                raise CustomException("Invalid comparator in WHERE clause", code=400)
            
            while True:
                condition.append(tokens.pop(0))
                condition.append(tokens.pop(0))
                condition.append(tokens.pop(0))
                if(not tokens or tokens[0] != "OR"):
                    break
                else:
                    condition.append(tokens.pop(0))
            root.val = condition
            if(tokens and tokens[0] not in LEGAL_COMMANDS_AFTER_WHERE):
                raise CustomException("Invalid command after WHERE clause", code=400)
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "ORDER":
            if tokens and tokens.pop(0).upper() == "BY":
                root = QueryTree(node_type="ORDER_BY", val=[])
                
                while tokens and tokens[0].upper() not in ["LIMIT"]:
                    col = tokens.pop(0).rstrip(',')
                    root.val.append(col)
                    

        elif token == "LIMIT":
            root = QueryTree(node_type="LIMIT", val=[tokens.pop(0)] if tokens else [])
        
        elif token == "SET":
            root = QueryTree(node_type="SET", val=[])

            if(not isAlphanumericWithQuotes(tokens[0].strip("'")) or not isAlphanumericWithQuotes(tokens[2].strip("'"))):
                raise CustomException("Invalid syntax for SET clause", code=400)
            
            if(not tokens[1] in LEGAL_COMPARATORS):
                raise CustomException("Invalid comparator in SET clause", code=400)
            values = []
            while True:
                values.append(tokens.pop(0))
                values.append(tokens.pop(0))
                values.append(tokens.pop(0))
                if(not tokens or tokens[0] != ","):
                    break
                else:
                    values.append(tokens.pop(0))

            root.val = values
            
            if(tokens[0] not in LEGAL_COMMANDS_AFTER_SET):
                raise CustomException("Invalid command after SET clause", code=400)
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "UPDATE":
            if(not tokens):
                raise CustomException("Incomplete syntax for UPDATE clause", code=400)
            
            root = QueryTree(node_type="UPDATE", val=[tokens.pop(0)])
            
            if(tokens[0] not in LEGAL_COMMANDS_AFTER_UPDATE):
                raise CustomException("Invalid command after UPDATE clause", code=400)    
            
            child = self.__createQueryTree(tokens)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "BEGIN":
            if tokens and tokens.pop(0).upper() == "TRANSACTION":
                root = QueryTree(node_type="BEGIN_TRANSACTION", val=[])
            
            if tokens : # if there is any token, add it as unknown
                root.children.append(QueryTree(node_type="UNKNOWN", val=[tokens]))
                
        elif token == "COMMIT":
            root = QueryTree(node_type="COMMIT", val=[])
            
            if tokens : # if there is any token, add it as unknown
                root.children.append(QueryTree(node_type="UNKNOWN", val=[tokens]))
                
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
