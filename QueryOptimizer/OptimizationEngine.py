from ParsedQuery import ParsedQuery
from QueryTree import QueryTree
import re
from constants import LEGAL_COMMANDS_AFTER_WHERE,LEGAL_COMPARATORS, LEGAL_COMMANDS_AFTER_UPDATE, LEGAL_COMMANDS_AFTER_SET
from helpers import isAlphanumericWithQuotesAndUnderscoreAndDots
from CustomException import CustomException
class OptimizationEngine:
    def __init__(self):
        self.statistics = {}  # Example: Holds table statistics for cost estimation
        self.one_node_constraint = [] # If there is one-token-only constraint used, add it here

    def parseQuery(self, query: str) -> ParsedQuery:
        """
        Parses the given SQL query string into a ParsedQuery object.
        """
        # Tokenize and construct a basic QueryTree for demonstration purposes
        tokens = re.findall(r'[^\s,]+|,', query)
        print('tokens:',tokens)

        # Validate the first token of the query
        if (not self.validateFirstToken(tokens)):
            raise CustomException("Invalid first token", code=400)
        
        # Reset the one-node constraint list
        self.one_node_constraint = []
        
        # Create a QueryTree from the tokens
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
    
    def validateFirstToken(self, tokens: list) -> bool:
        """
        Validates the first token of the query.
        """
        if tokens[0].upper() not in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "BEGIN", "COMMIT"]:
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

        token = tokens.pop(0).upper()

        # Check One-Node-Only constraint
        if token in self.one_node_constraint:
            raise CustomException(f"Syntax Error: Only one '{token}' allowed", code=400)

        root = QueryTree(node_type=token, val=[])
        
        if token == "SELECT":
            self.one_node_constraint.append("SELECT")

            # Create SELECT node
            root = QueryTree(node_type="SELECT", val=[])
            
            if (tokens and tokens[0].upper() in [",", "NATURAL", "JOIN"]) or not tokens:
                raise SyntaxError("Syntax Error: Missing attribute name")
            
            root.val.append(tokens.pop(0))
            
            while tokens and tokens[0].upper() == ",":
                tokens.pop(0)
                if tokens and (tokens[0].upper()) not in ["FROM", "NATURAL", "JOIN",",", "WHERE", "LIMIT", "ORDER"]:
                    root.val.append(tokens.pop(0))
                else:
                    raise SyntaxError("Syntax Error: Missing attribute name")
            
            if tokens and (tokens[0].upper()) == "FROM":
                child = self.__createQueryTree(tokens)
                if child is not None: 
                    root.children.append(child)
            else:
                raise CustomException("Invalid command after SELECT clause", code=400)
            
                
        elif token == "FROM":
            # Check if table name doesn't exist
            if (tokens and tokens[0].upper() in [",", "NATURAL", "JOIN"]) or not tokens:
                raise SyntaxError("Syntax Error: Missing table name")
            
            root.val.append(tokens.pop(0))
               
            # Check if any join operation is present
            if (tokens and tokens[0].upper() in [",", "JOIN", "NATURAL"]):
            
                parent = root
                child = root
                grand = root
                
                while True:
                    parent = child
                    child = QueryTree(node_type="", val=[])
                    
                    # Change root because of join operation
                    if parent.node_type == "FROM":
                            root = child
                    
                    # Check if join operation with syntax ','
                    if tokens and tokens[0].upper() == ',':
                        tokens.pop(0)
                        child.node_type = "JOIN"
                        
                        if not tokens:
                            raise SyntaxError("Syntax Error: Missing table name")

                        if tokens and tokens[0].upper() in ["NATURAL", "JOIN",",", "WHERE", "LIMIT", "ORDER"]:
                            raise SyntaxError("Syntax Error: Missing table name")
                        
                        if parent.node_type == "FROM":
                            childOne = QueryTree(node_type="Value1", val=[parent.val[0]])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            
                        # Change root if TJOIN is parent
                        if parent.node_type == "TJOIN":
                            childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                            child.children.append(parent)
                            child.children.append(childTwo)
                            
                            if grand.node_type == "FROM":
                                root = child
                            else:
                                grand.children.pop()
                                grand.children.append(child)
                            
                        else:
                            childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                            child.children.append(childOne)
                            child.children.append(childTwo)
                            
                            if parent.children != []:
                                parent.children.pop()
                                
                            parent.children.append(child)
                            
                    # Check if join operation with syntax 'JOIN'
                    elif tokens and tokens[0].upper() == 'JOIN':
                        tokens.pop(0)
                        child.node_type = "TJOIN"
                        
                        if not tokens or (tokens[0].upper() in [",", "NATURAL", "JOIN", "WHERE", "LIMIT", "ORDER"]):
                            raise SyntaxError("Syntax Error: Missing table name")
                        
                        if parent.node_type == "FROM":
                            childOne = QueryTree(node_type="Value1", val=[parent.val[0]])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            
                        childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                        
                        if tokens and tokens[0].upper() != "ON":
                            raise SyntaxError("Syntax Error: Missing ON")
                        
                        tokens.pop(0)
                        
                        isCheckCondition = True
                        newRoot = child
                        
                        # Check condition for join operation
                        while isCheckCondition:
                            if (tokens and tokens[0].upper() in ['=', '!=', '>', '<', '>=', '<=', 'OR', 'AND', ","]) or not tokens:
                                raise SyntaxError("Syntax Error: Missing condition")
                            
                            newRoot.val.append(tokens.pop(0))
                            
                            if (tokens and tokens[0].upper() not in ['=', '!=']) or not tokens:
                                raise SyntaxError("Syntax Error: Missing condition")
                            
                            newRoot.val.append(tokens.pop(0))
                            
                            if (tokens and tokens[0].upper() in ['=', '!=', '>', '<', '>=', '<=', 'OR', 'AND', ","]) or not tokens:
                                raise SyntaxError("Syntax Error: Missing condition")
                            
                            newRoot.val.append(tokens.pop(0))
                            
                            if (tokens and tokens[0].upper() not in ['AND', 'OR']) or not tokens:
                                isCheckCondition = False
                                
                            elif (tokens[0].upper() ==  'AND'):
                                oldRoot = newRoot
                                newRoot = QueryTree(node_type="TJOIN", val=[])
                                oldRoot.children.append(newRoot)
                                tokens.pop(0)
                                
                            elif (tokens[0].upper() ==  'OR'):
                                newRoot.val.append(tokens.pop(0))
                        
                        child.children.append(childOne)
                        child.children.append(childTwo)
                        
                        
                        if parent.children != []:
                            parent.children.pop()
                        
                        parent.children.append(child)
                        
                    # Check if join operation with syntax 'NATURAL JOIN'
                    elif tokens and (tokens[0].upper() == 'NATURAL' and tokens[1].upper() == "JOIN"):
                        tokens.pop(0)
                        tokens.pop(0)
                        child.node_type = "TJOIN"
                        
                        if not tokens or (tokens[0].upper() in [",", "NATURAL", "JOIN", "WHERE", "LIMIT", "ORDER"]):
                            raise SyntaxError("Syntax Error: Missing table name")
                        
                        if parent.node_type == "FROM":
                            childOne = QueryTree(node_type="Value1", val=[parent.val[0]])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            
                        childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                        child.children.append(childOne)
                        child.children.append(childTwo)
                        
                        
                        if parent.children != []:
                            parent.children.pop()
                        
                        parent.children.append(child)
                        
                    # Check token is not a join operation
                    else:
                        break
                    
                    grand = parent
                                  
            if not tokens:
                return root
            else:
                if tokens[0].upper() not in ["WHERE", "LIMIT", "ORDER"]:
                    raise SyntaxError("Syntax Error: Invalid syntax")
                child = self.__createQueryTree(tokens)
                if child is not None:  # Only append if the child is not None
                    root.children.append(child)  

        elif token == "AND":
            root = QueryTree(node_type="WHERE", val=[])
            condition = []
            if(not tokens):
                raise CustomException("Incomplete syntax for WHERE clause", code=400)
            
            if(not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[0].strip("'")) or not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[2].strip("'"))):
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
            self.one_node_constraint.append("WHERE")

            root = QueryTree(node_type="WHERE", val=[])
            condition = []

            if(not tokens):
                raise CustomException("Incomplete syntax for WHERE clause", code=400)
            
            if(not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[0].strip("'")) or not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[2].strip("'"))):
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
            self.one_node_constraint.append("ORDER")

            if not tokens:
                raise CustomException("Incomplete syntax for ORDER BY clause", code=400)
            
            if tokens and tokens.pop(0).upper() != "BY":
                raise CustomException("Invalid syntax for ORDER BY clause", code=400)

            root = QueryTree(node_type="SORT", val=[])
            
            while tokens and tokens[0].upper() not in ["LIMIT"]:
                col = tokens.pop(0)
                root.val.append(col)
                if tokens[0] == ",":
                    tokens.pop(0)
                    if tokens and tokens[0].upper() in ["LIMIT", ","]:
                        raise CustomException("Invalid syntax for ORDER BY clause", code=400)
                    elif not tokens:
                        raise CustomException("Incomplete syntax for ORDER BY clause", code=400)
                
            if not root.val:
                raise CustomException("Invalid syntax for ORDER BY clause", code=400)
            
            if tokens and tokens[0].upper() not in ["LIMIT"]:
                raise CustomException("Invalid command after ORDER BY clause", code=400)
                
            if tokens:
                child = self.__createQueryTree(tokens)
                if child is not None:
                    root.children.append(child)

        elif token == "LIMIT":
            self.one_node_constraint.append("LIMIT")

            if not tokens:
                raise CustomException("Incomplete syntax for LIMIT clause", code=400)

            if tokens and not tokens[0].isdigit():
                raise CustomException("Invalid syntax for LIMIT clause", code=400)
            
            if tokens and int(tokens[0]) < 0:
                raise CustomException("LIMIT value must be greater than or equal to 0", code=400)
            
            root = QueryTree(node_type="LIMIT", val=[tokens.pop(0)])

            if tokens:
                raise CustomException("Invalid command after LIMIT clause", code=400)
        
        elif token == "SET":
            root = QueryTree(node_type="SET", val=[])

            if(not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[0].strip("'")) or not isAlphanumericWithQuotesAndUnderscoreAndDots(tokens[2].strip("'"))):
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
            self.one_node_constraint.append("UPDATE")

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
            else:
                raise CustomException("Invalid syntax: 'BEGIN' must be followed by 'TRANSACTION'", code=400)
            
            if "BEGIN_TRANSACTION" in self.one_node_constraint:
                raise CustomException("Syntax Error: Only one 'BEGIN TRANSACTION' allowed", code=400)
            
            if tokens:
                raise CustomException("Invalid syntax: 'BEGIN TRANSACTION' must not be followed by any tokens", code=400)

            self.one_node_constraint.append("BEGIN_TRANSACTION")
            root = QueryTree(node_type="BEGIN_TRANSACTION", val=[])
                
        elif token == "COMMIT":
            if "COMMIT" in self.one_node_constraint:
                raise CustomException("Syntax Error: Only one 'COMMIT' allowed", code=400)
            
            if tokens: 
                raise CustomException("Invalid syntax: 'COMMIT' must not be followed by any tokens", code=400)

            self.one_node_constraint.append("COMMIT")
            root = QueryTree(node_type="COMMIT", val=[])

        elif token == "DELETE":
            self.one_node_constraint.append("DELETE")

            root = QueryTree(node_type="DELETE", val=[])
            
            if tokens and tokens[0].upper() != "FROM":
                raise CustomException("Invalid command after DELETE clause", code=400)
            tokens.pop(0)

            if not tokens:
                raise CustomException("Incomplete syntax for DELETE clause", code=400)
            
            root.val.append(tokens.pop(0))
            
            if tokens and tokens[0].upper() != "WHERE":
                raise CustomException("Invalid command after DELETE clause", code=400)
            
            child = self.__createQueryTree(tokens)
            if child is not None:
                root.children.append(child)

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
