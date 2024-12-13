from QueryOptimizer.ParsedQuery import ParsedQuery
from QueryOptimizer.QueryTree import QueryTree
import re
from QueryOptimizer.constants import LEGAL_COMMANDS_AFTER_WHERE,LEGAL_COMPARATORS, LEGAL_COMMANDS_AFTER_UPDATE, LEGAL_COMMANDS_AFTER_SET
from QueryOptimizer.helpers import isAlphanumericWithQuotesAndUnderscoreAndDots
from QueryOptimizer.CustomException import CustomException
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.Statistics import Statistics
from QueryOptimizer.whereOptimize import optimizeWhere
from QueryOptimizer.sortLimitOptimize import optimizeSortLimit
from QueryOptimizer.rule8Optimize import rule8
from QueryOptimizer.rule8Optimize import reverseQueryTree
from QueryOptimizer.rule8Optimize import reverseQueryTree

class OptimizationEngine:
    def __init__(self):
        self.statistics = {}  # Example: Holds table statistics for cost estimation
        self.one_node_constraint = [] # If there is one-token-only constraint used, add it here
        self.storage_manager = StorageManager()

    def parseQuery(self, query: str, statistics) -> ParsedQuery:
        """
        Parses the given SQL query string into a ParsedQuery object.
        """
        tokens = []
        current_token = ''
        in_quote = False
        quote_char = None
        
        # Iterate through each character
        for char in query:
            # Handle quotes (both single and double)
            if char in ["'", '"'] and not in_quote:
                in_quote = True
                quote_char = char
                current_token = char
                continue
            elif char == quote_char and in_quote:
                in_quote = False
                current_token += char
                tokens.append(current_token)
                current_token = ''
                continue
            
            # If we're inside quotes, keep building the token
            if in_quote:
                current_token += char
                continue
                
            # Handle spaces when not in quotes
            if char.isspace():
                if current_token in ['>','<']:
                    tokens.append(current_token)
                    current_token = ''
                if current_token:
                    tokens.append(current_token)
                    current_token = ''
                continue
            
            if char in ['>','<']:
                if current_token:
                    current_token = char
                
            # Handle special characters
            if char in ['=',',']:
                if current_token in ['>','<']:
                    current_token += char
                    tokens.append(current_token)
                    current_token = ''
                elif current_token:
                    tokens.append(current_token)
                    current_token = ''
                    tokens.append(char)
                else:
                    tokens.append(char)
                continue
                
            # Build token
            current_token += char
        
        # Add any remaining token
        if current_token:
            tokens.append(current_token)

        # Validate the first token of the query
        if (not self.validateFirstToken(tokens)):
            raise CustomException("Invalid first token", code=400)
        
        # Reset the one-node constraint list
        self.one_node_constraint = []
        
        # Create a QueryTree from the tokens
        root = self.__createQueryTree(tokens, statistics)

        # Return a ParsedQuery object
        return ParsedQuery(query=query, query_tree=root)
    

    
    def optimizeQuery(self, query: ParsedQuery, statistics) -> ParsedQuery:
        def extract_tables_from_tree(node):
            tables = set()
            def traverse(current_node):
                # Check for tables in Value1 or Value2 nodes
                if current_node.node_type in ["Value1", "Value2"]:
                    tables.add(current_node.val[0])

                # Recursively traverse children
                if hasattr(current_node, 'children'):
                    for child in current_node.children:
                        traverse(child)
            traverse(node)
            return tables
        def extract_conditions_from_tree( node):
            conditions = []
            def traverse(current_node):
                # Check for tables in Value1 or Value2 nodes
                if current_node.node_type in ["TJOIN"]:
                    conditions.append(current_node.val)
                    # current_node.val = None
                
                # Recursively traverse children
                if hasattr(current_node, 'children'):
                    for child in current_node.children:
                        traverse(child)
            
            traverse(node)
            return conditions
        def find_joined_tables(conditions, target_table):
            joined_tables = set()
            for condition in conditions:
                first_table = condition[0].split('.')[0]
                second_table = condition[2].split('.')[0]
                if first_table == target_table:
                    other_table = condition[2].split('.')[0]
                    joined_tables.add(other_table)
                elif second_table == target_table:
                    other_table = condition[0].split('.')[0]
                    joined_tables.add(other_table)
            return joined_tables

        def replace_join_children(node, table1, table2):
            node.children = [
                QueryTree("Value1", [table1]),
                QueryTree("Value2", [table2])
            ]

            return node
        
        def get_matching_conditions(conditions, table1, table2):
            result = []
            remaining_conditions = []
            for condition in conditions:
                # print("Condition", condition)
                table_names = {part.split('.')[0] for part in condition if '.' in part}
                # print("Table names", table_names)
                # print("Table1", table1)
                # print("Table2", table2)
                if table1 in table_names and table2 in table_names:
                    result.extend(condition)  
                else:
                    remaining_conditions.append(condition)  
            conditions.clear()
            conditions.extend(remaining_conditions)
            return result

        def optimize_recursive(current_tree, remaining_tables, remaining_conditions):
            has_join_child = any(child.node_type in ["JOIN", "TJOIN"] for child in current_tree.children)
            referenced_tables = list(set(all_tables) - set(remaining_tables))
            isReversed = bool(referenced_tables)
            # print('hey', current_tree)
            if (has_join_child):
                # print("Optimizing children...")
                # print( current_tree)
                current_tree.children = [
                    optimize_recursive(child, remaining_tables,remaining_conditions) if child.node_type in ["JOIN", "TJOIN"] else child
                    for child in current_tree.children
            ]
            if(current_tree.node_type == "TJOIN"):
                referenced_tables = list(set(all_tables) - set(remaining_tables))
                isReversed = referenced_tables != []
                # print("Optimizing join order...", current_tree)
                # print("list", list(set(all_tables) - set(remaining_tables)))
                # print("isreverse", isReversed)
                if(isReversed):
                    # print("Reversed")
                    joined_tables = set()
                    for table in referenced_tables:
                        joined_tables.update(find_joined_tables(remaining_conditions, table))
                    joined_tables = joined_tables & set(remaining_tables)
                    # print("Joined tables:", joined_tables)
                    smallest_table = joined_tables.pop()
                    smallest_joined_table = find_joined_tables(remaining_conditions, smallest_table)& set(referenced_tables)
                    smallest_joined_table = smallest_joined_table.pop()
                    # print("Smallest table:", smallest_table) 
                    # print("Smallest joined table:", smallest_joined_table) 
                    for child in current_tree.children:
                        if child.node_type == "Value1":
                            child.val = smallest_table        
                    current_tree.val = get_matching_conditions(remaining_conditions, smallest_table, smallest_joined_table)
                    remaining_tables.remove(smallest_table)
                else:
                    smallest_table = min(remaining_tables, key=lambda t: statistics.get(t, Statistics(0, 0, 0, 0, {})).n_r)
                    # print("Smallest table:", smallest_table)
                    joined_tables = find_joined_tables(remaining_conditions, smallest_table) & set(remaining_tables)
                    # print("Joined tables:", joined_tables)
                    if not joined_tables:
                        return current_tree
                    smallest_joined_table = min(joined_tables, key=lambda t: statistics.get(t, Statistics(0, 0, 0, 0, {})).n_r)
                    # print("Smallest joined table:", smallest_joined_table)
                    replace_join_children(current_tree, smallest_table, smallest_joined_table)
                    current_tree.val = get_matching_conditions(remaining_conditions, smallest_table, smallest_joined_table)
                    remaining_tables.remove(smallest_table)
                    remaining_tables.remove(smallest_joined_table)
            
            return current_tree
        
        def conjunctive_selection(node):
            if node.node_type == "SELECT" and "AND" in node.val:
                conditions = node.val.split("AND")
                return [QueryTree("SELECT", cond.strip()) for cond in conditions]
            return [node]

        def commutative_selection(node):
            if node.node_type == "SELECT" and hasattr(node, 'children'):
                node.children = sorted(node.children, key=lambda x: x.val)
            return node

        def last_projection(node):
            if node.node_type == "PROJECT" and hasattr(node, 'children'):
                last_projection = node.children[-1]
                return QueryTree("PROJECT", last_projection.val, [last_projection])
            return node
        
        query_tree = query.query_tree
        all_tables = extract_tables_from_tree(query_tree)
        remaining_conditions = extract_conditions_from_tree(query_tree)

        query_tree.children = [last_projection(commutative_selection(child)) for child in query_tree.children]
        query_tree.children = [grandchild for child in query_tree.children for grandchild in conjunctive_selection(child)]
        optimized_tree = optimize_recursive(query_tree, list(all_tables),remaining_conditions)
        # print("Optimized tree:", optimized_tree)
        return optimizeSortLimit(optimizeWhere(rule8(ParsedQuery(query.query, optimized_tree))))
    
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

    def __createQueryTree(self, tokens: list, statistics) -> QueryTree:
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
                child = self.__createQueryTree(tokens,statistics)
                if child is not None: 
                    root.children.append(child)
            else:
                raise CustomException("Invalid command after SELECT clause", code=400)
            
                
        elif token == "FROM":
            # Check if table name doesn't exist
            if (tokens and tokens[0].upper() in [",", "NATURAL", "JOIN"]) or not tokens:
                raise SyntaxError("Syntax Error: Missing table name")
            
            root.val.append(tokens.pop(0))
            table = []
            table.append(root.val[0])
            table = []
            table.append(root.val[0])
               
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
                            table.append(parent.val[0])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            table.append(parent.children[1].val[0])
                            
                        # Change root if TJOIN is parent
                        if parent.node_type == "TJOIN":
                            childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                            table.append(childTwo.val[0])
                            child.children.append(parent)
                            child.children.append(childTwo)
                            
                            if grand.node_type == "FROM":
                                root = child
                            else:
                                grand.children.pop()
                                grand.children.append(child)
                            
                        else:
                            childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                            table.append(childTwo.val[0])
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
                            table.append(parent.val[0])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            table.append(parent.children[1].val[0])
                            
                        childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                        table.append(childTwo.val[0])
                        
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
                       
                        seen = set()
                        uniqueTable = []

                        for item in table:
                            if item not in seen:
                                uniqueTable.append(item)
                                seen.add(item)
                        
                        if not tokens or (tokens[0].upper() in [",", "NATURAL", "JOIN", "WHERE", "LIMIT", "ORDER"]):
                            raise SyntaxError("Syntax Error: Missing table name")
                        
                        if parent.node_type == "FROM":
                            childOne = QueryTree(node_type="Value1", val=[parent.val[0]])
                        else:
                            childOne = QueryTree(node_type="Value1", val=[parent.children[1].val[0]])
                            
                        childTwo = QueryTree(node_type="Value2", val=[tokens.pop(0)])
                        child.children.append(childOne)
                        child.children.append(childTwo)
                        
                        table_columns = set(statistics[childTwo.val[0]].V_a_r.keys())

                        common_columns_all = []
                        table = list(set(table))
                        
                        for table_name in table:          
                            current_table_columns = set(statistics[table_name].V_a_r.keys())
                            common_columns = table_columns.intersection(current_table_columns)
                            
                            for column in common_columns:
                                common_columns_all.append(f"{childTwo.val[0]}.{column}")
                                common_columns_all.append("=")
                                common_columns_all.append(f"{table_name}.{column}")
                                    
                        common_columns_all = list((common_columns_all))
                        child.val = common_columns_all
                        
                        if parent.children != []:
                            parent.children.pop()
                        
                        parent.children.append(child)
                        
                    # Check token is not a join operation
                    else:
                        break
                    
                    grand = parent
                            
            if not tokens:
                # tree = reverseQueryTree(root)
                return root
            else:
                if tokens[0].upper() not in ["WHERE", "LIMIT", "ORDER"]:
                    raise SyntaxError("Syntax Error: Invalid syntax")
                child = self.__createQueryTree(tokens,statistics)
                if child is not None:  # Only append if the child is not None
                    # root = reverseQueryTree(root)
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
            child = self.__createQueryTree(tokens,statistics)
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
            child = self.__createQueryTree(tokens,statistics)
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
                child = self.__createQueryTree(tokens,statistics)
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
            child = self.__createQueryTree(tokens,statistics)
            if child is not None:  # Only append if the child is not None
                root.children.append(child)  

        elif token == "UPDATE":
            self.one_node_constraint.append("UPDATE")

            if(not tokens):
                raise CustomException("Incomplete syntax for UPDATE clause", code=400)
            
            root = QueryTree(node_type="UPDATE", val=[tokens.pop(0)])
            
            if(tokens[0] not in LEGAL_COMMANDS_AFTER_UPDATE):
                raise CustomException("Invalid command after UPDATE clause", code=400)    
            
            child = self.__createQueryTree(tokens,statistics)
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
            
            child = self.__createQueryTree(tokens,statistics)
            if child is not None:
                root.children.append(child)

        else:
            root = QueryTree(node_type="UNKNOWN", val=[token])
            
        return root

    def __applyHeuristicRules(self, query: ParsedQuery):
        if query.query_tree and query.query_tree.node_type == "SELECT":
            pass

    def __traverseAndEstimateCost(self, node: QueryTree) -> int:
        if not node:
            return 0

        base_cost = {
            "SELECT": 50,
            "FROM": 100,
            "WHERE": 30,
            "ORDER_BY": 70,
            "LIMIT": 10,
        }.get(node.node_type, 0)

        return base_cost + sum(self.__traverseAndEstimateCost(child) for child in node.children)
