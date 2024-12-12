
    


def optimizeQuery(self, query: ParsedQuery, statistics) -> ParsedQuery:
        """
        Optimize join order based on table sizes and join relationships
        
        :param query_tree: Root of the query tree
        :param table_sizes: Dictionary mapping table names to number of rows
        :return: Optimized query tree
        """
        remaining_tables = []
        remaining_conditions = []
        current_node = query.query_tree
        current_condition = None
        def extract_tables_from_tree(node):
            """
            Extract all unique table names from the query tree
            """
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
        def extract_conditions_from_tree(node):
            """
            Extract all unique table names from the query tree
            """
            conditions = []
            
            def traverse(current_node):
                # Check for tables in Value1 or Value2 nodes
                if current_node.node_type in ["TJOIN"]:
                    conditions.append(current_node.val)
                    current_node.val = None
                
                # Recursively traverse children
                if hasattr(current_node, 'children'):
                    for child in current_node.children:
                        traverse(child)
            
            traverse(node)
            return conditions
        def find_joined_tables(node, target_table):
            """
            Find tables directly joined with the target table
            """
            joined_tables = set()
            
            def find_joins(current_node):
                # Check if this is a TJOIN node with conditions
                if current_node.node_type == "TJOIN" and current_node.val:
                    # Split condition and look for table references
                    condition = current_node.val
                    table_ref = condition[0].split('.')[0]
                    if table_ref == target_table:
                        # Find other table in the join
                        print("Condition:", condition)
                        other_table = condition[2].split('.')[0]
                        joined_tables.add(other_table)
                # Recursively check children
                if hasattr(current_node, 'children'):
                    for child in current_node.children:
                        find_joins(child)
            
            find_joins(node)
            return joined_tables
        def replace_join_children(node, table1, table2):
            """
            Replace children of a TJOIN node with specific tables
            """
            def create_table_node(table_name):
                # Determine node type based on order of appearance
                node_type = "Value1" if table_name not in node.val[2:] else "Value2"
                return QueryTree(node_type, [table_name])
            
            node.children = [
                create_table_node(table1),
                create_table_node(table2)
            ]
            return node
        def find_and_remove_condition_with_tables(conditions, table1, table2):
            """
            Finds and removes the first condition that contains references to both specified table names.

            Args:
                conditions (list): List of conditions, each represented as a list.
                table1 (str): The name of the first table.
                table2 (str): The name of the second table.

            Returns:
                list: The extracted condition containing references to both table names, or None if not found.
            """
            for i, condition in enumerate(conditions):
                # Extract the tables from the condition
                tables_in_condition = set(
                    part.split('.')[0] for part in condition if '.' in part
                )
                # Check if both tables are present
                if table1 in tables_in_condition and table2 in tables_in_condition:
                    # Remove the condition from the list
                    return conditions.pop(i)
            return None
        
        def find_all_paired_tables(conditions, tableA):
            paired_tables = set()
            for condition in conditions:
                tables_in_condition = {part.split('.')[0] for part in condition if '.' in part}
                if tableA in tables_in_condition:
                    paired_tables.update(tables_in_condition - {tableA})
            return paired_tables
        
        def has_no_tjoin_child(node):
            for child in node.children:
                if child.node_type == "TJOIN":
                    return False
            return True
        
        remaining_tables = extract_tables_from_tree(query.query_tree)
        remaining_conditions = extract_conditions_from_tree(query.query_tree)
        while(remaining_conditions):
            def traverse(node):
                if(has_no_tjoin_child(node)):
                    print(node)
                    print('no tjoin child')
                    return node
                for child in node.children:
                    if(current_condition and child.val == current_condition):
                        print('found')
                        return child
                if hasattr(node, 'children'):
                    for child in node.children:
                        if(child.node_type == "TJOIN"):
                            return traverse(child)
            print("Complete node:", current_node)
            current_node = traverse(current_node)
            print("current_node:",current_node) 
            if(not remaining_tables):
                current_node.val = remaining_conditions[0]
                print("result", query.query_tree)
                return query
            print("remaining:",remaining_tables)
            print("remaining:",remaining_conditions)
            smallest_table = min(remaining_tables, key=lambda t: statistics.get(t, Statistics(0, 0, 0, 0, {})).n_r)
            print("Smallest table:", smallest_table)
            # Find tables joined with the smallest table
            joined_tables = find_all_paired_tables(remaining_conditions, smallest_table) & set(remaining_tables)
            print("Joined tables:", joined_tables)
            smallest_joined_table = min(joined_tables, key=lambda t: statistics.get(t, Statistics(0, 0, 0, 0, {})).n_r)
            print("Smallest joined table:", smallest_joined_table)
            replace_join_children(current_node, smallest_table, smallest_joined_table)
            new_condition = find_and_remove_condition_with_tables(remaining_conditions, smallest_table, smallest_joined_table)
            current_node.val = new_condition
            current_condition = new_condition
            print("Optimized tree:", current_node)
            remaining_tables.remove(smallest_table)
            remaining_tables.remove(smallest_joined_table)
        