from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from StorageManager.objects.DataRetrieval import DataRetrieval, Condition
from StorageManager.objects.JoinCondition import JoinCondition
from StorageManager.objects.JoinOperation import JoinOperation
from StorageManager.objects.DataWrite import DataWrite
# from FailureRecovery.FailureRecovery import FailureRecovery
from Interface.Rows import Rows
from Interface.Action import Action
from typing import List
from StorageManager.StorageManager import StorageManager
from datetime import datetime
from Interface.ExecutionResult import ExecutionResult


class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initializes QueryProcessor with a ConcurrentControlManager instance.
        """
        if not hasattr(self, "initialized"):  # Ensure __init__ is called only once
            self.concurrent_manager = ConcurrentControlManager()
            self.optimization_engine = OptimizationEngine()
            # self.failure_recovery = FailureRecovery()
            self.storage_manager = StorageManager()
            self.initialized = True

    def remove_aliases(self, query: str) -> str:
        tokens = query.split()
        alias_map = {}
        i = 0

        while i < len(tokens):
            if tokens[i].upper() == 'AS':
                alias = tokens[i + 1]
                original = tokens[i - 1]
                alias_map[alias] = original
                tokens.pop(i)  # remove 'AS'
                tokens.pop(i)  # remove alias
                i -= 1
            i += 1

        # replace aliases
        for j in range(len(tokens)):
            if tokens[j] in alias_map:
                tokens[j] = alias_map[tokens[j]]

        return ' '.join(tokens)

    def execute_query(self, query: List[str]) -> List:
        results = []

        # BEGIN OPTIMIZING
        optimized_query = []
        for q in query:
            query_without_aliases = self.remove_aliases(q)
            optimized_query.append(
                self.optimization_engine.optimizeQuery(self.optimization_engine.parseQuery(q)))
                # self.optimization_engine.optimizeQuery(self.optimization_engine.parseQuery(query_without_aliases)))
        # END OPTIMIZING
        print("optimized querynya adalah")
        for q in optimized_query:
            print(q.query_tree)

        # BEGIN CONCURRENCY CONTROL
        # Get transaction ID
        transaction_id = self.concurrent_manager.beginTransaction()
        print(f"Transaction ID: {transaction_id}")

        # Generate Rows object from optimized query
        rows = self.generate_rows_from_query_tree(optimized_query, transaction_id)
        print(rows.data)

        # validate and Log the transaction
        for row_data in rows.data:
            # Create a Rows object with a single row of data
            single_row = Rows([row_data])

            # Create an Action object based on the first character of the row string
            row_string = single_row.data[0]
            action_type = "read" if row_string[0] == 'R' else "write"
            action = Action([action_type])
            self.concurrent_manager.logObject(single_row, transaction_id)
            validate = self.concurrent_manager.validateObject(single_row, transaction_id, action)
            print(f"Validation result: {validate.allowed}")
            if validate.allowed:
                # TODO : Implement Query Execution to Storage Manager Here

                # Log the single row
                print(f"Logging single-row: {single_row.data}")


                # Send data to FailureRecovery
                # self.send_to_failure_recovery(transaction_id, row_string, action_type, single_row)

            else:
                # Abort the transaction (when validation fails, concurrent control manager abort the transaction)
                break
        # END CONCURRENCY CONTROL

        # BEGIN STORAGE MANAGER
        try:
            for query in optimized_query:
                query_tree = query.query_tree
                print("DIA MASUK KESINI")
                print(query_tree.node_type)
                self.data_retrievals_to_results(query_tree)
                # print("join operations")
                # jo = self.get_join_operations(query_tree)
                # print(jo)
                # print("loligaging")
            
                if query_tree.node_type == "SELECT":
                    old_rows = self.storage_manager.query_tree_to_data_retrieval(query_tree)
                    result_rows = self.storage_manager.readBlock(old_rows)
                    print("query tree ", query_tree)
                    print("old rows ", old_rows)
                    print("result rows ", result_rows)
                    result = ExecutionResult(
                        transaction_id = transaction_id,
                        timestamp=datetime.now(),
                        message="Query executed successfully",
                        data_before=result_rows,
                        data_after=result_rows,
                        query=query.query # udah string kan harusnya
                    )

                elif query_tree.node_type == "UPDATE":
                    old_rows = self.storage_manager.query_tree_to_data_retrieval(query_tree)
                    print("query tree ", query_tree)
                    result_rows = self.storage_manager.writeBlock(old_rows)
                    print("old rows ", old_rows)
                    print("result rows ", result_rows)
                    result = ExecutionResult(
                        transaction_id = transaction_id,
                        timestamp=datetime.now(),
                        message="Query executed successfully",
                        data_before=old_rows,
                        data_after=result_rows,
                        query=query.query # udah string kan harusnya
                    )

                # masih bingung isi data_beforenya gimana
                # elif query_tree.node_type == "DELETE":
                #     old_rows = self.storage_manager.__query_tree_to_data_retrieval(query_tree)
                #     result_rows = self.storage_manager.deleteBlock(old_rows)
                #     print("query tree ", query_tree)
                #     print("old rows ", old_rows)
                #     print("result rows ", result_rows)
                #     result = ExecutionResult(
                #         transaction_id = transaction_id,
                #         timestamp=datetime.now(),
                #         message="Query executed successfully",
                #         data_before=old_rows,
                #         data_after=None,
                #         query=query.query # udah string kan harusnya
                #     )

                results.append(result)
                print("hasilnya adalah ", result.data_after)
        
            self.concurrent_manager.endTransaction(transaction_id)

            return results

        except Exception as e:
            # TODO: ini harusnya ada abort ato rollback
            return results

    def send_to_failure_recovery(self, transaction_id: int, row_string: str, action_type: str, rows: List[str]):
        """
        Send data to FailureRecovery to store it into the buffer.
        """
        execution_result = ExecutionResult(
            transaction_id=transaction_id,
            timestamp=datetime.now(),
            query=row_string,
            message=f"{action_type.capitalize()} action executed successfully", 
            data_after=rows.data,
            data_before=rows.data   
        )
        self.failure_recovery.write_log(execution_result)

    def generate_rows_from_query_tree(self, optimized_query: List, transaction_id: int) -> Rows:
        """
        Generate Rows data from a list of optimized query objects based on transaction_id.

        Args:
            optimized_query (List): List of optimized queries. Each may contain a QueryTree object.
            transaction_id (int): Transaction ID obtained from beginTransaction.

        Returns:
            Rows[str]: A Rows object filled with strings representing read/write operations.
        """
        operations = []

        for optimized_item in optimized_query:
            # Extract the QueryTree object from the optimized item
            query_tree = getattr(optimized_item, 'query_tree', optimized_item)

            # Process SELECT (READ)
            if query_tree.node_type == "SELECT":
                for child in query_tree.children:
                    if child.node_type == "FROM":
                        table_name = child.val[0]
                        operations.append(f"R{transaction_id}({table_name})")

            # Process UPDATE (WRITE)
            if query_tree.node_type == "UPDATE":
                table_name = query_tree.val[0]
                operations.append(f"W{transaction_id}({table_name})")

        # Create and return Rows object
        return Rows(operations)
    
    def get_table_and_condition(self, qt: QueryTree, tables: list, conditions: list):
        for child in qt.children:
            print("node type nya", child.node_type)
            if child.node_type == "WHERE":
                conditions.append(child.val)
            elif child.node_type == "Value1" or child.node_type == "Value2":
                tables.append(child.val[0])
            self.get_table_and_condition(child, tables, conditions)

    def query_tree_to_data_retrievals(self, qt: QueryTree):
        tables = []
        conditions = []
        self.get_table_and_condition(qt, tables, conditions)
        print("tablenya", tables)
        print("conditionnya", conditions)

        columns = {}
        for table in tables:
            temp = []
            for key, value in self.storage_manager.readBlock(DataRetrieval(table=[table], column=[], conditions=[]))[0].items():
                temp.append(key)
            columns[table] = temp
            temp = []

        print("columns", columns)

        conditions_objects = []

        for condition in conditions:
            if 'or' not in [c.lower() for c in condition]:
                if condition == conditions[0]:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector=None))
                else:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector="AND"))
            else:
                if condition == conditions[0]:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector=None))
                    conditions_objects.append(Condition(column=condition[4], operation=condition[5], operand=condition[6], connector="OR"))
                else:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector="AND"))
                    conditions_objects.append(Condition(column=condition[4], operation=condition[5], operand=condition[6], connector="OR"))

        print("objek kondisi", conditions_objects)

        list_of_data_retrievals = []

        for table in tables:
            condition_object_temp = []
            for condition in conditions_objects:
                if condition.column in columns[table]:
                    condition_object_temp.append(condition)
            list_of_data_retrievals.append(DataRetrieval(table=[table], column=[], conditions=condition_object_temp))
        
        return list_of_data_retrievals, tables

    def query_tree_to_data_writes(self, qt: QueryTree):
        tables = []
        conditions = []
        self.get_table_and_condition(qt, tables, conditions)
        
        # For write operations, we only support single table operations
        selected_table = tables[0] if tables else None
        
        # Get columns from the table
        columns = []
        if selected_table:
            for key, _ in self.storage_manager.readBlock(DataRetrieval(table=[selected_table], column=[], conditions=[]))[0].items():
                columns.append(key)
        
        # Process conditions
        conditions_objects = []
        for condition in conditions:
            if 'or' not in [c.lower() for c in condition]:
                if condition == conditions[0]:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector=None))
                else:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector="AND"))
            else:
                if condition == conditions[0]:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector=None))
                    conditions_objects.append(Condition(column=condition[4], operation=condition[5], operand=condition[6], connector="OR"))
                else:
                    conditions_objects.append(Condition(column=condition[0], operation=condition[1], operand=condition[2], connector="AND"))
                    conditions_objects.append(Condition(column=condition[4], operation=condition[5], operand=condition[6], connector="OR"))
        
        # Create DataWrite object based on operation type
        if qt.node_type == "INSERT":
            return DataWrite(
                overwrite=False,
                selected_table=selected_table,
                column=columns,
                conditions=[],
                new_value=qt.val if hasattr(qt, 'val') else None
            )
        elif qt.node_type == "UPDATE":
            # For UPDATE, we expect val to contain [column_name, new_value] pairs
            update_columns = []
            update_values = []
            if hasattr(qt, 'val') and qt.val:
                for col, val in qt.val:
                    update_columns.append(col)
                    update_values.append(val)
            
            return DataWrite(
                overwrite=True,
                selected_table=selected_table,
                column=update_columns if update_columns else columns,
                conditions=conditions_objects,
                new_value=update_values if update_values else None
            )
        
        return None

    def get_join_operations(self, qt: QueryTree):
        print("iterating", qt.node_type)
        print("qt nya", qt)
        if qt.node_type == "Value1" or qt.node_type == "Value2":
            print("masuk 1")
            print("return", qt.val[0])
            return qt.val[0]
        if qt.node_type == "JOIN": # cross join 
            print("masuk 2")
            return JoinOperation([self.get_join_operations(qt.children[0]), self.get_join_operations(qt.children[1])], JoinCondition("CROSS", []))
        elif qt.node_type == "TJOIN" and len(qt.children) == 2 and len(qt.children) == 0: # natural join
            print("masuk 3")
            return JoinOperation([self.get_join_operations(qt.children[0]), self.get_join_operations(qt.children[1])], JoinCondition("NATURAL", []))
        elif qt.node_type == "TJOIN":
            cond = [qt.val]
            val1 = str()
            val2 = str()
            new_tree = False
            for child in qt.children:
                if child.node_type == "Value1":
                    val1 = child.val[0]
                elif child.node_type == "Value2":
                    val2 = child.val[0]
                elif child.node_type == "TJOIN" and len(child.children) == 0:
                    print("condnya sblm", cond)
                    cond.append(child.val)
                    print("condnya sesudah", cond)
                elif "JOIN" in child.node_type:
                    new_tree = child
            print("val 1", val1)
            print("val 2", val2)
            if val1 and val2:
                print("masuk 1.1")
                print(val1)
                print(val2)
                print(cond)
                return JoinOperation([val1, val2], JoinCondition("ON", cond))
            elif val1:
                print("masuk 1.2")
                return JoinOperation([val1, self.get_join_operations(new_tree)], JoinCondition("ON", cond))
            elif val2:
                print("masuk 1.3")
                return JoinOperation([self.get_join_operations(new_tree), val2], JoinCondition("ON", cond))
        else:
            print("masuk 4")
            return self.get_join_operations(qt.children[0])

    
    def data_retrievals_to_results(self, qt: QueryTree):
        list_of_data_retrievals, tables = self.query_tree_to_data_retrievals(qt)
        results = {}
        for data_retrieval in list_of_data_retrievals:
            results[data_retrieval.table[0]] = self.storage_manager.readBlock(data_retrieval)

        for table in tables:
            print("hasil akhirnya", results[table])

    