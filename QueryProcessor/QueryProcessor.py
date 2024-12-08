from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from StorageManager.objects.DataRetrieval import DataRetrieval, Condition
from StorageManager.objects.JoinCondition import JoinCondition
from StorageManager.objects.JoinOperation import JoinOperation
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

            validate = self.concurrent_manager.validateObject(single_row, transaction_id, action)
            print(f"Validation result: {validate.allowed}")
            if validate.allowed:
                # TODO : Implement Query Execution to Storage Manager Here

                # Log the single row
                print(f"Logging single-row: {single_row.data}")
                self.concurrent_manager.logObject(single_row, transaction_id)

                # Send data to FailureRecovery
                self.send_to_failure_recovery(transaction_id, row_string, action_type, single_row)

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
            data=rows,  
            rows=rows.data   
        )
        self.failure_recovery.writeLog(execution_result)

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
    
    # def get_join_operations(self, qt: QueryTree, jo: list[JoinOperation]):
    #     if ("JOIN" in qt.node_type):

    #     for child in qt.children:
    #         if "JOIN" in child.node

    
    def data_retrievals_to_results(self, qt: QueryTree):
        list_of_data_retrievals, tables = self.query_tree_to_data_retrievals(qt)
        results = {}
        for data_retrieval in list_of_data_retrievals:
            results[data_retrieval.table[0]] = self.storage_manager.readBlock(data_retrieval)

        for table in tables:
            print("hasil akhirnya", results[table])

    