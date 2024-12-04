from QueryOptimizer.OptimizationEngine import OptimizationEngine
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from FailureRecovery.FailureRecovery import FailureRecovery
from FailureRecovery.RecoverCriteria import RecoverCriteria
from Interface.Rows import Rows  
from Interface.Action import Action
from QueryOptimizer import QueryTree
from typing import List
from datetime import datetime
from StorageManager.StorageManager import StorageManager
from Interface.ExecutionResult import ExecutionResult
from Interface.Response import Response

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
            self.failure_recovery = FailureRecovery()
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

        # optimizing
        optimized_query = []
        for q in query:
            query_without_aliases = self.remove_aliases(q)
            optimized_query.append(self.optimization_engine.optimizeQuery(self.optimization_engine.parseQuery(query_without_aliases)))

        # concurrency control (validate and logging)
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
            print(f"Validation result: {validate.status}")
            if validate.status:
                #TODO : Implement Query Execution to Storage Manager Here

                # Log the single row
                print(f"Logging single-row: {single_row.data}")
                self.concurrent_manager.logObject(single_row, transaction_id)

                # Send data to FailureRecovery
                self.send_to_failure_recovery(transaction_id, row_string, action_type)
            else:
                #Abort the transaction (when validation fails, concurrent control manager abort the transaction)
                break

        return optimized_query

    def send_to_failure_recovery(self, transaction_id: int, row_string: str, action_type: str):
        """
        Send data to FailureRecovery to store it into the buffer.
        """
        execution_result = ExecutionResult(
            transaction_id=transaction_id,
            timestamp=datetime.now(),
            query=row_string,
            message=f"{action_type.capitalize()} action executed successfully"
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