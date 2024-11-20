from QueryOptimizer.OptimizationEngine import OptimizationEngine
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from Interface.Rows import Rows  
from QueryOptimizer import QueryTree
from typing import List

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
            self.initialized = True

    
    def execute_query(self, query):
        optimized_query = []
        for i in range (len(query)):
            optimization_engine = OptimizationEngine()
            optimized_query.append(optimization_engine.optimizeQuery(optimization_engine.parseQuery(query[i])))
            if (optimized_query[0].query_tree.node_type == "BEGIN_TRANSACTION"):
                return optimized_query
            
        return optimized_query

    def accept_query(self):
        query_input = ""
        print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()

        return query_input
    

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


    def main_driver(self):
        query = []
        query.append(self.accept_query())
        check_begin = query[0].upper().split()
        if (check_begin[0] == "BEGIN" and check_begin[1] == "TRANSACTION"):
            while (query[-1].upper() != "COMMIT"):
                query.append(self.accept_query())
        
        # nunggu valen
        optimized_query = []
        optimization_engine = OptimizationEngine()
        for q in query:
            optimized_query.append(optimization_engine.optimizeQuery(optimization_engine.parseQuery(q)))

        for q in optimized_query:
            print(q.query_tree)

        # Inisialisasi ID Transaction
        transaction_id = self.concurrent_manager.beginTransaction()
        print(f"Transaction ID: {transaction_id}")

        # Konversi QueryTree ke dalam format Transaction
        rows = self.generate_rows_from_query_tree(optimized_query, transaction_id)
        print(rows.data)
        print(rows.rows_count)

        # TODO: Penyesuaian Method Concurrency Control Manager dengan Tipe Data Rows


        # ini jangan apus dulu
        # optimized_query = self.execute_query(query)
        # while (optimized_query[0].query_tree.node_type == "BEGIN_TRANSACTION"):
        #     query_temp = self.accept_query()
        #     if (query_temp.upper() == "COMMIT"):
        #         query.pop(0)
        #         optimized_query.pop(0)
        #         optimized_query = self.execute_query(query)
        #         break

        #     query.append(query_temp)

        # for q in optimized_query:
        #     print(q)