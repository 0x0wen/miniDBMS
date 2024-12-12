from QueryOptimizer.OptimizationEngine import OptimizationEngine
from QueryOptimizer.QueryTree import QueryTree
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from StorageManager.objects.DataRetrieval import DataRetrieval, Condition
from StorageManager.objects.JoinCondition import JoinCondition
from StorageManager.objects.JoinOperation import JoinOperation
from StorageManager.objects.DataWrite import DataWrite
from FailureRecovery.FailureRecovery import FailureRecovery
from StorageManager.manager.SchemaManager import SchemaManager 
from Interface.Rows import Rows
from Interface.Action import Action
from typing import List
from StorageManager.StorageManager import StorageManager
from datetime import datetime
from Interface.ExecutionResult import ExecutionResult
import time
from copy import deepcopy

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
                alias_without_comma = tokens[i + 1].split(',')[0]
                alias =alias_without_comma
                alias.removesuffix(',')
                original = tokens[i - 1]
                alias_map[alias] = original
                tokens.pop(i)  # Remove 'AS'
                tokens.pop(i)  # Remove alias
                tokens.insert(i, ',')
                i+=1
            elif tokens[i].upper() == 'FROM':
                tokens.pop(i-1)
                break
            else:
                i += 1

        # for j in range(len(tokens)):
        #     if tokens[j].split('.')[0] in alias_map:
        #         tokens[j] = alias_map[tokens[j].split('.')[0]] + '.' + tokens[j].split('.')[1]
            
        print(' '.join(tokens))
        return ' '.join(tokens), alias_map
    
    def check_transaction_course(self, query, client_id):
        # BEGIN OPTIMIZING
        optimized_query = []
        statistics = self.storage_manager.getStats()
        for q in query:
            query_without_aliases, alias_map = self.remove_aliases(q)
            optimized_query.append(
                self.optimization_engine.optimizeQuery(self.optimization_engine.parseQuery(q, statistics), statistics))
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
        print("atas")
        rows = self.generate_rows_from_query_tree(optimized_query, transaction_id)
        print("bawah")
        print(rows.data)

        # validate and Log the transaction
        for row_data in rows.data:
            # Create a Rows object with a single row of data
            single_row = Rows([row_data])

            # Create an Action object based on the first character of the row string
            row_string = single_row.data[0]
            action_type = "read" if row_string[0] == 'R' else "write"
            action = Action([action_type])
            self.concurrent_manager.two_phase_lock.logObject(single_row, transaction_id)
            response = self.concurrent_manager.two_phase_lock.validate(single_row, transaction_id, action)
        # END CONCURRENCY CONTROL

        return optimized_query, client_id, transaction_id, response

    def execute_query(self, optimized_query, client_id, transaction_id) -> List:
        results = []
        send_to_client = ""
        print("client id: ", client_id)
        for query in optimized_query:
            query_tree = query.query_tree
            if query_tree.node_type == "SELECT":
                send_to_client += self.format_results_as_table(self.query_tree_to_results(query_tree))
                print("isi send to client\n", send_to_client)
            elif query_tree.node_type == "UPDATE":
                old_rows, new_rows, table_name = self.query_tree_to_update_operations(query_tree)
                send_to_client = send_to_client + "UPDATED " + str(len(new_rows)) + " ROWS"
                self.send_to_failure_recovery(transaction_id, old_rows, new_rows, table_name, results)
    
        # self.concurrent_manager.endTransaction(transaction_id)
        # print("ini yg dikirim ke klien")
        # print(send_to_client)
        return send_to_client, results

    def send_to_failure_recovery(self, transaction_id, old_rows, new_rows, table_name, results):
        """
        Send data to FailureRecovery to store it into the buffer.
        """
        execution_result = ExecutionResult(
            transaction_id = transaction_id,
            timestamp=datetime.now(),
            message="Query executed successfully",
            data_before=old_rows,
            data_after=new_rows,
            table_name=table_name
        )
        results.append(execution_result)
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
                tables = []
                conditions = []
                self.get_table_and_condition(optimized_item.query_tree, tables, conditions)
                for table_name in tables:
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
            elif child.node_type == "Value1" or child.node_type == "Value2" or child.node_type == "FROM":
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

    def get_join_operations(self, qt: QueryTree):
        def iterate_join_on(qt: QueryTree, cond):
            cond.append(qt.val)
            if len(qt.children) == 1:
                iterate_join_on(qt.children[0], cond)
        if qt.node_type == "Value1" or qt.node_type == "Value2" or qt.node_type == "FROM":
            return qt.val[0]
        if qt.node_type == "JOIN": # cross join 
            return JoinOperation([self.get_join_operations(qt.children[0]), self.get_join_operations(qt.children[1])], JoinCondition("CROSS", []))
        elif qt.node_type == "TJOIN" and (len(qt.val) == 0): # natural join
            return JoinOperation([self.get_join_operations(qt.children[0]), self.get_join_operations(qt.children[1])], JoinCondition("NATURAL", []))
        elif qt.node_type == "TJOIN": # join on
            cond = [qt.val]
            val1 = str()
            val2 = str()
            new_tree = False
            for child in qt.children:
                if child.node_type == "Value1":
                    val1 = child.val[0]
                elif child.node_type == "Value2":
                    val2 = child.val[0]
                elif child.node_type == "TJOIN" and len(child.children) <= 1:
                    iterate_join_on(child, cond)
                elif "JOIN" in child.node_type:
                    new_tree = child
            if val1 and val2:
                return JoinOperation([val1, val2], JoinCondition("ON", cond))
            elif val1:
                return JoinOperation([val1, self.get_join_operations(new_tree)], JoinCondition("ON", cond))
            elif val2:
                return JoinOperation([self.get_join_operations(new_tree), val2], JoinCondition("ON", cond))
        else:
            return self.get_join_operations(qt.children[0])
    

    def format_results_as_table(self, results):
        # print("isi results nya", results[0])
        start_time = time.time() 
        output = []  # Use list for better performance
        # for _, rows in results.items():  # ignore table name
        if results:
            # get headers from the first row
            headers = results[0].keys()

            # Calculate column widths
            column_widths = {header: max(len(header), max(len(str(row[header])) for row in results)) for header in headers}

            # Generate header and separator lines
            header_line = " | ".join(f"{header:<{column_widths[header]}}" for header in headers)
            separator_line = "-+-".join("-" * column_widths[header] for header in headers)

            # save header and separator lines
            output.append(header_line)
            output.append(separator_line)

            # save results
            for row in results:
                row_line = " | ".join(f"{str(row[col]):<{column_widths[col]}}" for col in headers)
                output.append(row_line)

            # save number of results
            output.append(f"\n({len(results)} rows)\n")
        else:
            output.append("(No data available)\n")

        # execution_time = time.time() - start_time
        # output.append(f"\nQuery Execution Time: {execution_time:.3f} ms")

        return "\n".join(output)
        

    def query_tree_to_results(self, qt: QueryTree):
        if qt.node_type == "SELECT":
            list_of_data_retrievals, tables = self.query_tree_to_data_retrievals(qt)
            # print("list of data", list_of_data_retrievals)
            # print("table", tables)
            join_operations = self.get_join_operations(qt)
            results = {}
            for data_retrieval in list_of_data_retrievals:
                results[data_retrieval.table[0]] = self.storage_manager.readBlock(data_retrieval)
            # for table in tables:
            #     print("hasil akhirnya", results[table])
            # print("join conditionnya nya", join_operations.join_condition)
            # for j in join_operations.join_condition.condition:
            #     print(j)
            if (qt.children[0].node_type != "FROM"):
                after_join = self.apply_join_operation(join_operations, results)
            else:
                after_join = results[qt.children[0].val[0]]
            print(after_join)

            if qt.val[0] == "*":
                all_column = list(after_join[0].keys())
                after_select = self.apply_select(after_join, all_column)
            else:
                after_select = self.apply_select(after_join, qt.val)
            # print("isi qt.val itu", qt.val)

        print("after select: ", after_select)
        return after_select
    
    def apply_join_operation(self, jo: JoinOperation, results):
        # Handle case ketika jo.tables memiliki JoinOperation
        if isinstance(jo.tables[0], JoinOperation):
            left_result = self.apply_join_operation(jo.tables[0], results)
            left_table_name = "_".join(self.get_all_table_names(jo.tables[0]))
            results[left_table_name] = left_result
            jo.tables[0] = left_table_name

        if isinstance(jo.tables[1], JoinOperation):
            right_result = self.apply_join_operation(jo.tables[1], results)
            right_table_name = "_".join(self.get_all_table_names(jo.tables[1]))
            results[right_table_name] = right_result
            jo.tables[1] = right_table_name

        result = []
        if jo.join_condition.join_type == "ON":
            for d1 in results[jo.tables[0]]:
                for d2 in results[jo.tables[1]]:
                    match = True

                    # Cek setiap kondisi
                    for condition in jo.join_condition.condition:
                        if len(condition) == 3:
                            key1, operator, key2 = condition
                            val1 = None
                            val2 = None
                            
                            if key1 in d1:
                                val1 = d1[key1]
                            else:
                                for k, v in d1.items():
                                    if k.endswith(f".{key1}"):
                                        val1 = v
                                        break
                                        
                            if key2 in d2:
                                val2 = d2[key2]
                            else:
                                for k, v in d2.items():
                                    if k.endswith(f".{key2}"):
                                        val2 = v
                                        break
                            
                            if operator == "=":
                                if val1 != val2:
                                    match = False
                                    break
                        else:
                            match = False
                            or_condition = []
                            for el in condition:
                                or_condition.append(el)
                                if el.upper() == "OR":
                                    or_condition = []
                                elif len(or_condition) != 3:
                                    continue
                                elif len(or_condition) == 3:
                                    key1, operator, key2 = or_condition
                                    val1 = None
                                    val2 = None
                                    
                                    if key1 in d1:
                                        val1 = d1[key1]
                                    else:
                                        for k, v in d1.items():
                                            if k.endswith(f".{key1}"):
                                                val1 = v
                                                break
                                                
                                    if key2 in d2:
                                        val2 = d2[key2]
                                    else:
                                        for k, v in d2.items():
                                            if k.endswith(f".{key2}"):
                                                val2 = v
                                                break
                                    
                                    if operator == "=" and val1 == val2:
                                        match = True
                                        break
                    
                    # Jika semua kondisi terpenuhi, gabungkan kedua dictionary
                    if match:
                        merged_dict = {
                            f"{jo.tables[0]}.{k}": v for k, v in d1.items()
                        }
                        merged_dict.update({
                            f"{jo.tables[1]}.{k}": v for k, v in d2.items()
                        })
                        result.append(merged_dict)
        elif jo.join_condition.join_type == "CROSS":
            for d1 in results[jo.tables[0]]:
                for d2 in results[jo.tables[1]]:
                    merged_dict = {
                        f"{jo.tables[0]}.{k}": v for k, v in d1.items()
                    }
                    merged_dict.update({
                        f"{jo.tables[1]}.{k}": v for k, v in d2.items()
                    })
                    result.append(merged_dict)    
        elif jo.join_condition.join_type == "NATURAL":
            for d1 in results[jo.tables[0]]:
                for d2 in results[jo.tables[1]]:
                    # Cari atribut yang sama di kedua dictionary
                    common_keys = set(d1.keys()).intersection(set(d2.keys()))
                    if common_keys and all(d1[key] == d2[key] for key in common_keys):
                        merged_dict = {
                            f"{jo.tables[0]}.{k}": v for k, v in d1.items() if k not in common_keys
                        }
                        merged_dict.update({
                            f"{jo.tables[1]}.{k}": v for k, v in d2.items() if k not in common_keys
                        })
                        for key in common_keys:
                            merged_dict[key] = d1[key]
                        result.append(merged_dict)                   
        print("crot")
        return result

    def get_all_table_names(self, jo: JoinOperation) -> list:
        """Helper function to get all table names from a JoinOperation"""
        table_names = []
        for table in jo.tables:
            if isinstance(table, JoinOperation):
                table_names.extend(self.get_all_table_names(table))
            else:
                table_names.append(table)
        return table_names

    def apply_select(self, result, select_attributes):
        filtered_result = []
        # print("isi result tuh")
        print(result)
        for row in result:
            filtered_row = {key: value for key, value in row.items() if key in select_attributes}
            filtered_result.append(filtered_row)
        return filtered_result

    def query_tree_to_update_operations(self, qt: QueryTree):
        """
        Generate update operations based on the provided QueryTree.

        Args:
            qt (QueryTree): The QueryTree object representing the update query.

        Returns:
            old_rows: The original data before the update.
            new_rows: The updated data after the update.
            table_name: The name of the table being updated.
        """
        # TODO: belom test yang gaada WHERE nya soalnya owen belom update
        
        # Pisah qt menjadi qt_set, qt_where
        table = qt.val[0]
        qt_set = qt.children[0]
        qt_where = qt_set.children[0]

        # Ambil operation-nya
        set_operations = qt_set.val  # ['harga', '=', '15000', ',', 'desk', '=', "'data999'"]
        where_operation = qt_where.val

        # Read semua data pada table tersebut
        read_qt_select = QueryTree(node_type="SELECT", val=["*"])
        read_qt_from = QueryTree(node_type="FROM", val=[table])
        read_qt_where = QueryTree(node_type="WHERE", val=where_operation)
        read_qt_select.children.append(read_qt_from)
        read_qt_from.parent = read_qt_select
        if where_operation != []:
            read_qt_select.children.append(read_qt_where)
            read_qt_where.parent = read_qt_select
        list_of_data_retrievals, tables = self.query_tree_to_data_retrievals(read_qt_select)
        results = {}
        for data_retrieval in list_of_data_retrievals:
            results[data_retrieval.table[0]] = self.storage_manager.readBlock(data_retrieval)

        # Ambil data lama
        old_rows = results[table]
        new_rows = deepcopy(results[table])
        
        # Ganti value sesuai set_operations
        i = 0
        while i < len(set_operations):
            if set_operations[i] == ',':
                i += 1
                continue
                
            column = set_operations[i]
            value = set_operations[i + 2]
            print(f"Updating column: {column} with value: {value}")
            
            for row in new_rows:
                for key in row.keys():
                    if key == column:
                        # Remove quotes if present in the value
                        if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        row[key] = value
            
            i += 3

        # Sesuai tipe datanya
        schema_manager = SchemaManager()
        check = schema_manager.readSchema(table)
        data_types = {}
        for column in check:
            data_types[column[0]] = column[1]
        for row in new_rows:
            for key, value in row.items():
                if key in data_types:
                    if data_types[key] == "int":
                        row[key] = int(value)
                    elif data_types[key] == "float":
                        row[key] = float(value)
                    elif data_types[key] == "char":
                        row[key] = str(value)
        
        # Simpan data baru
        table_name = table

        # Kalo querynya UPDATE user2 SET harga = 15000, desk = 'data999' WHERE id = 99;
        print("Old rows:", old_rows) # [{'id': 99, 'umur': 'data99', 'harga': 34.75, 'desk': 'desk99'}]
        print("New rows:", new_rows) # [{'id': 99, 'umur': 'data99', 'harga': 15000.0, 'desk': 'data999'}]
        print("Table name:", table_name) # user2
        return old_rows, new_rows, table_name
