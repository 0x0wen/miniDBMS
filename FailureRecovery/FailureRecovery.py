from datetime import datetime
from typing import Any, Optional

# Importing modules in Failure Recovery
from FailureRecovery.Buffer import Buffer
from FailureRecovery.LogManager import LogManager
from FailureRecovery.RecoverCriteria import RecoverCriteria

# Importing modules from the Interface
from Interface.ExecutionResult import ExecutionResult

# Importing modules in Query Optimizer to parse
from QueryOptimizer.OptimizationEngine import OptimizationEngine

# Importing modules from Query Processor
from QueryProcessor.QueryProcessor import QueryProcessor

# Importing modules in Storage Manager for manipulating the database
from StorageManager.StorageManager import StorageManager
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataRetrieval import DataRetrieval

class FailureRecovery:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, buffer_size: int = 1024):
        if not hasattr(self, 'initialized'):
            self.buffer = Buffer[Any](buffer_size)
            self.log_manager = LogManager()
            self.storage_manager = StorageManager()
            self.optimization_engine = OptimizationEngine()
            self.query_processor = QueryProcessor()
            self.initialized = True
    
    def write_log(self, info: ExecutionResult) -> None:
        """Write-Ahead Logging implementation for ExecutionResult"""
        try:
            parsed_query = self.optimization_engine.parseQuery(info.query)
            query_tree = parsed_query.query_tree
            table = query_tree.val[0] if query_tree.val else None

            if not table:
                raise ValueError("No table found in query")

            # Get current data before changes
            try:
                data_retrieval = DataRetrieval(
                    table=[table],
                    column=list(info.data.data[0].keys()) if info.data and info.data.data else []
                )
                old_data = self.storage_manager.readBlock(data_retrieval)
                old_value = old_data.data[0] if old_data and old_data.data else None

            except Exception as e:
                raise Exception(f"Failed to retrieve old value: {e}")

            # Write log entry
            self.log_manager.write_log_entry(
                info.transaction_id,
                query_tree.node_type,
                table,
                old_value,
                info.data.data[0] if info.data and info.data.data else None,
                committed=True  # Mark as committed
            )

            # Buffer management with error handling
            entry_data = {
                "transaction_id": info.transaction_id,
                "operation": query_tree.node_type,
                "table": table,
                "data": info.data.data[0] if info.data and info.data.data else None
            }

            if not self.buffer.add(entry_data):
                self.save_checkpoint()
                if not self.buffer.add(entry_data):
                    raise Exception("Buffer still full after checkpoint")

        except Exception as e:
            print(f"Error in write_log: {e}")
            raise

    def save_checkpoint(self) -> None:
        """Save buffer contents to disk"""
        buffered_data = self.buffer.flush()
        
        # Group operations by table
        table_operations = {}
        for data in buffered_data:
            if data["table"] not in table_operations:
                table_operations[data["table"]] = []
            table_operations[data["table"]].append(data)
        
        # Write to storage
        for table, operations in table_operations.items():
            write_ops = [op for op in operations if op["operation"] == "WRITE"]
            if write_ops:
                data_write = DataWrite(
                    overwrite=True,
                    selected_table=table,
                    column=list(write_ops[0]["data"].keys()),
                    conditions=[],
                    new_value=[op["data"] for op in write_ops]
                )
                self.storage_manager.writeBlock(data_write)
    
    def recover(self, criteria: RecoverCriteria) -> bool:
        """Recover database state based on criteria"""
        logs = self.log_manager.read_logs(criteria.transaction_id)
        
        try:
            for log in logs:
                # Skip if after timestamp
                if criteria.timestamp and datetime.fromisoformat(log["timestamp"]) > criteria.timestamp:
                    continue
                
                # Replay operation
                if log["operation"] == "WRITE":
                    data_write = DataWrite(
                        overwrite=True,
                        selected_table=log["table"],
                        column=list(log["data_after"].keys()),
                        conditions=[],
                        new_value=[log["data_after"]]
                    )
                    self.storage_manager.writeBlock(data_write)
                
                elif log["operation"] == "DELETE":
                    # Handle delete operations
                    pass
            
            return True
        except Exception as e:
            print(f"Recovery failed: {e}")
            return False

    def rollback(self, transaction_id: int) -> bool:
        """Rollback specific transaction"""
        try:
            logs = self.log_manager.read_logs(transaction_id)
            # Reverse logs to undo operations
            for log in reversed(logs):
                if log["operation"] == "WRITE":
                    # Restore previous state
                    if log["data_before"]:
                        data_write = DataWrite(
                            overwrite=True,
                            selected_table=log["table"],
                            column=list(log["data_before"].keys()),
                            conditions=[],
                            new_value=[log["data_before"]]
                        )
                        self.storage_manager.writeBlock(data_write)
            return True
        except Exception as e:
            print(f"Rollback failed: {e}")
            return False