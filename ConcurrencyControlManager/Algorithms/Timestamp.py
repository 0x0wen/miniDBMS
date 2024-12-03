import time
import random
from Interface import Response, Action, Rows
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm

class TimestampBasedAlgorithm(AbstractAlgorithm):

    def __init__(self, max_retries=5, retry_delay=1):
        self.timestamp_table = {}
        self.read_ts_table = {}
        self.write_ts_table = {}
        self.max_retries = max_retries  
        self.retry_delay = retry_delay  

    def getTransactionTimestamp(self, transaction_id: int) -> int:
        return self.timestamp_table.get(transaction_id, None)

    def setTransactionTimestamp(self, transaction_id: int, timestamp: int):
        self.timestamp_table[transaction_id] = timestamp

    def isValidRead(self, transaction_id: int, data_item: str) -> bool:
        read_ts = self.read_ts_table.get(data_item, -1)
        write_ts = self.write_ts_table.get(data_item, -1)
        txn_ts = self.getTransactionTimestamp(transaction_id)
        if txn_ts < write_ts:
            return False
        return True

    def isValidWrite(self, transaction_id: int, data_item: str) -> bool:
        read_ts = self.read_ts_table.get(data_item, -1)
        write_ts = self.write_ts_table.get(data_item, -1)
        txn_ts = self.getTransactionTimestamp(transaction_id)
        if txn_ts < read_ts:
            return False
        return True

    def read(self, transaction_id: int, data_item: str) -> bool:
        if not self.isValidRead(transaction_id, data_item):
            print(f"Failed to read {data_item} (Timestamp violation)")
            return False
        self.read_ts_table[data_item] = self.getTransactionTimestamp(transaction_id)
        print(f"Transaction {transaction_id} reads {data_item}")
        return True

    def write(self, transaction_id: int, data_item: str) -> bool:
        if not self.isValidWrite(transaction_id, data_item):
            print(f"Failed to write {data_item} (Timestamp violation)")
            return False
        self.write_ts_table[data_item] = self.getTransactionTimestamp(transaction_id)
        print(f"Transaction {transaction_id} writes {data_item}")
        return True

    def commit(self, transaction_id: int) -> bool:
        print(f"Transaction {transaction_id} commits")
        return True

    def parseRows(self, db_object: Rows):
        parsed_rows = []
        for row in db_object.data:
            if row[0] in ["W", "R"]:
                parsed_rows.append([row[0], row[1]])
            else:
                parsed_rows.append([row[0]])
        return parsed_rows

    def run(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        is_committed = False
        retry_count = 0
        
        for item in parsed_db_object:
            action = item[0]
            data_item = item[1] if len(item) > 1 else None
            
            if action == "W":
                valid = self.write(transaction_id, data_item)
                if not valid:
                    retry_count += 1
                    if retry_count > self.max_retries:
                        print(f"Transaction {transaction_id} aborted due to repeated write conflicts.")
                        break
                    print(f"Retrying transaction {transaction_id} due to write conflict...")
                    time.sleep(self.retry_delay)  
                    continue  

            elif action == "R":
                valid = self.read(transaction_id, data_item)
                if not valid:
                    retry_count += 1
                    if retry_count > self.max_retries:
                        print(f"Transaction {transaction_id} aborted due to repeated read conflicts.")
                        break
                    print(f"Retrying transaction {transaction_id} due to read conflict...")
                    time.sleep(self.retry_delay) 
                    continue  

            elif action == "C":
                valid = self.commit(transaction_id)
                if not valid:
                    break
                is_committed = True
                break

        if not is_committed:
            print(f"Transaction {transaction_id} failed to commit due to timestamp violations.")

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        data_item = db_object["data_item"]
        read_ts = self.read_ts_table.get(data_item, -1)
        write_ts = self.write_ts_table.get(data_item, -1)
        txn_ts = self.getTransactionTimestamp(transaction_id)

        if  action == "write":
            if read_ts is not None and  txn_ts < read_ts:
                return Response(status=False, message="Validated failed") 
            else:
                return Response(status=True, message="Validated successfully")
        elif  action == "read" :
            if write_ts is not None and txn_ts < write_ts:
                return Response(status=False, message="Validated failed") 
            else:
                return Response(status=True, message="Validated successfully")
        # if txn_ts is None:
        #     return Response(success=False, message="Validated failed")

        # if action == "read":
        #     if not self.isValidRead(transaction_id, data_item):
        #         return Response(success=False, message="Validated failed")
        #     return Response(success=True, message="Validated successfully")

        # elif action == "write":
        #     if not self.isValidWrite(transaction_id, data_item):
        #         return Response(success=False, message="Validated failed")
        #     return Response(success=True, message="Validated successfully")
        
        ################################################################################
        # if txn_ts is None:
        #     return Response(success=False, message="Validated failed")
        
        ########################################################
        # if  action == "write":
        #     if getReadTimestamp != NULL and self.compareTimestamps(transaction_timestamp, getReadTimestamp): 
        #         return Response(success=False, message="Validated failed") 
        #     else:
        #         return Response(success=True, message="Validated successfully")
        # elif  action == "read" :
        #     if getWriteTimestamp != NULL and self.compareTimestamps(transaction_timestamp, getWriteTimestamp):
        #         return Response(success=False, message="Validated failed") 
        #     else:
        #         return Response(success=True, message="Validated successfully")
        ##return Response(success=True, message="Validated successfully")

    def end(self, transaction_id: int) -> bool:
        print(f"Ending transaction {transaction_id}")
        return True
