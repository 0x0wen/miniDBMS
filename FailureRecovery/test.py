import os
import json
import shutil
import sqlite3
import time

from os import path
from typing import Optional
from datetime import datetime
from typing import Generic, TypeVar, List

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.rows_count = len(data)

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data: Rows, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query

class QueryProcessor:
    _instance = None  # Static variable to hold the single instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def execute_query(self, query):
        # tinggal buat ngirim query ke query optimization
        print(query)

    def accept_query(self):
        # penerimaan query dari user
        query_input = ""
        print("> ", end="")
        while True:
            data = input()
            if ';' in data:
                query_input += data[:data.index(';')]
                break
            query_input += (data + " ")
        query_input = query_input.strip()

        # eksekusi query
        self.execute_query(query_input)


class RecoverCriteria:
    def __init__(self, timestamp: Optional[datetime] = None, transaction_id: Optional[int] = None) -> None:
        self.timestamp = timestamp
        self.transaction_id = transaction_id


class FailureRecovery:
    __instance: Optional['FailureRecovery'] = None

    # Singleton pattern
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(FailureRecovery, cls).__new__(cls)
        return cls.__instance
    
    # Menginisialisasi config file
    def __init__(self):
        # Dapetin base directory dari FailureRecovery
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        
        # Inisialisasi path file-file yang bakal dipake
        self._log_file = os.path.join(self.BASE_DIR, "write_ahead_log.txt")
        self.BACKUP_DIR = os.path.join(self.BASE_DIR, "backups")
        self.DB_FILE = os.path.join(self.BASE_DIR, "database.db") 
        self.CHECKPOINT_META = os.path.join(self.BASE_DIR, "checkpoint_meta.txt")
        self.MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB

        # Memastikan database ada 
        if not os.path.exists(self.DB_FILE):
            open(self.DB_FILE, 'a').close()

    # Method buat ngecheck ukuran log bakal return true kalo ukuran log lebih dari batas
    def checkLogSize(self) -> bool:
        return os.path.exists(self._log_file) and os.path.getsize(self._log_file) > self.MAX_LOG_SIZE 

    # Method buat nulis log pake format json
    def writeLog(self, info: ExecutionResult) -> None:
        log_entry = {
            'transaction_id': info.transaction_id,
            'timestamp': info.timestamp.isoformat(),
            'query': info.query,
            'message': info.message
        }
        with open(self._log_file, 'a') as log_file:
            json.dump(log_entry, log_file)
            log_file.write('\n')
        
        # Lakuin checkpoint kalo ukuran log udah lebih dari batas (10MB)
        if self.checkLogSize():
            self.saveCheckpoint()

    def __saveCheckpoint(self) -> None:
        try:
            # Bikin backup directory kalo belom ada
            if not os.path.exists(self.BACKUP_DIR):
                os.makedirs(self.BACKUP_DIR)

            # Ambil waktu checkpoint
            checkpoint_time = datetime.now()
            timestamp = checkpoint_time.strftime('%Y%m%d_%H%M%S')

           # 1 Ngelekauin Backup database
            dbBasename = os.path.basename(self.DB_FILE)
            backup_name = f"{dbBasename}.{timestamp}"
            backup_path = os.path.join(self.BACKUP_DIR, backup_name)
            
            if os.path.exists(self.DB_FILE):
                print(f"Copying DB to: {backup_path}")
                shutil.copy2(self.DB_FILE, backup_path)
            else:
                print("Database file not found!")

            # 2 Ngebuat metadata file checkpoint
            metadata = {
                'timestamp': checkpoint_time.isoformat(),
                'backup_file': backup_name,
                'log_size': os.path.getsize(self._log_file) if os.path.exists(self._log_file) else 0
            }
            
            meta_path = os.path.join(self.BACKUP_DIR, f"checkpoint_{timestamp}.meta")
            with open(meta_path, 'w') as meta_file:
                json.dump(metadata, meta_file, indent=2)

            # 3 Ngearchive log file
            if os.path.exists(self._log_file):
                log_basename = os.path.basename(self._log_file)
                log_archive = f"{log_basename}.{timestamp}"
                archivePath = os.path.join(self.BACKUP_DIR, log_archive)
                shutil.copy2(self._log_file, archivePath)
                os.remove(self._log_file)

            # 4 Buat log file baru yang kosong
            open(self._log_file, 'w').close()

            print(f"Checkpoint created successfully at {checkpoint_time}")
            
        except Exception as e:
            print(f"Error during checkpoint: {str(e)}")
            if not os.path.exists(self._log_file):
                open(self._log_file, 'w').close()

    def saveCheckpoint(self) -> None:
        """Method ini harusnya gada karena saveCheckpoint itu private method, 
        ini versi publicnya sementara utk keperluan testing"""
        self.__saveCheckpoint()
    
    def recover(self, criteria: RecoverCriteria) -> None:
        """This method accepts a checkpoint object that contains the criteria for checkpoint. This criteria can be timestamp or transaction id.
        The recovery process started backward from the latest log in write-ahead log until the criteria is no longer met. For each log entry,
        this method will interact with the query processor to execute a recovery query, restoring the database to its state prior to the
        execution of that log entry."""
        # Implementasi di sini

def setup_database():
    """Create initial database with users table"""
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def show_db_contents():
    """Display current database contents"""
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users')
    rows = cursor.fetchall()
    conn.close()
    return rows

def execute_query(query):
    """Execute SQL query on database"""
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

def test_failure_recovery():
    # Clean backup directory
    if os.path.exists("backups"):
        for f in os.listdir("backups"):
            os.remove(os.path.join("backups", f))
    else:
        os.makedirs("backups")

    # Setup fresh database
    if os.path.exists("database.db"):
        os.remove("database.db")
    setup_database()

    # Initialize systems
    qp = QueryProcessor()
    fr = FailureRecovery()

    # Step 1: First checkpoint
    print("\n---- First Checkpoint ----")
    result1 = ExecutionResult(
        transaction_id=1,
        timestamp=datetime.now(),
        message="Inserted Alice into users",
        data=None,
        query="INSERT INTO users (id, name) VALUES (1, 'Alice')"
    )
    execute_query(result1.query)
    fr.writeLog(result1)
    fr.saveCheckpoint()
    print("Current DB state:", show_db_contents())
    
    time.sleep(2)

    # Step 2: Second checkpoint
    print("\n---- Second Checkpoint ----")
    result2 = ExecutionResult(
        transaction_id=2,
        timestamp=datetime.now(),
        message="Inserted Bob into users",
        data=None,
        query="INSERT INTO users (id, name) VALUES (2, 'Bob')"
    )
    execute_query(result2.query)
    fr.writeLog(result2)
    fr.saveCheckpoint()
    print("Current DB state:", show_db_contents())
    
    time.sleep(2)

    # Step 3: Third checkpoint
    print("\n---- Third Checkpoint ----")
    result3 = ExecutionResult(
        transaction_id=3,
        timestamp=datetime.now(),
        message="Updated Alice to Charlie",
        data=None,
        query="UPDATE users SET name = 'Charlie' WHERE id = 1"
    )
    execute_query(result3.query)
    fr.writeLog(result3)
    fr.saveCheckpoint()
    print("Current DB state:", show_db_contents())

    print("\nBackups created:")
    for f in os.listdir(fr.BACKUP_DIR):
        print(f"- {f}")

if __name__ == "__main__":
    test_failure_recovery()
