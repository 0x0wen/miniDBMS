import os
import json
import shutil

from os import path
from typing import Optional
from datetime import datetime
from recovercriteria import RecoverCriteria
from Interface.ExecutionResult import ExecutionResult
from QueryProcessor.QueryProcessor import QueryProcessor


# Todo : Penamaan dan integrasi sama QueryProcessor
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
        
        # Cek dulu apakah log file ada
        if not os.path.exists(self._log_file):
            raise FileNotFoundError("No log file found")
        
        # Cek apakah criteria valid
        elif not criteria.timestamp and not criteria.transaction_id:
            raise ValueError("Recovery criteria must contain either timestamp or transaction id")
        
        # Membaca log file
        with open(self._log_file) as log_file:
            log_entries = log_file.readlines()
        
        # Proses recovery
        for log_entry in reversed(log_entries):
            entry = json.loads(log_entry)
            
            # Cek apakah entry memenuhi criteria
            if criteria.timestamp and entry['timestamp'] < criteria.timestamp:
                print(f"Recovery completed at timestamp {criteria.timestamp}")
                break
            if criteria.transaction_id and entry['transaction_id'] == criteria.transaction_id:
                print(f"Recovery completed at transaction id {criteria.transaction_id}")
                break
            
            # Eksekusi query untuk recovery
            try:
                QueryProcessor().execute_query(f"ROLLBACK {entry['transaction_id']}")
            except Exception as e:
                print(f"Error during recovery: {str(e)}")
                break
                           

        