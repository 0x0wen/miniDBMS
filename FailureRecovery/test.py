from datetime import datetime
from failurerecovery import FailureRecovery, ExecutionResult
import os

# Create an instance of FailureRecovery
recovery = FailureRecovery()

# Create some ExecutionResult objects
result1 = ExecutionResult(query="SELECT * FROM users", success=True, timestamp=datetime.now())
result2 = ExecutionResult(query="UPDATE users SET name='John' WHERE id=1", success=True, timestamp=datetime.now())

# Write logs
recovery.writeLog(result1)
recovery.writeLog(result2)

# Print the contents of the write-ahead log before saving the checkpoint
print("Write-ahead log before checkpoint:")
for entry in recovery.getWriteAheadLog():
    print(entry.toDict())

# Save a checkpoint
recovery.saveCheckpoint()

# Define the checkpoint file path
checkpoint_file = os.path.join(os.path.dirname(__file__), 'checkpoints', 'checkpoint_log.txt')

# Print the contents of the checkpoint_log.txt file to see the saved log entries
print("\nContents of checkpoint_log.txt after saving checkpoint:")
with open(checkpoint_file, 'r') as file:
    for line in file:
        print(line.strip())

# Add another ExecutionResult object but don't save it
result3 = ExecutionResult(query="DELETE FROM users WHERE id=1", success=True, timestamp=datetime.now())
recovery.writeLog(result3)

# Print the contents of the write-ahead log after adding the new entry
print("\nWrite-ahead log after adding a new entry (unsaved):")
for entry in recovery.getWriteAheadLog():
    print(entry.toDict())

# Print the last saved log entry from the checkpoint file
print("\nLast saved log entry from checkpoint_log.txt:")
with open(checkpoint_file, 'r') as file:
    lines = file.readlines()
    if lines:
        print(lines[-1].strip())
    else:
        print("No entries found in checkpoint_log.txt")