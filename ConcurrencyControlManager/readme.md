# Concurrency Control Manager

## Overview

The Concurrency Control Manager is a Python-based system designed to manage database transactions using various concurrency control algorithms. It supports different algorithms such as Two-Phase Locking and Timestamp-Based Protocols to ensure data consistency and handle concurrent transactions effectively.

## Features

- **Two-Phase Locking (2PL):** Implements strict two-phase locking to manage read and write locks for transactions.
- **Timestamp-Based Protocol:** Uses timestamps to order transactions and resolve conflicts.
- **Deadlock Prevention:** Includes mechanisms to prevent deadlocks in concurrent transactions.
- **Transaction Management:** Supports beginning, validating, logging, and ending transactions.

## Installation

To use the Concurrency Control Manager, clone the repository and ensure you have Python installed on your system.

```bash
git clone <repository-url>
cd miniDBMS
```

## Usage

### Initializing the Manager

```python
from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager

# Initialize the concurrency control manager
manager = ConcurrentControlManager()
```

### Starting a Transaction

```python
transaction_id = manager.beginTransaction()
```

### Logging and Validating Transactions

```python
from Interface.Rows import Rows
from Interface.Action import Action

# Create a database object
db_object = Rows(["W1(A)"])

# Log the object
manager.logObject(db_object, transaction_id)

# Validate the transaction
action = Action(["write"])
response = manager.validateObject(db_object, transaction_id, action)
print(response.response_action)
```

### Ending a Transaction

```python
manager.endTransaction(transaction_id)
```

### Switching Concurrency Control Algorithms

```python
from Enum.ConcurrencyControlAlgorithmEnum import ConcurrencyControlAlgorithmEnum

# Set to use Timestamp-Based Protocol
manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.TIMESTAMP)
```

## Testing

The project includes unit tests to verify the functionality of the concurrency control algorithms. You can run the tests using:

```bash
python -m unittest discover -s ConcurrencyControlManager -p "*.py"
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.



