from FailureRecovery.Structs.Row import Row
from StorageManager.objects.Condition import Condition

# Create a Row object
row = Row(1, [1, 'John', 20])

# Define some conditions to test
conditions = [
    Condition(0, '==', 1),
    Condition(1, '==', 'John'),
    Condition(2, '>', 18),
    Condition(2, '<', 25),
    Condition(2, '!=', 20),
    Condition(2, '>=', 20),
    Condition(2, '<=', 20)
]

# Test the isRowFullfilngCondition method
for condition in conditions:
    result = row.isRowFullfilngCondition(condition)
    print(f"Condition: column {condition.column} {condition.operation} {condition.operand} -> Result: {result}")
