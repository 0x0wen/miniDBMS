import unittest
from FailureRecovery.Structs.Row import Row
from StorageManager.objects.Condition import Condition

# test_Row.py

class TestRow(unittest.TestCase):

    def setUp(self):
        self.row = Row(1, [1, 'John', 20])

    def test_isRowFullfilngCondition_equal(self):
        condition = Condition(column=2, operation='==', operand=20)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_not_equal(self):
        condition = Condition(column=2, operation='!=', operand=25)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_less_than(self):
        condition = Condition(column=2, operation='<', operand=25)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_greater_than(self):
        condition = Condition(column=2, operation='>', operand=15)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_less_than_or_equal(self):
        condition = Condition(column=2, operation='<=', operand=20)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_greater_than_or_equal(self):
        condition = Condition(column=2, operation='>=', operand=20)
        self.assertTrue(self.row.isRowFullfilngCondition(condition))

    def test_isRowFullfilngCondition_unsupported_operator(self):
        condition = Condition(column=2, operation='<>', operand=20)
        with self.assertRaises(ValueError):
            self.row.isRowFullfilngCondition(condition)

if __name__ == '__main__':
    unittest.main()