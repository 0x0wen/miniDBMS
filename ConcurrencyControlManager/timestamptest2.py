import unittest
from unittest.mock import MagicMock
from ConcurrencyControlManager.Algorithms.Timestamp import TimestampBasedProtocol
from Interface.Rows import Rows
from Interface.Action import Action
from Interface.Response import Response


class TestTimestampBasedProtocol(unittest.TestCase):

    def setUp(self):
        self.timestamp_protocol = TimestampBasedProtocol()

    def test_get_timestamp(self):
        transaction_id = 1
        timestamp = self.timestamp_protocol.getTimestamp(transaction_id)
        self.assertIsInstance(timestamp, int)
        self.assertEqual(self.timestamp_protocol.timestampTable[transaction_id], timestamp)

    def test_read_lock(self):
        transaction_id_1 = 1
        transaction_id_2 = 2
        data_item = "A"
        result_1 = self.timestamp_protocol.lockS(transaction_id_1, data_item)
        self.assertTrue(result_1)
        result_2 = self.timestamp_protocol.lockS(transaction_id_2, data_item)
        self.assertFalse(result_2)

    def test_write_lock(self):
        transaction_id_1 = 1
        transaction_id_2 = 2
        data_item = "A"
        result_1 = self.timestamp_protocol.lockX(transaction_id_1, data_item)
        self.assertTrue(result_1)
        result_2 = self.timestamp_protocol.lockX(transaction_id_2, data_item)
        self.assertFalse(result_2)

    def test_validate_read_action(self):
        db_object = Rows(["R1(A)"])
        transaction_id = 1
        action = Action(["read"])
        response = self.timestamp_protocol.validate(db_object, transaction_id, action)
        self.assertEqual(response.response_action, "ALLOW")

    def test_validate_write_action(self):
        db_object = Rows(["W1(A)"])
        transaction_id = 1
        action = Action(["write"])
        response = self.timestamp_protocol.validate(db_object, transaction_id, action)
        self.assertEqual(response.response_action, "ALLOW")

    def test_deadlock_prevention(self):
        transaction_id = 1
        self.timestamp_protocol.deadlockPrevention(transaction_id)
        self.assertIn(transaction_id, self.timestamp_protocol.transactionQueue)

    def test_log_object_write(self):
        db_object = Rows(["W1(A)"])
        transaction_id = 1
        action = Action(["write"])
        self.timestamp_protocol.logObject(db_object, transaction_id)
        self.assertEqual(self.timestamp_protocol.locks["A"], transaction_id)  

    def test_validate_wait_action(self):
        db_object = Rows(["W2(A)"])
        transaction_id_1 = 1
        transaction_id_2 = 2
        action = Action(["write"])
        self.timestamp_protocol.lockX(transaction_id_1, "A")
        response = self.timestamp_protocol.validate(db_object, transaction_id_2, action)
        self.assertEqual(response.response_action, "WAIT")


    def test_validate_wait_action(self):
        db_object = Rows(["W2(A)"])
        transaction_id_1 = 1
        transaction_id_2 = 2
        action = Action(["write"])
        self.timestamp_protocol.lockX(transaction_id_1, "A")
        response = self.timestamp_protocol.validate(db_object, transaction_id_2, action)
        self.assertEqual(response.response_action, "WOUND")

    def test_end_transaction(self):
        transaction_id = 1
        db_object = Rows(["W1(A)"])
        self.timestamp_protocol.lockX(transaction_id, "A")
        self.assertIn("A", self.timestamp_protocol.locks)
        self.timestamp_protocol.end(transaction_id)
        self.assertNotIn("A", self.timestamp_protocol.locks)


if __name__ == "__main__":
    unittest.main()
