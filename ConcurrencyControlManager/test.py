from unittest import TestCase

from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from Enum.ConcurrencyControlAlgorithmEnum import ConcurrencyControlAlgorithmEnum
from Interface.Rows import Rows
from Interface.Action import Action

class TestConcurrentControlManager(TestCase):

    def test_begin_transaction(self):
        concurrency_control_manager = ConcurrentControlManager()
        self.assertEqual(concurrency_control_manager.beginTransaction(), 1)
        self.assertEqual(concurrency_control_manager.beginTransaction(), 2)
        self.assertEqual(concurrency_control_manager.beginTransaction(), 3)
        self.assertEqual(concurrency_control_manager.beginTransaction(), 4)

    def test_log_object(self):
        concurrency_control_manager = ConcurrentControlManager()
        concurrency_control_manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.TIMESTAMP)

        # Test case 1
        db_object_1 = Rows(["W1(A)"])
        db_object_2 = Rows(["W2(A)"])
        db_object_3 = Rows(["C1(A)"])
        self.assertIs(concurrency_control_manager.abstract_algorithm.logObject(db_object_1, 1), None)
        self.assertIs(concurrency_control_manager.abstract_algorithm.logObject(db_object_2, 2), None)
        self.assertIs(concurrency_control_manager.abstract_algorithm.logObject(db_object_3, 1), None)


    def test_validate_object(self):
        concurrency_control_manager = ConcurrentControlManager()

        db_object_1 = Rows(["W1(A)"])
        db_object_2 = Rows(["W2(A)"])
        db_object_3 = Rows(["C1(A)"])
        db_object_4 = Rows(["W2(A)"])

        concurrency_control_manager.logObject(db_object_1, 1)
        trans_1 = concurrency_control_manager.validateObject(db_object_1, 1, Action(["write"]))
        self.assertTrue(trans_1.response_action == "ALLOW")

        concurrency_control_manager.logObject(db_object_2, 2)
        trans_2 = concurrency_control_manager.validateObject(db_object_2, 2, Action(["write"]))
        print(trans_2)
        self.assertTrue(trans_2.response_action == "WAIT")

        concurrency_control_manager.logObject(db_object_3, 1)
        trans_3 = concurrency_control_manager.validateObject(db_object_3, 1, Action(["commit"]))
        self.assertTrue(trans_3.response_action == "ALLOW")
        concurrency_control_manager.endTransaction(1)

        concurrency_control_manager.logObject(db_object_4, 2)
        trans_4 = concurrency_control_manager.validateObject(db_object_4, 2, Action(["write"]))
        self.assertTrue(trans_4.response_action == "ALLOW")


    def test_end_transaction(self):
        concurrency_control_manager = ConcurrentControlManager()
        concurrency_control_manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.LOCK)

        db_object_3 = Rows(["C1(A)"])
        db_object_2 = Rows(["W2(A)"])
        concurrency_control_manager.logObject(db_object_3, 1)
        trans_3 = concurrency_control_manager.validateObject(db_object_2, 1, Action(["commit"]))
        concurrency_control_manager.endTransaction(1)

    def test_set_concurrency_control(self):
        # Initialize the concurrency control manager
        concurrency_control_manager = ConcurrentControlManager()

        # Set the concurrency control algorithm to MVCC
        concurrency_control_manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.LOCK)

        # Check if the concurrency control algorithm was set to Lock
        self.assertEqual(concurrency_control_manager.concurrency_control, ConcurrencyControlAlgorithmEnum.LOCK)
        self.assertTrue(type(concurrency_control_manager.abstract_algorithm).__name__ == "TwoPhaseLock")


