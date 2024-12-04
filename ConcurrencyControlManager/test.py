from unittest import TestCase

from ConcurrencyControlManager.ConcurrentControlManager import ConcurrentControlManager
from Enum.ConcurrencyControlAlgorithmEnum import ConcurrencyControlAlgorithmEnum


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
        db_object_1 = ["W1(A)", "R1(A)", "C1"]
        self.assertTrue(concurrency_control_manager.logObject(db_object_1, 1))
        self.assertTrue(concurrency_control_manager.logObject(db_object_1, 2))


    def test_validate_object(self):
        concurrency_control_manager = ConcurrentControlManager()
        concurrency_control_manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.TIMESTAMP)

        # Test case 1
        db_object_1 = ["W1(A)", "R1(A)", "C1"]
        self.assertTrue(concurrency_control_manager.validateObject(db_object_1, 1))



    def test_end_transaction(self):


    def test_set_concurrency_control(self):
        # Initialize the concurrency control manager
        concurrency_control_manager = ConcurrentControlManager()

        # Set the concurrency control algorithm to MVCC
        concurrency_control_manager.setConcurrencyControl(ConcurrencyControlAlgorithmEnum.LOCK)

        # Check if the concurrency control algorithm was set to Lock
        self.assertEqual(concurrency_control_manager.concurrency_control, ConcurrencyControlAlgorithmEnum.LOCK)
        self.assertTrue(type(concurrency_control_manager.abstract_algorithm).__name__ == "TwoPhaseLock")


