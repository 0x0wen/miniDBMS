from unittest import TestCase

from ConcurrencyControlManager.Algorithms.TwoPhaseLock import TwoPhaseLock
from Interface.Response import Response
from Interface.Rows import Rows

class TestTwoPhaseLock(TestCase):
    
    def test_lock_tables(self):
        two_phase_lock = TwoPhaseLock()

        db_object_1 = Rows(["R1(A)"])
        db_object_2 = Rows(["R2(A)"])
        db_object_3 = Rows(["W1(B)"])
        db_object_4 = Rows(["C1"])
        db_object_5 = Rows(["C2"])

        two_phase_lock.logObject(db_object_1, 1)
        self.assertTrue(two_phase_lock.isLockedSBySelf(1, "A"))

        two_phase_lock.logObject(db_object_2, 2)
        self.assertTrue(two_phase_lock.isLockedSBySelf(2, "A"))

        two_phase_lock.logObject(db_object_3, 1)
        self.assertTrue(two_phase_lock.isLockedXBySelf(1, "B"))

        two_phase_lock.logObject(db_object_4, 1)
        self.assertFalse(two_phase_lock.isLockedSBySelf(1, "A"))
        self.assertFalse(two_phase_lock.isLockedXBySelf(1, "B"))

        two_phase_lock.logObject(db_object_5, 2)
        self.assertFalse(two_phase_lock.isLockedSBySelf(2, "A"))

    def test_scenario_1(self):
        two_phase_lock = TwoPhaseLock()

        db_object_1 = Rows(["W1(A)"])
        db_object_2 = Rows(["R1(B)"])
        db_object_3 = Rows(["R2(B)"])
        db_object_4 = Rows(["C1"])
        db_object_5 = Rows(["W2(A)"])
        db_object_6 = Rows(["R3(B)"])
        db_object_7 = Rows(["C2"])
        db_object_7 = Rows(["C3"])

        two_phase_lock.logObject(db_object_1, 1)
        res = two_phase_lock.validate(db_object_1, 1, "WRITE")
        self.assertEqual(res, Response("ALLOW", 1, 1))

        two_phase_lock.logObject(db_object_2, 1)
        res = two_phase_lock.validate(db_object_2, 1, "READ")
        self.assertEqual(res, Response("ALLOW", 1, 1))

        two_phase_lock.logObject(db_object_3, 2)
        res = two_phase_lock.validate(db_object_3, 2, "READ")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_4, 1)
        res = two_phase_lock.validate(db_object_4, 1, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 1, 1))

        two_phase_lock.logObject(db_object_5, 2)
        res = two_phase_lock.validate(db_object_5, 2, "WRITE")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_6, 3)
        res = two_phase_lock.validate(db_object_6, 3, "READ")
        self.assertEqual(res, Response("ALLOW", 3, 3))

        two_phase_lock.logObject(db_object_7, 2)
        res = two_phase_lock.validate(db_object_7, 2, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_7, 3)
        res = two_phase_lock.validate(db_object_7, 3, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 3, 3))
    
    def test_scenario_2(self):
        two_phase_lock = TwoPhaseLock()

        db_object_1 = Rows(["R1(A)"])
        db_object_2 = Rows(["R2(A)"])
        db_object_3 = Rows(["W1(A)"])
        db_object_4 = Rows(["R3(A)"])
        db_object_5 = Rows(["C1"])
        db_object_6 = Rows(["R2(A)"])
        db_object_7 = Rows(["R3(A)"])
        db_object_8 = Rows(["W2(B)"])
        db_object_9 = Rows(["C2"])
        db_object_10 = Rows(["R3(C)"])
        db_object_11 = Rows(["C3"])

        two_phase_lock.logObject(db_object_1, 1)
        res = two_phase_lock.validate(db_object_1, 1, "READ")
        self.assertEqual(res, Response("ALLOW", 1, 1))

        two_phase_lock.logObject(db_object_2, 2)
        res = two_phase_lock.validate(db_object_2, 2, "READ")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_3, 1)
        res = two_phase_lock.validate(db_object_3, 1, "WRITE")
        self.assertEqual(res, Response("WOUND", 1, 2))

        two_phase_lock.logObject(db_object_4, 3)
        res = two_phase_lock.validate(db_object_4, 3, "READ")
        self.assertEqual(res, Response("ALLOW", 3, 3))

        two_phase_lock.logObject(db_object_5, 1)
        res = two_phase_lock.validate(db_object_5, 1, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 1, 1))

        two_phase_lock.logObject(db_object_6, 2)
        res = two_phase_lock.validate(db_object_6, 2, "READ")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_7, 3)
        res = two_phase_lock.validate(db_object_7, 3, "READ")
        self.assertEqual(res, Response("ALLOW", 3, 3))

        two_phase_lock.logObject(db_object_8, 2)
        res = two_phase_lock.validate(db_object_8, 2, "WRITE")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_9, 2)
        res = two_phase_lock.validate(db_object_9, 2, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 2, 2))

        two_phase_lock.logObject(db_object_10, 3)
        res = two_phase_lock.validate(db_object_10, 3, "READ")
        self.assertEqual(res, Response("ALLOW", 3, 3))

        two_phase_lock.logObject(db_object_11, 3)
        res = two_phase_lock.validate(db_object_11, 3, "COMMIT")
        self.assertEqual(res, Response("ALLOW", 3, 3))