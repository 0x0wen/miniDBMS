from unittest import TestCase
from ConcurrencyControlManager.Algorithms.Timestamp import TimestampBasedProtocol
from Interface.Action import Action
from Interface.Response import Response
from Interface.Rows import Rows


class TestTimestampBasedProtocol(TestCase):
    
    def test_lock_tables(self):
        timestamp_protocol = TimestampBasedProtocol()

        db_object_1 = Rows(["R1(A)"])
        db_object_2 = Rows(["R2(A)"])
        db_object_3 = Rows(["W1(B)"])
        db_object_4 = Rows(["C1"])
        db_object_5 = Rows(["C2"])

        timestamp_protocol.logObject(db_object_1, 1)
        self.assertIn("A", timestamp_protocol.locks)
        self.assertEqual(timestamp_protocol.locks["A"], 1)

        timestamp_protocol.logObject(db_object_2, 2)
        self.assertIn("A", timestamp_protocol.locks)
        self.assertEqual(timestamp_protocol.locks["A"], 1)  

        timestamp_protocol.logObject(db_object_3, 1)
        self.assertIn("B", timestamp_protocol.locks)
        self.assertEqual(timestamp_protocol.locks["B"], 1)

        timestamp_protocol.logObject(db_object_4, 1)
        self.assertNotIn("A", timestamp_protocol.locks)
        self.assertNotIn("B", timestamp_protocol.locks)

        timestamp_protocol.logObject(db_object_5, 2)
        self.assertNotIn("A", timestamp_protocol.locks)

    def test_scenario_1(self):
        timestamp_protocol = TimestampBasedProtocol()

        db_object_1 = Rows(["W1(A)"])
        db_object_2 = Rows(["R1(B)"])
        db_object_3 = Rows(["R2(B)"])
        db_object_4 = Rows(["C1"])
        db_object_5 = Rows(["W2(A)"])
        db_object_6 = Rows(["R3(B)"])
        db_object_7 = Rows(["C2"])
        db_object_8 = Rows(["C3"])

        timestamp_protocol.logObject(db_object_1, 1)
        res = timestamp_protocol.validate(db_object_1, 1, Action(["write"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_2, 1)
        res = timestamp_protocol.validate(db_object_2, 1, Action(["read"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_3, 2)
        res = timestamp_protocol.validate(db_object_3, 2, Action(["read"]))
        self.assertEqual(res, Response("WOUND", 2, 1))
        
        timestamp_protocol.logObject(db_object_4, 1)
        res = timestamp_protocol.validate(db_object_4, 1, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_5, 2)
        res = timestamp_protocol.validate(db_object_5, 2, Action(["write"]))
        self.assertEqual(res, Response("ALLOW", 2, 2))

        timestamp_protocol.logObject(db_object_6, 3)
        res = timestamp_protocol.validate(db_object_6, 3, Action(["read"]))
        self.assertEqual(res, Response("ALLOW", 3, 3))

        timestamp_protocol.logObject(db_object_7, 2)
        res = timestamp_protocol.validate(db_object_7, 2, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 2, 2))

        timestamp_protocol.logObject(db_object_8, 3)
        res = timestamp_protocol.validate(db_object_8, 3, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 3, 3))

    def test_scenario_2(self):
        timestamp_protocol = TimestampBasedProtocol()

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

        timestamp_protocol.logObject(db_object_1, 1)
        res = timestamp_protocol.validate(db_object_1, 1, Action(["read"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_2, 2)
        res = timestamp_protocol.validate(db_object_2, 2, Action(["read"]))
        self.assertEqual(res, Response("WOUND", 2, 1))

        timestamp_protocol.logObject(db_object_3, 1)
        res = timestamp_protocol.validate(db_object_3, 1, Action(["write"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_4, 3)
        res = timestamp_protocol.validate(db_object_4, 3, Action(["read"]))
        self.assertEqual(res, Response("WOUND", 3, 1))

        timestamp_protocol.logObject(db_object_5, 1)
        res = timestamp_protocol.validate(db_object_5, 1, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 1, 1))

        timestamp_protocol.logObject(db_object_6, 2)
        res = timestamp_protocol.validate(db_object_6, 2, Action(["read"]))
        self.assertEqual(res, Response("ALLOW", 2, 2))

        timestamp_protocol.logObject(db_object_7, 3)
        res = timestamp_protocol.validate(db_object_7, 3, Action(["read"]))
        self.assertEqual(res, Response("WOUND", 3, 2))

        timestamp_protocol.logObject(db_object_8, 2)
        res = timestamp_protocol.validate(db_object_8, 2, Action(["write"]))
        self.assertEqual(res, Response("ALLOW", 2, 2))

        timestamp_protocol.logObject(db_object_9, 2)
        res = timestamp_protocol.validate(db_object_9, 2, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 2, 2))

        timestamp_protocol.logObject(db_object_10, 3)
        res = timestamp_protocol.validate(db_object_10, 3, Action(["read"]))
        self.assertEqual(res, Response("ALLOW", 3, 3))

        timestamp_protocol.logObject(db_object_11, 3)
        res = timestamp_protocol.validate(db_object_11, 3, Action(["commit"]))
        self.assertEqual(res, Response("ALLOW", 3, 3))

    def test_deadlock_prevention(self):
        timestamp_protocol = TimestampBasedProtocol()

        db_object_1 = Rows(["R1(A)"])
        db_object_2 = Rows(["W2(A)"])
        db_object_3 = Rows(["W1(B)"])
        db_object_4 = Rows(["R2(B)"])

        timestamp_protocol.logObject(db_object_1, 1)
        self.assertEqual(timestamp_protocol.locks["A"], 1)

        timestamp_protocol.logObject(db_object_2, 2)
        res = timestamp_protocol.validate(db_object_2, 2, Action(["write"]))
        self.assertEqual(res.response_action, "WOUND")
        
        timestamp_protocol.logObject(db_object_3, 1)
        self.assertEqual(timestamp_protocol.locks["B"], 1)

        timestamp_protocol.logObject(db_object_4, 2)
        res = timestamp_protocol.validate(db_object_4, 2, Action(["read"]))
        self.assertEqual(res.response_action, "WOUND")

        timestamp_protocol.deadlockPrevention(1)  
        timestamp_protocol.deadlockPrevention(2)  

        self.assertTrue(1 in timestamp_protocol.transactionQueue)
        self.assertTrue(2 in timestamp_protocol.transactionQueue)