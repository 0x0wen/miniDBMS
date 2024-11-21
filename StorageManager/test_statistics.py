import unittest
from SerializerBlock import Serializer
from StorageManager import StorageManager

class TestStatistics(unittest.TestCase):
    def setUp(self):
        """
        Set up the testing environment.
        """
        self.serializer = Serializer()
        self.manager = StorageManager()
         
        # Skema tabel testing
        self.mock_schema_test1 = [
            ("col1", "int", 4),
            ("col2", "varchar", 50),
            ("col3", "float", 4),
        ]
        self.mock_schema_test2 = [
            ("colA", "int", 4),
            ("colB", "varchar", 50),
        ]
        
        # Mock data 
        self.mock_data_test1 = [
            [1, "Alice", 3.8],
            [2, "Bob", 3.5],
            [3, "Charlie", 3.9],
        ]

        self.mock_data_test2 = [
            [10, "TestA"],
            [20, "TestB"],
            [30, "TestC"],
        ]

    def test_getStatsOneTable(self):
        """
        Test stats satu tabel.
        """
        table_name = "test1"
        self.serializer.writeTable(
            file_name=table_name,
            data=self.mock_data_test1,
            schema=self.mock_schema_test1
        )

        stats = self.manager.getStatsOneTable(table_name)

        self.assertEqual(stats.n_r, 3)  # 3 tuples
        self.assertEqual(stats.b_r, 1)  # Semua data muat dalam 1 blok
        self.assertEqual(stats.l_r, 58)  # Ukuran tuple sesuai skema
        self.assertEqual(stats.f_r, 4)  # Faktor blocking
        self.assertEqual(stats.V_a_r["col1"], 3)  # 3 nilai unik

    def test_getAllStats(self):
        """
        Test statistik untuk semua tabel.
        """
        # Mock write table "test1"
        self.serializer.writeTable(
            file_name="test1",
            data=self.mock_data_test1,
            schema=self.mock_schema_test1
        )

        # Mock write table "test2"
        self.serializer.writeTable(
            file_name="test2",
            data=self.mock_data_test2,
            schema=self.mock_schema_test2
        )

        # Hitung statistik semua tabel
        all_stats = self.manager.getStats()

        # Cek statistik "test1"
        self.assertIn("test1", all_stats)
        stats_test1 = all_stats["test1"]
        self.assertEqual(stats_test1.n_r, 3)  # 3 tuples
        self.assertEqual(stats_test1.b_r, 1)  # Semua data muat dalam 1 blok
        self.assertEqual(stats_test1.l_r, 58)  # Ukuran tuple sesuai skema
        self.assertEqual(stats_test1.V_a_r["col1"], 3)  # 3 nilai unik

        # Cek statistik "test2"
        self.assertIn("test2", all_stats)
        stats_test2 = all_stats["test2"]
        self.assertEqual(stats_test2.n_r, 3)  # 3 tuples
        self.assertEqual(stats_test2.b_r, 1)  # Semua data muat dalam 1 blok
        self.assertEqual(stats_test2.l_r, 54)  # Ukuran tuple sesuai skema
        self.assertEqual(stats_test2.V_a_r["colA"], 3)  # 3 nilai unik

if __name__ == "__main__":
    unittest.main()
