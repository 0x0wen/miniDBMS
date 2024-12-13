import unittest
from unittest.mock import MagicMock, patch
from QueryProcessor.QueryProcessor import QueryProcessor
from QueryOptimizer.QueryTree import QueryTree
from StorageManager.objects.JoinOperation import JoinOperation
from StorageManager.objects.JoinCondition import JoinCondition
from Interface.Rows import Rows

class TestQueryProcessor(unittest.TestCase):

    def setUp(self):
        # Reset the singleton instance before each test
        QueryProcessor._instance = None
        self.query_processor = QueryProcessor()

    def test_singleton(self):
        # Create two instances of QueryProcessor
        instance1 = QueryProcessor()
        instance2 = QueryProcessor()

        # Check if both instances are the same
        self.assertIs(instance1, instance2)

    def test_initialization(self):
        # Check if the instance is initialized properly
        self.assertTrue(hasattr(self.query_processor, 'concurrent_manager'))
        self.assertTrue(hasattr(self.query_processor, 'initialized'))
        self.assertTrue(self.query_processor.initialized)

    def test_remove_aliases(self):
        query = "SELECT coursename as cs FROM course"
        expected_query = "SELECT coursename FROM course"
        result_query, alias_map = self.query_processor.remove_aliases(query)
        self.assertEqual(result_query, expected_query)
        self.assertEqual(alias_map, {'cs': 'coursename'})

    def test_generate_rows_from_query_tree(self):
        # Mock query trees for SELECT and UPDATE operations
        mock_query_tree_select = MagicMock(node_type='SELECT', children=[MagicMock(node_type='FROM', val=['table1'])])
        mock_query_tree_update = MagicMock(node_type='UPDATE', val=['table2'])
        
        # Mock optimized query that contains SELECT and UPDATE query trees
        mock_optimized_query = [MagicMock(query_tree=mock_query_tree_select), MagicMock(query_tree=mock_query_tree_update)]
        
        mock_rows = MagicMock()
        
        mock_rows.data = ['R1(table1)', 'W1(table2)']
        
        # Mock the Rows instantiation within the QueryProcessor class
        with patch('QueryProcessor.QueryProcessor.Rows', return_value=mock_rows):
            transaction_id = 1
            rows = self.query_processor.generate_rows_from_query_tree(mock_optimized_query, transaction_id)

            # Assertions
            self.assertEqual(rows, mock_rows) 
            self.assertEqual(rows.data, ['R1(table1)', 'W1(table2)']) 


    def test_apply_join_operation(self):
        results = {
            'table1': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
            'table2': [{'id': 1, 'age': 25}, {'id': 2, 'age': 30}]
        }
        join_condition = JoinCondition(join_type="ON", condition=[("table1.id", "=", "table2.id")])
        join_operation = JoinOperation(tables=["table1", "table2"], join_condition=join_condition)

        result = self.query_processor.apply_join_operation(join_operation, results)
        expected_result = [
            {'table1.id': 1, 'table1.name': 'Alice', 'table2.id': 1, 'table2.age': 25},
            {'table1.id': 2, 'table1.name': 'Bob', 'table2.id': 2, 'table2.age': 30}
        ]

        self.assertEqual(result, expected_result)

    def test_format_results_as_table(self):
        results = [
            {'id': 1, 'name': 'Alice', 'age': 25},
            {'id': 2, 'name': 'Bob', 'age': 30}
        ]
        expected_output = (
            "id | name  | age\n"
            "---+-------+----\n"
            "1  | Alice | 25 \n"
            "2  | Bob   | 30 \n"
            "\n(2 rows)\n"
        )
        output = self.query_processor.format_results_as_table(results)
        self.assertEqual(output, expected_output)

def test_query_tree_to_results(self):
    # Siapkan query tree
    qt = QueryTree(node_type="SELECT", val=["*"])
    qt.children.append(QueryTree(node_type="FROM", val=["employees"]))
    qt.children.append(QueryTree(node_type="WHERE", val=["employee_id", "=", "1"]))

    # Mock data retrievals dan operasi join
    mock_data_retrieval = MagicMock()
    mock_data_retrieval.table = ["employees"]

    with patch.object(self.query_processor, 'query_tree_to_data_retrievals', return_value=([mock_data_retrieval], ["employees"])):
        with patch.object(self.query_processor, 'get_join_operations', return_value=MagicMock()):
            with patch.object(self.query_processor.storage_manager, 'readBlock', return_value=[{'employee_id': 1, 'name': 'Alice', 'position': 'cashier'}]):
                with patch.object(self.query_processor, 'apply_join_operation', return_value=[{'employee_id': 1, 'name': 'Alice', 'position': 'cashier'}]):
                    # Panggil metode yang diuji
                    result = self.query_processor.query_tree_to_results(qt)

                    # Hasil yang diharapkan
                    expected_result = [{'employee_id': 1, 'name': 'Alice', 'position': 'cashier'}]

                    # Bandingkan hasilnya
                    self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()