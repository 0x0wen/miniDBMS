import unittest
from QueryProcessor import QueryProcessor
from unittest.mock import MagicMock

class TestQueryProcessor(unittest.TestCase):

    def setUp(self):
        # Reset the singleton instance before each test
        QueryProcessor._instance = None

    def test_singleton(self):
        # Create two instances of QueryProcessor
        instance1 = QueryProcessor()
        instance2 = QueryProcessor()

        # Check if both instances are the same
        self.assertIs(instance1, instance2)

    def test_initialization(self):
        # Create an instance of QueryProcessor
        instance = QueryProcessor()

        # Check if the instance is initialized properly
        self.assertTrue(hasattr(instance, 'concurrent_manager'))
        self.assertTrue(hasattr(instance, 'initialized'))
        self.assertTrue(instance.initialized)

    def test_execute_query(self):
        # Mock the OptimizationEngine and its methods
        mock_optimization_engine = MagicMock()
        mock_optimization_engine.parseQuery.return_value = 'parsed_query'
        mock_optimization_engine.optimizeQuery.return_value = MagicMock(query_tree=MagicMock(node_type='QUERY_TYPE'))

        # Patch the OptimizationEngine in QueryProcessor
        with unittest.mock.patch('QueryProcessor.OptimizationEngine', return_value=mock_optimization_engine):
            instance = QueryProcessor()
            query = ['SELECT * FROM table']
            result = instance.execute_query(query)

            # Check if the query was optimized
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].query_tree.node_type, 'QUERY_TYPE')

    def test_generate_rows_from_query_tree(self):
        # Mock the QueryTree and Rows
        mock_query_tree_select = MagicMock(node_type='SELECT', children=[MagicMock(node_type='FROM', val=['table1'])])
        mock_query_tree_update = MagicMock(node_type='UPDATE', val=['table2'])
        mock_optimized_query = [MagicMock(query_tree=mock_query_tree_select), MagicMock(query_tree=mock_query_tree_update)]
        mock_rows = MagicMock()

        # Patch the Rows in QueryProcessor
        with unittest.mock.patch('QueryProcessor.Rows', return_value=mock_rows):
            instance = QueryProcessor()
            transaction_id = 1
            rows = instance.generate_rows_from_query_tree(mock_optimized_query, transaction_id)

            # Check if the rows were generated correctly
            self.assertEqual(rows, mock_rows)
            self.assertEqual(rows.data, ['R1(table1)', 'W1(table2)'])

if __name__ == '__main__':
    unittest.main()