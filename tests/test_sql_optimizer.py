import unittest
from spark_analyzer.sql_optimizer import SQLOptimizer

class TestSQLOptimizer(unittest.TestCase):
    def setUp(self):
        self.optimizer = SQLOptimizer()

    def test_cartesian_and_exchange_detection(self):
        plan_text = 'Exchange HashPartitioning\nCartesianProduct\nExchange'
        metrics = {'query_plan': plan_text, 'description': 'select * from a join b'}
        result = self.optimizer.analyze_query(metrics)
        joined = ' '.join(result.recommendations).lower()
        self.assertIn('cartesian', joined)
        self.assertIn('exchange', joined)

    def test_json_plan_parsing(self):
        plan_json = {"nodeName":"SortMergeJoin","children":[{"nodeName":"Exchange","children":[]},{"nodeName":"Exchange","children":[]}]}  # simplified
        metrics = {'spark_plan_info': plan_json, 'description': 'select * from a join b on a.id=b.id'}
        result = self.optimizer.analyze_query(metrics)
        recs = ' '.join(result.recommendations).lower()
        self.assertIn('sortmergejoin', recs)
        self.assertIn('broadcast', recs)

if __name__ == '__main__':
    unittest.main()
