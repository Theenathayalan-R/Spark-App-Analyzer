import unittest
from spark_analyzer.root_cause import RootCauseAnalyzer

class TestRootCauseAdvanced(unittest.TestCase):
    def setUp(self):
        self.analyzer = RootCauseAnalyzer()

    def test_memory_pressure_and_critical_path(self):
        metrics = {
            'jobs': [{'duration': 1000}],
            'stages': {
                1: {'duration': 400, 'parent_ids': []},
                2: {'duration': 300, 'parent_ids': [1]},
                3: {'duration': 250, 'parent_ids': [2]},
            },
            'tasks': {1: [{'duration': 10},{'duration':20}], 2: [{'duration':30}], 3: [{'duration':40}]},
            'shuffle': {2: {'disk_spill_size': 900000000, 'shuffle_read_bytes': 500000000, 'shuffle_write_bytes': 200000000}},
            'gc': {'exec1': {'gc_time': 30, 'executor_time': 200}},
            'executors': {}
        }
        issues = self.analyzer.analyze_bottlenecks(metrics)
        cats = {i.category for i in issues}
        self.assertIn('critical_path', cats)
        self.assertTrue(any(i.category.startswith('memory_pressure') for i in issues))

if __name__ == '__main__':
    unittest.main()
