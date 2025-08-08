import unittest
import json
from spark_analyzer.root_cause import RootCauseAnalyzer, PerformanceIssue

class TestRootCauseAnalyzer(unittest.TestCase):
    def setUp(self):
        self.analyzer = RootCauseAnalyzer()
        self.sample_metrics = {
            'tasks': {
                1: [
                    {'duration': 100}, {'duration': 500}, {'duration': 150},
                    {'duration': 800}, {'duration': 120}  # High skew
                ]
            },
            'shuffle': {
                1: {
                    'disk_spill_size': 2e9,  # 2GB spill
                    'shuffle_read_bytes': 1e9,
                    'shuffle_write_bytes': 1e9
                }
            },
            'executors': {
                'exec1': {'cpu_usage': 0.95, 'memory_usage': 0.92},
                'exec2': {'cpu_usage': 0.88, 'memory_usage': 0.95},
                'exec3': {'cpu_usage': 0.90, 'memory_usage': 0.88}
            },
            'gc': {
                'exec1': {'gc_time': 50, 'executor_time': 200},  # 25% GC time
                'exec2': {'gc_time': 30, 'executor_time': 200}   # 15% GC time
            },
            'stages': {
                1: {'duration': 1000, 'parent_ids': []},
                2: {'duration': 500, 'parent_ids': [1]},
                3: {'duration': 600, 'parent_ids': [1]},
                4: {'duration': 400, 'parent_ids': [1]},
                5: {'duration': 300, 'parent_ids': [2, 3]}
            }
        }

    def test_full_analysis(self):
        print("\n=== Testing Root Cause Analysis ===\n")
        
        # Run analysis
        issues = self.analyzer.analyze_bottlenecks(self.sample_metrics)
        
        # Print findings
        print(f"Found {len(issues)} performance issues:\n")
        for i, issue in enumerate(issues, 1):
            print(f"Issue {i}:")
            print(f"Category: {issue.category}")
            print(f"Severity: {issue.severity:.2f}")
            print(f"Impact: {issue.impact}")
            print(f"Recommendation: {issue.recommendation}")
            print(f"Affected Stages: {issue.affected_stages}")
            print("Metrics:", json.dumps(issue.metrics, indent=2))
            print()

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2)
