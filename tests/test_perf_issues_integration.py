import os
import json
import unittest
from spark_analyzer_modular import EventParser, Analyzer, RecommendationEngine

PERF_FILE = os.path.join(os.path.dirname(__file__), 'data', 'sample_history_perf_issues.json')

class TestPerformanceIssuesIntegration(unittest.TestCase):
    def setUp(self):
        # Load perf issue rich history log
        with open(PERF_FILE, 'r') as f:
            self.events = [json.loads(line) for line in f if line.strip()]
        self.parsed = EventParser().parse(self.events)
        self.analysis = Analyzer().analyze(self.parsed)

    def test_failed_job_detected(self):
        stats = self.analysis.get('job_stats', {})
        failed = stats.get('failed_jobs', [])
        self.assertEqual(len(failed), 1, 'Expected one failed job detected')
        self.assertEqual(failed[0]['status'], 'JobFailed')

    def test_performance_issue_categories(self):
        categories = {iss.category for iss in self.analysis.get('performance_issues', [])}
        # Expect skew, shuffle spill and GC overhead
        self.assertIn('data_skew', categories)
        self.assertIn('shuffle_spill', categories)
        self.assertIn('gc_overhead', categories)

    def test_skew_severity_positive(self):
        skew_issues = [iss for iss in self.analysis.get('performance_issues', []) if iss.category == 'data_skew']
        self.assertTrue(skew_issues, 'No data_skew issue found')
        self.assertTrue(all(iss.severity > 0 for iss in skew_issues))

    def test_recommendations_include_perf_issues(self):
        recs = RecommendationEngine().generate(self.analysis, self.parsed)
        # With severity prefix, pattern is (sev=XX) [CATEGORY]
        self.assertTrue(any('[' in r for r in recs))

if __name__ == '__main__':
    unittest.main()
