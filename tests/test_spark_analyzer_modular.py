import unittest
import os
import json
from spark_analyzer_modular import DataLoader, EventParser, Analyzer, RecommendationEngine, Reporter

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.sample_file = os.path.join(os.path.dirname(__file__), 'data/sample_spark_history.json')
        # Create a minimal sample file if not exists
        if not os.path.exists(self.sample_file):
            with open(self.sample_file, 'w') as f:
                f.write(json.dumps({"Event": "SparkListenerApplicationStart", "App Name": "TestApp"}) + '\n')
                f.write(json.dumps({"Event": "SparkListenerJobStart", "Job ID": 1, "Submission Time": 1000, "Stage IDs": [1], "Properties": {}}) + '\n')
                f.write(json.dumps({"Event": "SparkListenerJobEnd", "Job ID": 1, "Completion Time": 2000, "Job Result": {"Result": "JobSucceeded"}}) + '\n')

    def test_load_events_from_file(self):
        loader = DataLoader()
        events_generator = loader.load_events_from_file(self.sample_file)
        events = list(events_generator)  # Convert generator to list for testing
        self.assertTrue(isinstance(events, list))
        self.assertGreater(len(events), 0)
        # Find the first SparkListenerJobStart event
        job_start_event = next((e for e in events if e.get('Event') == 'SparkListenerJobStart'), None)
        self.assertIsNotNone(job_start_event)
        self.assertEqual(job_start_event['Event'], 'SparkListenerJobStart')

class TestEventParser(unittest.TestCase):
    def test_parse_minimal(self):
        parser = EventParser()
        events = [
            {"Event": "SparkListenerJobStart", "Job ID": 1, "Submission Time": 1000, "Stage IDs": [1], "Properties": {"callSite.short": "desc"}},
            {"Event": "SparkListenerJobEnd", "Job ID": 1, "Completion Time": 2000, "Job Result": {"Result": "JobSucceeded"}}
        ]
        parsed = parser.parse(events)
        self.assertIn('jobs', parsed)
        self.assertIsInstance(parsed['jobs'], list)

class TestAnalyzer(unittest.TestCase):
    def test_analyze_empty(self):
        analyzer = Analyzer()
        parsed = {'jobs': [], 'stages': [], 'tasks': [], 'sql_operations': [], 'executors': [], 'metrics': {}}
        analysis = analyzer.analyze(parsed)
        self.assertIn('job_stats', analysis)
        self.assertIn('bottlenecks', analysis)

class TestRecommendationEngine(unittest.TestCase):
    def test_generate(self):
        recommender = RecommendationEngine()
        recs = recommender.generate({'job_stats': {}})
        self.assertTrue(any('parallelism' in r.lower() or 'shuffle' in r.lower() for r in recs))

class TestReporter(unittest.TestCase):
    def test_generate(self):
        reporter = Reporter()
        # Should not raise
        reporter.generate({'jobs': []}, {'job_stats': {}}, ["Test rec"])  # Just check stub runs

if __name__ == '__main__':
    unittest.main()
