import os
import json
import unittest
from spark_analyzer_modular import EventParser, Analyzer, RecommendationEngine, Reporter
from spark_analyzer.code_mapper import CodeMapper

DATA_FILE = os.path.join(os.path.dirname(__file__), 'data', 'sample_history_with_callsites.json')
REPO_ROOT = os.path.join(os.path.dirname(__file__), 'sample_repo')

class TestCodeMappingIntegration(unittest.TestCase):
    def setUp(self):
        # Load events
        with open(DATA_FILE, 'r') as f:
            events = [json.loads(line) for line in f if line.strip()]
        self.parsed = EventParser().parse(events)
        self.analysis = Analyzer().analyze(self.parsed)

    def test_mapping_produces_entry(self):
        mapper = CodeMapper(REPO_ROOT)
        mappings = mapper.map_jobs_to_code(self.parsed.get('jobs', []), self.analysis.get('performance_issues', []))
        self.assertTrue(any(m['file'].endswith('pipeline.py') for m in mappings))
        # Ensure language inference present
        self.assertTrue(all('language' in m for m in mappings))

    def test_report_generation_with_mapping(self):
        mapper = CodeMapper(REPO_ROOT)
        mappings = mapper.map_jobs_to_code(self.parsed.get('jobs', []), self.analysis.get('performance_issues', []))
        reporter = Reporter()
        out_file = 'test_report.html'
        reporter.generate(self.parsed, self.analysis, ["Test rec"], output_file=out_file, code_mappings=mappings)
        self.assertTrue(os.path.exists(out_file))
        with open(out_file, 'r') as f:
            html = f.read()
        self.assertIn('Code Mapping', html)
        self.assertIn('pipeline.py', html)
        # Clean up
        os.remove(out_file)

if __name__ == '__main__':
    unittest.main()
