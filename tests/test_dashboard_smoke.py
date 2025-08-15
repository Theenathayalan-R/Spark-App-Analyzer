import unittest
import importlib.util

class TestDashboardSmoke(unittest.TestCase):
    def test_layout_build(self):
        spec = importlib.util.find_spec('dash')
        if spec is None:
            self.skipTest('dash not installed')
        from spark_analyzer.dashboard import SparkDashboard
        dash_app = SparkDashboard()
        ids = {getattr(c, 'id', None) for c in dash_app.app.layout.children}
        self.assertIn('main-graph', ids)
        self.assertIn('resource-heatmap', ids)

if __name__ == '__main__':
    unittest.main()
