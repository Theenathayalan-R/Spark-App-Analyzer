import unittest
from hypothesis import given, strategies as st
import numpy as np
from spark_analyzer.root_cause import RootCauseAnalyzer

class TestSkewProperty(unittest.TestCase):
    @given(st.lists(st.floats(min_value=1, max_value=2000), min_size=5, max_size=40))
    def test_skew_detection_cv(self, durations):
        # Build metrics with single stage's tasks
        metrics = {
            'tasks': {1: [{'duration': float(d)} for d in durations]},
            'shuffle': {}, 'executors': {}, 'gc': {}, 'stages': {}
        }
        analyzer = RootCauseAnalyzer()
        issues = analyzer.analyze_bottlenecks(metrics)
        cv = np.std(durations)/np.mean(durations)
        if cv > 0.5:
            self.assertTrue(any(i.category == 'data_skew' for i in issues))
        else:
            # Should not emit high severity skew
            self.assertFalse(any(i.category == 'data_skew' and i.severity > 0.8 for i in issues))

if __name__ == '__main__':
    unittest.main()
