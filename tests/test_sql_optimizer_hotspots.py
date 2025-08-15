import unittest
from spark_analyzer.sql_optimizer import SQLOptimizer

class TestSQLOptimizerHotspots(unittest.TestCase):
    def test_hotspot_ranking_and_join_reorder(self):
        plan_json = {
            'nodeName':'AdaptiveSparkPlan',
            'children':[{
                'nodeName':'SortMergeJoin',
                'numOutputRows': 500000,
                'children':[{'nodeName':'Exchange','numOutputRows':80000},{'nodeName':'Exchange','numOutputRows':75000}]
            },{
                'nodeName':'BroadcastHashJoin',
                'numOutputRows': 10000,
                'children':[{'nodeName':'Exchange','numOutputRows':500},{'nodeName':'Exchange','numOutputRows':400}]
            },{
                'nodeName':'SortMergeJoin',
                'numOutputRows': 300000,
                'children':[{'nodeName':'Exchange','numOutputRows':60000},{'nodeName':'Exchange','numOutputRows':55000}]
            }]
        }
        opt = SQLOptimizer()
        res = opt.analyze_query({'spark_plan_info': plan_json, 'description': 'select * from a join b join c'})
        recs = '\n'.join(res.recommendations).lower()
        self.assertIn('hotspots', recs)
        self.assertTrue('join reorder' in recs or 'broadcast' in recs)

if __name__ == '__main__':
    unittest.main()
