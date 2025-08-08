"""
Main entry point for enhanced Spark Application Analyzer
"""
from typing import Dict, List, Optional
from .root_cause import RootCauseAnalyzer
from .sql_optimizer import SQLOptimizer
from .dashboard import SparkDashboard

class EnhancedSparkAnalyzer:
    def __init__(self):
        self.root_cause_analyzer = RootCauseAnalyzer()
        self.sql_optimizer = SQLOptimizer()
        self.dashboard = SparkDashboard()
        
    def analyze_application(self, history_file: str, real_time: bool = False):
        """
        Perform comprehensive analysis of Spark application
        
        Args:
            history_file: Path to history file or app ID
            real_time: Whether to enable real-time monitoring
        """
        # Load and parse events
        events = self.load_events(history_file)
        
        # Perform root cause analysis
        performance_issues = self.root_cause_analyzer.analyze_bottlenecks(events)
        
        # Analyze SQL queries
        sql_optimizations = self.sql_optimizer.analyze_queries(events)
        
        # Update dashboard
        if real_time:
            self.dashboard.update_metrics({
                'performance_issues': performance_issues,
                'sql_optimizations': sql_optimizations,
                'events': events
            })
            self.dashboard.run()
        
        return {
            'performance_issues': performance_issues,
            'sql_optimizations': sql_optimizations,
            'recommendations': self.generate_recommendations(
                performance_issues,
                sql_optimizations
            )
        }
    
    def generate_recommendations(self, 
                               performance_issues: List[Dict],
                               sql_optimizations: List[Dict]) -> List[str]:
        """Generate prioritized list of recommendations"""
        recommendations = []
        
        # Add high-severity performance issues
        for issue in sorted(performance_issues, key=lambda x: x.severity, reverse=True):
            recommendations.append(
                f"[{issue.category.upper()}] {issue.impact}: {issue.recommendation}"
            )
        
        # Add SQL optimizations with significant improvement potential
        for opt in sorted(sql_optimizations, 
                         key=lambda x: x.estimated_improvement, 
                         reverse=True):
            if opt.estimated_improvement > 0.2:  # 20% or more improvement
                recommendations.append(
                    f"[SQL] Query {opt.query_id}: {', '.join(opt.recommendations)}"
                )
        
        return recommendations
