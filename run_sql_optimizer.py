"""
Script to run SQL Query Optimizer on Spark application history
"""
import json
from spark_analyzer.sql_optimizer import SQLOptimizer

def main():
    # Load sample history data
    with open('tests/data/sample_spark_history.json', 'r') as f:
        history_lines = f.readlines()
        
    # Initialize optimizer
    optimizer = SQLOptimizer()
    
    # Process SQL executions
    for line in history_lines:
        event = json.loads(line)
        if event['Event'] == 'SparkListenerSQLExecutionStart':
            print("\nAnalyzing SQL Query:")
            print("-" * 50)
            print(f"Query: {event['description']}")
            
            # Create metrics dict for analysis
            query_metrics = {
                'query_id': event['execution_id'],
                'query_plan': event.get('queryPlan', ''),  # May not be in sample data
                'metrics': {
                    'submission_time': event['time']
                }
            }
            
            # Get optimization suggestions
            optimization = optimizer.analyze_query(query_metrics)
            
            # Print results
            print("\nOptimization Recommendations:")
            for i, rec in enumerate(optimization.recommendations, 1):
                print(f"{i}. {rec}")
            
            print(f"\nEstimated Improvement: {optimization.estimated_improvement*100:.1f}%")
            print("-" * 50)

if __name__ == "__main__":
    main()
