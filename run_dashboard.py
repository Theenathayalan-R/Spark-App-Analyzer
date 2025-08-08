"""
Run the Spark Performance Analysis Dashboard with sample data
"""
import time
import json
from spark_analyzer.dashboard import SparkDashboard

def generate_sample_metrics(timestamp):
    """Generate sample metrics data"""
    return {
        'timestamp': timestamp,
        'executors': {
            'exec1': {
                'cpu_usage': 0.85 + (timestamp % 10) * 0.01,
                'memory_usage': 0.90 + (timestamp % 5) * 0.01
            },
            'exec2': {
                'cpu_usage': 0.80 + (timestamp % 8) * 0.01,
                'memory_usage': 0.88 + (timestamp % 6) * 0.01
            },
            'exec3': {
                'cpu_usage': 0.75 + (timestamp % 12) * 0.01,
                'memory_usage': 0.85 + (timestamp % 7) * 0.01
            }
        },
        'stages': {
            1: {
                'start_time': timestamp * 1000,
                'end_time': (timestamp + 5) * 1000,
                'duration': 5000,
                'parent_ids': []
            },
            2: {
                'start_time': (timestamp + 2) * 1000,
                'end_time': (timestamp + 8) * 1000,
                'duration': 6000,
                'parent_ids': [1]
            },
            3: {
                'start_time': (timestamp + 6) * 1000,
                'end_time': (timestamp + 12) * 1000,
                'duration': 6000,
                'parent_ids': [1]
            }
        },
        'tasks': {
            1: [
                {'duration': 100 + (timestamp % 5) * 20},
                {'duration': 500 + (timestamp % 3) * 30},
                {'duration': 150 + (timestamp % 4) * 25},
                {'duration': 800 + (timestamp % 6) * 15},
                {'duration': 120 + (timestamp % 7) * 18}
            ]
        },
        'gc': {
            'exec1': {'gc_time': 50, 'executor_time': 200},
            'exec2': {'gc_time': 30, 'executor_time': 200},
            'exec3': {'gc_time': 20, 'executor_time': 200}
        },
        'shuffle': {
            1: {
                'disk_spill_size': 2e9,
                'shuffle_read_bytes': 1e9,
                'shuffle_write_bytes': 1e9
            }
        }
    }

def main():
    # Create dashboard instance
    dashboard = SparkDashboard()
    
    # Add initial metrics
    initial_metrics = generate_sample_metrics(int(time.time()))
    dashboard.update_metrics(initial_metrics)
    
    # Start the dashboard
    print("Starting Spark Performance Dashboard...")
    print("Visit http://localhost:8050 to view the dashboard")
    dashboard.run()

if __name__ == '__main__':
    main()
