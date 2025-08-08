import os
import json
import shutil
from spark_analyzer import SparkApplicationAnalyzer

def read_sample_data():
    """Read the sample Spark history data."""
    sample_path = os.path.join('tests', 'data', 'sample_spark_history.json')
    with open(sample_path, 'r') as f:
        events = [json.loads(line) for line in f.readlines() if line.strip()]
    return events

def cleanup_test_environment():
    """Clean up the test environment."""
    if os.path.exists('temp_history'):
        shutil.rmtree('temp_history')
    if os.path.exists('job_durations.html'):
        os.remove('job_durations.html')

def main():
    """Run the analyzer with large sample data."""
    print("Starting dry run with large-scale sample data...")
    
    try:
        # Read the large sample data
        sample_path = os.path.join('tests', 'data', 'large_spark_history.json')
        with open(sample_path, 'r') as f:
            events = [json.loads(line) for line in f if line.strip()]
        
        # Create analysis structure
        analysis = {
            'jobs': [],
            'stages': [],
            'tasks': [],
            'sql_operations': [],
            'executors': [],
            'metrics': {
                'total_gc_time': 0,
                'total_memory_spilled': 0,
                'total_shuffle_read': 0,
                'total_shuffle_write': 0,
                'executor_metrics': {}
            }
        }
        
        # Process events
        for event in events:
            event_type = event.get('Event')
            
            if event_type == 'SparkListenerJobStart':
                analysis['jobs'].append({
                    'job_id': event['Job ID'],
                    'submission_time': event['Submission Time'],
                    'stage_ids': event['Stage IDs']
                })
            elif event_type == 'SparkListenerJobEnd':
                for job in analysis['jobs']:
                    if job['job_id'] == event['Job ID']:
                        job['completion_time'] = event['Completion Time']
                        job['status'] = event['Job Result']['Result']
                        
            elif event_type == 'SparkListenerSQLExecutionStart':
                analysis['sql_operations'].append({
                    'execution_id': event['execution_id'],
                    'description': event['description'],
                    'start_time': event['time']
                })
        
        # Generate report
        print("\nGenerating performance report...")
        analyzer = SparkApplicationAnalyzer(os.path.join('tests', 'data', 'test_s3_config.json'))
        analyzer.generate_report(analysis)
        
        print("\nDry run completed successfully!")
        print("Check 'job_durations.html' for the visualization report.")
        
    except Exception as e:
        print(f"\nError during dry run: {str(e)}")
    finally:
        if os.path.exists('job_durations.html'):
            print("\nGenerated visualization file: job_durations.html")

if __name__ == '__main__':
    main()
