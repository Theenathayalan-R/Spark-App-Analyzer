import json
import random
from datetime import datetime, timedelta

def generate_large_spark_history(num_jobs=3000):
    base_time = int(datetime(2025, 8, 8).timestamp() * 1000)
    events = []
    
    # Application Start
    events.append({
        "Event": "SparkListenerApplicationStart",
        "App Name": "Large Scale Data Processing",
        "App ID": "app-20250808-complex",
        "Timestamp": base_time
    })
    
    # Environment Info
    events.append({
        "Event": "SparkListenerEnvironmentUpdate",
        "JVM Information": [{"name": "java.version", "value": "11.0.12"}],
        "Spark Properties": [
            {"name": "spark.executor.memory", "value": "32g"},
            {"name": "spark.executor.cores", "value": "8"},
            {"name": "spark.default.parallelism", "value": "2000"}
        ]
    })
    
    # Common SQL patterns
    sql_patterns = [
        "SELECT * FROM table_{} WHERE date > '2025-01-01'",
        "SELECT t1.*, t2.* FROM table_{} t1 JOIN table_{} t2 ON t1.id = t2.id",
        "SELECT category, SUM(value) FROM table_{} GROUP BY category",
        "SELECT * FROM table_{} WHERE column_{} IN (SELECT id FROM table_{})",
        "WITH cte AS (SELECT * FROM table_{}) SELECT * FROM cte WHERE value > 1000"
    ]
    
    current_time = base_time
    execution_id = 0
    
    # Generate job patterns
    for job_id in range(num_jobs):
        # Simulate different job patterns
        is_complex = random.random() < 0.2  # 20% complex jobs
        is_data_intensive = random.random() < 0.3  # 30% data intensive
        has_sql = random.random() < 0.7  # 70% SQL operations
        
        # Number of stages for this job
        num_stages = random.randint(3, 8) if is_complex else random.randint(1, 2)
        stage_ids = list(range(job_id * 10, job_id * 10 + num_stages))
        
        # Job Start
        events.append({
            "Event": "SparkListenerJobStart",
            "Job ID": job_id,
            "Submission Time": current_time,
            "Stage IDs": stage_ids,
            "Properties": {"callSite.short": f"job_{job_id} at Application.scala:120"}
        })
        
        # SQL Execution if applicable
        if has_sql:
            sql_pattern = random.choice(sql_patterns)
            events.append({
                "Event": "SparkListenerSQLExecutionStart",
                "execution_id": execution_id,
                "description": sql_pattern.format(
                    random.randint(1, 100),
                    random.randint(1, 100),
                    random.randint(1, 100)
                ),
                "time": current_time
            })
        
        # Generate stages and tasks
        for stage_id in stage_ids:
            num_tasks = random.randint(100, 1000) if is_data_intensive else random.randint(10, 100)
            
            # Stage Submit
            events.append({
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": {
                    "Stage ID": stage_id,
                    "Stage Name": f"stage_{stage_id}",
                    "Number of Tasks": num_tasks
                }
            })
            
            # Sample task metrics
            step = max(1, num_tasks//2)  # Ensure step is at least 1
            for task_id in range(0, num_tasks, step):  # Add metrics for some tasks
                executor_id = random.randint(1, 20)
                
                # Task metrics
                gc_time = random.randint(100, 1000) if is_complex else random.randint(10, 100)
                memory_spill = random.randint(1024*1024, 10*1024*1024) if is_data_intensive else 0
                shuffle_read = random.randint(1024*1024, 5*1024*1024) if is_complex else 0
                shuffle_write = random.randint(1024*1024, 5*1024*1024) if is_complex else 0
                
                events.append({
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": stage_id,
                    "Task Info": {
                        "Task ID": task_id,
                        "Executor ID": str(executor_id),
                        "Host": f"worker{executor_id}",
                        "Finish Time": current_time + random.randint(1000, 5000),
                        "Failed": random.random() < 0.01,  # 1% failure rate
                        "Killed": False
                    },
                    "Task Metrics": {
                        "Executor Run Time": random.randint(1000, 10000),
                        "JVM GC Time": gc_time,
                        "Memory Bytes Spilled": memory_spill,
                        "Shuffle Read Size": shuffle_read,
                        "Shuffle Write Size": shuffle_write
                    }
                })
                
                # Executor Metrics
                events.append({
                    "Event": "SparkListenerExecutorMetricsUpdate",
                    "Executor ID": str(executor_id),
                    "Metrics": {
                        "JVMHeapMemory": random.randint(2000000000, 4000000000),
                        "DiskBytesSpilled": memory_spill
                    }
                })
            
            # Stage Completion
            stage_duration = random.randint(5000, 20000) if is_complex else random.randint(1000, 5000)
            events.append({
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {
                    "Stage ID": stage_id,
                    "Stage Name": f"stage_{stage_id}",
                    "Number of Tasks": num_tasks,
                    "Submission Time": current_time,
                    "Completion Time": current_time + stage_duration
                }
            })
        
        # SQL Execution End if applicable
        if has_sql:
            events.append({
                "Event": "SparkListenerSQLExecutionEnd",
                "execution_id": execution_id,
                "time": current_time + random.randint(1000, 10000)
            })
            execution_id += 1
        
        # Job End
        job_duration = random.randint(15000, 60000) if is_complex else random.randint(5000, 15000)
        current_time += job_duration
        events.append({
            "Event": "SparkListenerJobEnd",
            "Job ID": job_id,
            "Completion Time": current_time,
            "Job Result": {
                "Result": "JobSucceeded" if random.random() > 0.02 else "JobFailed"  # 2% failure rate
            }
        })
    
    # Application End
    events.append({
        "Event": "SparkListenerApplicationEnd",
        "Timestamp": current_time + 1000
    })
    
    return events

def main():
    print("Generating large Spark history dataset...")
    events = generate_large_spark_history(3000)
    
    output_file = "tests/data/large_spark_history.json"
    with open(output_file, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"Generated {len(events)} events")
    print(f"Output written to {output_file}")

if __name__ == "__main__":
    main()
