"""
Comprehensive, extensible Spark Application Analyzer
- Modular design: DataLoader, EventParser, Analyzer, Reporter, RecommendationEngine
- Actionable insights and developer guidance
- PEP8, type hints, docstrings, memory efficiency
"""
import json
import sys
import os
from typing import Dict, List, Any, Optional, Iterator, TYPE_CHECKING
import pandas as pd
import numpy as np
import html as html_mod
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(level=os.environ.get('SPARK_ANALYZER_LOG_LEVEL', 'INFO'),
                    format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Ensure optional CodeMapper import block near imports
# (Already attempted earlier; add if missing)
try:
    CodeMapper  # type: ignore  # noqa: F821
except NameError:  # pragma: no cover
    try:
        from spark_analyzer.code_mapper import CodeMapper  # type: ignore
    except Exception:  # pragma: no cover
        CodeMapper = None  # type: ignore

# --- DataLoader ---
class DataLoader:
    """Loads Spark history logs from S3 or local file."""
    def __init__(self, config_path: Optional[str] = None):
        self.config = None
        self.s3_client = None
        self.bucket_name = None
        self.malformed_lines = 0
        if config_path:
            self._load_s3_config(config_path)

    def _load_s3_config(self, config_path: str) -> None:
        import boto3
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config['access_key'],
            aws_secret_access_key=self.config['secret_key'],
            endpoint_url=self.config['endpoint_url']
        )
        self.bucket_name = self.config.get('bucket_name', 'default-bucket')

    def load_events_from_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Load Spark event log from local file using generator for memory efficiency."""
        try:
            with open(file_path, 'r') as f:
                for idx, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception as e:  # malformed line
                        self.malformed_lines += 1
                        if self.malformed_lines < 5:
                            logger.warning(f"Skipping malformed line {idx}: {e}")
                        continue
        except Exception as e:
            logger.error(f"Error loading from file: {e}")
            return

    def load_events_from_s3(self, app_id: str) -> Iterator[Dict[str, Any]]:
        """Load Spark event log from S3 using streaming for memory efficiency."""
        if not self.s3_client:
            print("S3 client not initialized. Please provide valid S3 config.")
            return
        try:
            path = f"spark-history/{app_id}"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=path)
            content = response['Body'].read().decode('utf-8')
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    yield json.loads(line)
        except Exception as e:
            print(f"Error loading from S3: {e}")
            return

# --- EventParser ---
class EventParser:
    """Parses raw Spark events into structured records."""
    def parse(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse Spark events into jobs, stages, tasks, sql, metrics."""
        jobs = []
        stages = {}
        tasks = {}
        executors = {}
        sql_map = {}
        metrics = {
            'gc': {},
            'shuffle': {},
            'memory': {}
        }
        
        for event in events:
            event_type = event.get('Event')
            
            if event_type == 'SparkListenerJobStart':
                props = event.get('Properties', {}) or {}
                # Expanded fallback chain for description extraction
                description = (
                    props.get('spark.job.description') or
                    props.get('callSite.short') or
                    props.get('callSite') or
                    props.get('callSite.long') or
                    props.get('Job Description') or
                    'Unknown'
                )
                jobs.append({
                    'job_id': event.get('Job ID'),
                    'submission_time': event.get('Submission Time'),
                    'stage_ids': event.get('Stage IDs', []),
                    'description': description,
                    'status': 'RUNNING',
                    'completion_time': None,
                    'duration': 0
                })
            elif event_type == 'SparkListenerJobEnd':
                job_id = event.get('Job ID')
                for job in jobs:
                    if job['job_id'] == job_id:
                        job['completion_time'] = event.get('Completion Time')
                        job['status'] = event.get('Job Result', {}).get('Result', 'Unknown')
                        if job['completion_time'] and job['submission_time']:
                            job['duration'] = job['completion_time'] - job['submission_time']
            elif event_type == 'SparkListenerStageSubmitted':
                stage_info = event.get('Stage Info', {})
                stage_id = stage_info.get('Stage ID')
                if stage_id is not None:
                    stages[stage_id] = {
                        'stage_id': stage_id,
                        'stage_name': stage_info.get('Stage Name', ''),
                        'num_tasks': stage_info.get('Number of Tasks', 0),
                        'submission_time': stage_info.get('Submission Time'),
                        'completion_time': None,
                        'duration': 0,
                        'parent_ids': stage_info.get('Parent IDs', [])
                    }
            elif event_type == 'SparkListenerStageCompleted':
                stage_info = event.get('Stage Info', {})
                stage_id = stage_info.get('Stage ID')
                if stage_id in stages:
                    stages[stage_id]['completion_time'] = stage_info.get('Completion Time')
                    if stages[stage_id]['submission_time'] and stages[stage_id]['completion_time']:
                        stages[stage_id]['duration'] = stages[stage_id]['completion_time'] - stages[stage_id]['submission_time']
            elif event_type == 'SparkListenerTaskStart':
                task_info = event.get('Task Info', {})
                task_id = task_info.get('Task ID')
                stage_id = event.get('Stage ID')
                if task_id is not None:
                    if stage_id not in tasks:
                        tasks[stage_id] = []
                    tasks[stage_id].append({
                        'task_id': task_id,
                        'stage_id': stage_id,
                        'executor_id': task_info.get('Executor ID'),
                        'host': task_info.get('Host'),
                        'start_time': task_info.get('Launch Time'),
                        'finish_time': None,
                        'duration': 0,
                        'failed': False,
                        'killed': False,
                        'metrics': {}
                    })
            elif event_type == 'SparkListenerTaskEnd':
                task_info = event.get('Task Info', {})
                task_metrics = event.get('Task Metrics', {})
                task_id = task_info.get('Task ID')
                stage_id = event.get('Stage ID')
                
                if stage_id in tasks:
                    for task in tasks[stage_id]:
                        if task['task_id'] == task_id:
                            task['finish_time'] = task_info.get('Finish Time')
                            task['failed'] = task_info.get('Failed', False)
                            task['killed'] = task_info.get('Killed', False)
                            if task['start_time'] and task['finish_time']:
                                task['duration'] = task['finish_time'] - task['start_time']
                            
                            task['metrics'] = {
                                'executor_run_time': task_metrics.get('Executor Run Time', 0),
                                'jvm_gc_time': task_metrics.get('JVM GC Time', 0),
                                'memory_bytes_spilled': task_metrics.get('Memory Bytes Spilled', 0),
                                'disk_bytes_spilled': task_metrics.get('Disk Bytes Spilled', 0),
                                'shuffle_read_bytes': task_metrics.get('Shuffle Read Size', 0),
                                'shuffle_write_bytes': task_metrics.get('Shuffle Write Size', 0)
                            }
                            
                            executor_id = task['executor_id']
                            if executor_id:
                                if executor_id not in metrics['gc']:
                                    metrics['gc'][executor_id] = {'gc_time': 0, 'executor_time': 0}
                                metrics['gc'][executor_id]['gc_time'] += task['metrics']['jvm_gc_time']
                                metrics['gc'][executor_id]['executor_time'] += task['metrics']['executor_run_time']
                            
                            if stage_id not in metrics['shuffle']:
                                metrics['shuffle'][stage_id] = {
                                    'disk_spill_size': 0,
                                    'shuffle_read_bytes': 0,
                                    'shuffle_write_bytes': 0
                                }
                            metrics['shuffle'][stage_id]['disk_spill_size'] += task['metrics']['disk_bytes_spilled']
                            metrics['shuffle'][stage_id]['shuffle_read_bytes'] += task['metrics']['shuffle_read_bytes']
                            metrics['shuffle'][stage_id]['shuffle_write_bytes'] += task['metrics']['shuffle_write_bytes']
                            break
            elif event_type == 'SparkListenerExecutorMetricsUpdate':
                executor_id = event.get('Executor ID')
                executor_metrics = event.get('Metrics', {})
                if executor_id:
                    executors[executor_id] = {
                        'executor_id': executor_id,
                        'jvm_heap_memory': executor_metrics.get('JVMHeapMemory', 0),
                        'disk_bytes_spilled': executor_metrics.get('DiskBytesSpilled', 0),
                        'memory_usage': 0,  # Will be calculated
                        'cpu_usage': 0      # Will be calculated
                    }
                    
                    if executor_id not in metrics['memory']:
                        metrics['memory'][executor_id] = {}
                    metrics['memory'][executor_id].update(executor_metrics)
            elif event_type == 'SparkListenerSQLExecutionStart':
                exec_id = event.get('execution_id')
                if exec_id is None:
                    exec_id = event.get('executionId')
                sql_map[exec_id] = {
                    'execution_id': exec_id,
                    'description': event.get('description', ''),
                    'start_time': event.get('time'),
                    'end_time': None,
                    'duration': None
                }
            elif event_type == 'SparkListenerSQLExecutionEnd':
                exec_id = event.get('execution_id')
                if exec_id is None:
                    exec_id = event.get('executionId')
                if exec_id in sql_map:
                    sql_map[exec_id]['end_time'] = event.get('time')
                    if sql_map[exec_id]['start_time'] is not None:
                        sql_map[exec_id]['duration'] = sql_map[exec_id]['end_time'] - sql_map[exec_id]['start_time']
        
        sql_operations = list(sql_map.values())
        # Post-process unknown job descriptions with richer context
        def _truncate(txt: str, n: int = 80) -> str:
            return txt if len(txt) <= n else txt[:n-3] + '...'
        # Build helper list for efficient overlap checks
        sql_windows = []
        for sql in sql_operations:
            st = sql.get('start_time')
            et = sql.get('end_time')
            if st is not None and et is not None:
                sql_windows.append((st, et, sql.get('execution_id'), sql.get('description', '')))
        for job in jobs:
            if job.get('description') == 'Unknown':
                jst = job.get('submission_time')
                jet = job.get('completion_time')
                matched_sql = None
                if jst is not None and jet is not None:
                    for st, et, exec_id, desc in sql_windows:
                        if st <= jet and et >= jst:  # overlap
                            matched_sql = (exec_id, desc)
                            break
                if matched_sql:
                    exec_id, desc = matched_sql
                    job['description'] = f"SQL Exec {exec_id}: {_truncate(str(desc))}"
                else:
                    stage_ids = job.get('stage_ids') or []
                    if stage_ids:
                        # Gather first non-empty stage name
                        names = [stages.get(sid, {}).get('stage_name', '') for sid in stage_ids]
                        names = [n for n in names if n]
                        if names:
                            job['description'] = _truncate(f"Stages {stage_ids[:5]}: {names[0]}")
                        else:
                            job['description'] = f"Stages {stage_ids[:5]} (total {len(stage_ids)})"
                    else:
                        job['description'] = 'Spark Job'
        return {
            'jobs': jobs,
            'stages': stages,
            'tasks': tasks,
            'sql_operations': sql_operations,
            'executors': executors,
            'metrics': metrics
        }

# --- Analyzer ---
class Analyzer:
    """Performs performance and pattern analysis on parsed data."""
    def __init__(self):
        try:
            from spark_analyzer.root_cause import RootCauseAnalyzer
            self.root_cause_analyzer = RootCauseAnalyzer()
        except ImportError:
            self.root_cause_analyzer = None
    
    def analyze(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze jobs for stats, slow/failed jobs, bottlenecks."""
        jobs = parsed.get('jobs', [])
        if not jobs:
            return {'job_stats': {}, 'bottlenecks': [], 'patterns': [], 'recommendations': [], 'performance_issues': []}

        df = pd.DataFrame(jobs)
        stats = {
            'total_jobs': len(df),
            'avg_duration': float(df['duration'].mean()) if not df.empty else 0,
            'max_duration': float(df['duration'].max()) if not df.empty else 0,
            'min_duration': float(df['duration'].min()) if not df.empty else 0,
            'success_rate': float((df['status'] == 'JobSucceeded').mean() * 100) if not df.empty else 0,
            'failed_jobs': df[df['status'] != 'JobSucceeded'].to_dict('records'),  # type: ignore[arg-type]
            'slow_jobs': df[df['duration'] > df['duration'].mean() + df['duration'].std()].to_dict('records') if not df.empty else [],  # type: ignore[arg-type]
        }
        
        # Basic bottleneck detection: jobs with duration > 2*avg
        basic_bottlenecks = df[df['duration'] > 2 * df['duration'].mean()].to_dict('records') if not df.empty else []  # type: ignore[arg-type]
        
        performance_issues = []
        if self.root_cause_analyzer:
            try:
                analysis_metrics = {
                    'jobs': jobs,
                    'stages': parsed.get('stages', {}),
                    'tasks': parsed.get('tasks', {}),
                    'executors': parsed.get('executors', {}),
                    'gc': parsed.get('metrics', {}).get('gc', {}),
                    'shuffle': parsed.get('metrics', {}).get('shuffle', {}),
                    'memory': parsed.get('metrics', {}).get('memory', {})
                }
                performance_issues = self.root_cause_analyzer.analyze_bottlenecks(analysis_metrics)
            except Exception as e:
                print(f"Warning: Root cause analysis failed: {e}")
        
        return {
            'job_stats': stats,
            'bottlenecks': basic_bottlenecks,
            'patterns': [],
            'recommendations': [],
            'performance_issues': performance_issues
        }

# --- RecommendationEngine ---
class RecommendationEngine:
    """Suggests optimizations and actionable advice."""
    def __init__(self):
        try:
            from pyspark_code_analyzer import PySparkCodeAnalyzer
            self.pyspark_analyzer = PySparkCodeAnalyzer()
        except ImportError:
            self.pyspark_analyzer = None

    def _config_snippets(self, category: str) -> List[str]:
        mapping = {
            'data_skew': ["spark.sql.adaptive.enabled=true", "spark.sql.adaptive.skewJoin.enabled=true"],
            'shuffle_spill': ["spark.shuffle.sort.bypassMergeThreshold=200", "spark.reducer.maxSizeInFlight=96m"],
            'gc_overhead': ["--conf spark.executor.memory=8g", "--conf spark.memory.fraction=0.6"],
            'cpu_bottleneck': ["--conf spark.executor.cores=4", "--conf spark.dynamicAllocation.enabled=true"],
            'memory_bottleneck': ["--conf spark.memory.fraction=0.7", "--conf spark.storage.memoryFraction=0.4"],
            'memory_pressure_model': ["--conf spark.memory.fraction=0.7", "--conf spark.sql.adaptive.enabled=true"]
        }
        return mapping.get(category, [])

    def generate(self, analysis: Dict[str, Any], parsed_data: Optional[Dict[str, Any]] = None) -> List[str]:
        recs_struct = []
        stats = analysis.get('job_stats', {})

        def add(msg: str, sev: float = 0.4):
            recs_struct.append({'msg': msg, 'severity': sev})

        baseline_added = False
        if stats.get('failed_jobs'):
            add("[FAILURES] Some jobs failed. Check logs for errors and consider increasing executor memory or reviewing input data quality.", 0.8)
        if stats.get('slow_jobs'):
            add("[SLOW_JOBS] Several jobs are significantly slower than average. Tune parallelism / partitions or cache intermediate results.", 0.6)
            baseline_added = True
        if analysis.get('bottlenecks'):
            add("[BOTTLENECKS] Detected bottleneck jobs (>2x avg). Investigate skew, shuffles, or GC overhead.", 0.7)
            baseline_added = True

        performance_issues = analysis.get('performance_issues', [])
        # Group performance issues by category to aggregate similar bottlenecks
        from collections import defaultdict
        grouped = defaultdict(list)
        for issue in performance_issues:
            grouped[issue.category].append(issue)
        for category, issues in grouped.items():
            if not issues:
                continue
            snippets = self._config_snippets(category)
            if len(issues) == 1:
                issue = issues[0]
                base = f"[{issue.category.upper()}] {issue.impact}: {issue.recommendation}"
                if snippets:
                    base += " | Config: " + ', '.join(snippets)
                add(base, min(1.0, 0.5 + issue.severity/2))
            else:
                sev_max = max(i.severity for i in issues)
                # Union of affected stages
                affected = sorted({s for i in issues for s in (i.affected_stages or [])})
                rep = max(issues, key=lambda x: x.severity)
                impacts_sample = rep.impact
                rec_text = rep.recommendation
                base = (f"[{category.upper()}-AGG] {len(issues)} occurrences across stages {affected or 'N/A'}. "
                        f"Max severity {sev_max:.2f}. Example impact: {impacts_sample}. Recommendation: {rec_text}")
                if snippets:
                    base += " | Config: " + ', '.join(snippets)
                add(base, min(1.0, 0.5 + sev_max/2))
            baseline_added = True

        if self.pyspark_analyzer and parsed_data:
            try:
                pyspark_issues = self.pyspark_analyzer.analyze_pyspark_patterns(parsed_data)
                sev_map = {'HIGH':0.9,'MEDIUM':0.6,'LOW':0.3}
                for issue in pyspark_issues:
                    add(f"[PYSPARK-{issue.severity}] {issue.description}: {issue.recommendation}", sev_map.get(issue.severity.upper(),0.5))
                    baseline_added = True
            except Exception as e:
                logger.warning(f"PySpark analysis failed: {e}")

        if not baseline_added:
            add("[BASELINE] No major issues detected; review parallelism and shuffle configuration for incremental gains.", 0.2)

        # De-duplicate and order
        dedup: Dict[str, float] = {}
        for r in recs_struct:
            if r['msg'] not in dedup or r['severity'] > dedup[r['msg']]:
                dedup[r['msg']] = r['severity']
        ordered = sorted(dedup.items(), key=lambda x: x[1], reverse=True)
        return [f"(sev={sev:.2f}) {msg}" for msg, sev in ordered]

# --- Reporter ---
class Reporter:
    """Generates HTML/Markdown reports with links and summaries."""
    def generate(self, parsed: Dict[str, Any], analysis: Dict[str, Any], recommendations: List[str], output_file: str = "spark_analysis_report.html", code_mappings: Optional[List[Dict[str, Any]]] = None) -> None:
        """Generate a modern HTML report with summary, bottlenecks, and recommendations."""
        stats = analysis.get('job_stats', {})
        bottlenecks = analysis.get('bottlenecks', [])
        failed_jobs = stats.get('failed_jobs', [])
        slow_jobs = stats.get('slow_jobs', [])
        performance_issues = analysis.get('performance_issues', [])
        # Build category aggregation
        from collections import defaultdict
        cat_counts = defaultdict(list)
        for issue in performance_issues:
            cat_counts[issue.category].append(issue)
        category_summary_rows = []
        for cat, issues in sorted(cat_counts.items()):
            max_sev = max(i.severity for i in issues)
            union_stages = sorted({s for i in issues for s in (i.affected_stages or [])})
            category_summary_rows.append((cat, len(issues), max_sev, union_stages))
        # Helper to find related SQL query for a job
        def find_sql_for_job(job, sql_ops):
            # Match by time overlap if possible
            for sql in sql_ops:
                if sql.get('start_time') and sql.get('end_time') and job.get('submission_time') and job.get('completion_time'):
                    if sql['start_time'] <= job['completion_time'] and sql['end_time'] >= job['submission_time']:
                        return sql.get('description', '')
            return ''

        sql_ops = parsed.get('sql_operations', [])
        def fmt_sec(ms):
            if ms is None:
                return "-"
            return f"{ms/1000:.2f} s"

        from datetime import datetime
        def fmt_time(ts):
            if ts is None:
                return "-"
            try:
                return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return str(ts)

        pyspark_issues = []
        if parsed:
            try:
                from pyspark_code_analyzer import PySparkCodeAnalyzer
                pyspark_analyzer = PySparkCodeAnalyzer()
                pyspark_issues = pyspark_analyzer.analyze_pyspark_patterns(parsed)
            except ImportError:
                pass

        html_parts = [  # renamed from html to avoid shadowing html_mod
            '<!DOCTYPE html>',
            '<html><head><meta charset="utf-8">',
            '<title>Spark Application Analysis Report</title>',
            '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">',
            '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/themes/prism.min.css">',
            '<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/components/prism-core.min.js"></script>',
            '<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/plugins/autoloader/prism-autoloader.min.js"></script>',
            '<style>',
            'body { font-family: Inter, Arial, sans-serif; background: #f8f9fa; color: #222; margin: 0; padding: 0; }',
            '.navbar { background: #fff; padding: 1rem 2rem; box-shadow: 0 2px 4px rgba(0,0,0,0.07); position: sticky; top: 0; z-index: 100; }',
            '.nav-links { display: flex; gap: 2rem; margin-top: 1rem; }',
            '.nav-links a { color: #222; text-decoration: none; font-weight: 500; padding: 0.5rem 1rem; border-radius: 8px; transition: background 0.2s; }',
            '.nav-links a:hover { background: #e3eafc; }',
            '.container { max-width: 1200px; margin: 2rem auto; background: #fff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); padding: 2rem; }',
            '.summary-cards { display: flex; gap: 2rem; margin-bottom: 2rem; flex-wrap: wrap; }',
            '.summary-card { flex: 1 1 180px; background: #f5f6fa; border-radius: 8px; padding: 1.5rem; box-shadow: 0 1px 2px rgba(0,0,0,0.03); text-align: center; }',
            '.section { margin-bottom: 2.5rem; }',
            '.section h2 { margin-top: 0; }',
            '.bottleneck, .failed, .slow { background: #fff3cd; border-left: 4px solid #e67e22; margin: 1rem 0; padding: 1rem; border-radius: 6px; }',
            '.recommendation { background: #e3eafc; border-left: 4px solid #3498db; margin: 1rem 0; padding: 1rem; border-radius: 6px; }',
            '.pyspark-issue { background: #f0f8ff; border-left: 4px solid #4169e1; margin: 1rem 0; padding: 1.5rem; border-radius: 6px; }',
            '.pyspark-issue h4 { margin: 0 0 10px 0; color: #4169e1; }',
            '.severity-high { border-left-color: #ff4444; background: #ffe6e6; }',
            '.severity-medium { border-left-color: #ff8800; background: #fff2e6; }',
            '.severity-low { border-left-color: #44aa44; background: #e6ffe6; }',
            '.code-fix { background: #f8f8f8; border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; font-family: "Fira Code", "Consolas", monospace; font-size: 13px; overflow-x: auto; }',
            '.job-table, .issue-table { width: 100%; border-collapse: collapse; margin-top: 1rem; }',
            '.job-table th, .job-table td, .issue-table th, .issue-table td { border: 1px solid #eee; padding: 0.5rem 0.75rem; text-align: left; }',
            '.job-table th, .issue-table th { background: #f5f6fa; }',
            '.performance-issue { background: #fff9e6; border-left: 4px solid #ffa500; margin: 1rem 0; padding: 1rem; border-radius: 6px; }',
            '</style>',
            '</head><body>',
            '<div class="navbar"><h1><i class="fas fa-chart-line"></i> Spark Application Analysis</h1>',
            '<div class="nav-links">',
            '<a href="#summary">Summary</a>',
            '<a href="#pyspark-issues">PySpark Issues</a>',
            '<a href="#performance-issues">Performance Issues</a>',
            '<a href="#bottlenecks">Bottlenecks</a>',
            '<a href="#failures">Failures</a>',
            '<a href="#recommendations">Recommendations</a>',
            ("<a href=\"#code-mapping\">Code Mapping</a>" if code_mappings else ''),
            '</div></div>',
            '<div class="container">',
            '<section id="summary" class="section">',
            '<h2>Summary</h2>',
            '<div class="summary-cards">',
            f'<div class="summary-card"><h3>Total Jobs</h3><div>{stats.get("total_jobs", 0)}</div></div>',
            f'<div class="summary-card"><h3>Avg Duration</h3><div>{fmt_sec(stats.get("avg_duration", 0))}</div></div>',
            f'<div class="summary-card"><h3>Success Rate</h3><div>{stats.get("success_rate", 0):.1f}%</div></div>',
            f'<div class="summary-card"><h3>PySpark Issues</h3><div>{len(pyspark_issues)}</div></div>',
            f'<div class="summary-card"><h3>Performance Issues</h3><div>{len(analysis.get("performance_issues", []))}</div></div>',
            f'<div class="summary-card"><h3>Issue Categories</h3><div>{len(cat_counts)}</div></div>',
            '</div></section>',
            
            '<section id="pyspark-issues" class="section">',
            '<h2><i class="fab fa-python"></i> PySpark Code Issues</h2>',
            ("<div>No PySpark-specific issues detected.</div>" if not pyspark_issues else ""),
            ''.join([
                f'<div class="pyspark-issue severity-{issue.severity.lower()}">'
                f'<h4>[{issue.severity}] {issue.pattern.replace("_", " ").title()}</h4>'
                f'<p><strong>Problem:</strong> {issue.description}</p>'
                f'<p><strong>Impact:</strong> {issue.estimated_impact}</p>'
                f'<p><strong>Fix:</strong> {issue.recommendation}</p>'
                f'<div class="code-fix"><pre><code class="language-python">{issue.code_fix}</code></pre></div>'
                f'</div>'
                for issue in pyspark_issues
            ]),
            '</section>',
            
            '<section id="performance-issues" class="section">',
            '<h2><i class="fas fa-tachometer-alt"></i> Performance Issues</h2>',
            ("<div>No performance issues detected by advanced analysis.</div>" if not performance_issues else ""),
            ''.join([
                f'<div class="performance-issue">'
                f'<strong>[{issue.category.upper()}]</strong> {issue.impact}<br>'
                f'<em>Recommendation:</em> {issue.recommendation}<br>'
                f'<em>Severity:</em> {issue.severity:.2f} | <em>Affected Stages:</em> {", ".join(map(str, issue.affected_stages)) if issue.affected_stages else "N/A"}'
                f'</div>'
                for issue in performance_issues
            ]),
            ('' if not category_summary_rows else '<h3>Category Summary</h3>'
             '<table class="issue-table"><tr><th>Category</th><th>Count</th><th>Max Severity</th><th>Affected Stages (Union)</th></tr>' +
             ''.join([
                 f'<tr><td>{cat}</td><td>{cnt}</td><td>{sev:.2f}</td><td>{" ,".join(map(str, stages)) if stages else "-"}</td></tr>'
                 for cat, cnt, sev, stages in category_summary_rows
             ]) + '</table>'),
            '</section>',
            '<section id="bottlenecks" class="section">',
            '<h2>Bottleneck Jobs</h2>',
            ("<div>No bottlenecks detected.</div>" if not bottlenecks else ""),
            ''.join([
                f'<div class="bottleneck"><b>Job {job["job_id"]}</b>: '
                f'Start: {fmt_time(job.get("submission_time"))}, End: {fmt_time(job.get("completion_time"))}, Duration: {fmt_sec(job["duration"])}'
                f', Status: {job["status"]}, Desc: {job["description"]}'
                + (f'<br><b>Query:</b> <code>{find_sql_for_job(job, sql_ops)}</code>' if find_sql_for_job(job, sql_ops) else '') + '</div>'
                for job in bottlenecks
            ]),
            '</section>',
            '<section id="failures" class="section">',
            '<h2>Failed Jobs</h2>',
            ("<div>No failed jobs.</div>" if not failed_jobs else ""),
            ''.join([
                f'<div class="failed"><b>Job {job["job_id"]}</b>: '
                f'Start: {fmt_time(job.get("submission_time"))}, End: {fmt_time(job.get("completion_time"))}, Duration: {fmt_sec(job["duration"])}'
                f', Status: {job["status"]}, Desc: {job["description"]}'
                + (f'<br><b>Query:</b> <code>{find_sql_for_job(job, sql_ops)}</code>' if find_sql_for_job(job, sql_ops) else '') + '</div>'
                for job in failed_jobs
            ]),
            '</section>',
            '<section id="recommendations" class="section">',
            '<h2>Recommendations</h2>',
            ''.join([f'<div class="recommendation">{rec}</div>' for rec in recommendations]),
            '</section>',
        ]
        # Replace appended large block by reusing previous content
        # Reconstruct code mapping section with proper escaping
        if code_mappings is not None:
            if not code_mappings:
                code_section = '<section id="code-mapping" class="section"><h2>Code Mapping</h2><div>No code repository mappings found.</div></section>'
            else:
                blocks = []
                for m in code_mappings:
                    blocks.append(
                        f"<div class='recommendation'><b>Job {m.get('job_id')}</b> → {m.get('file')}:{m.get('line')}" \
                        f"<br>Issues: {', '.join(m.get('related_issue_categories', [])) or 'None'}" \
                        f"<pre class='code-fix'><code class=\"language-{html_mod.escape(m.get('language','python'))}\">{html_mod.escape(m.get('snippet',''))}</code></pre></div>"
                    )
                code_section = '<section id="code-mapping" class="section"><h2>Code Mapping</h2>' + ''.join(blocks) + '</section>'
            html_parts.append(code_section)
        # NOTE: Rebuilding trailing sections
        # (We assume earlier content up to recommendations already appended)
        html_parts.extend([
            '<section id="jobs" class="section">',
            '<h2>All Jobs</h2>',
            '<table class="job-table"><tr><th>Job ID</th><th>Status</th><th>Start Time</th><th>End Time</th><th>Duration (s)</th><th>Description</th><th>Query</th></tr>',
            ''.join([
                '<tr>' +
                f'<td>{job["job_id"]}</td>' +
                f'<td>{job["status"]}</td>' +
                f'<td>{fmt_time(job.get("submission_time"))}</td>' +
                f'<td>{fmt_time(job.get("completion_time"))}</td>' +
                f'<td>{fmt_sec(job["duration"])}</td>' +
                f'<td>{job["description"]}</td>' +
                f'<td><code>{find_sql_for_job(job, sql_ops)}</code></td>' +
                '</tr>'
                for job in parsed.get('jobs', [])
            ]),
            '</table></section>',
        ])
        html_parts.append('</div></body></html>')
        html_content = ''.join(html_parts)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Report generated: {output_file}")

# JSON serialization helper
def build_json_summary(parsed: Dict[str, Any], analysis: Dict[str, Any], recommendations: List[str], code_mappings: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    from collections import defaultdict
    from datetime import datetime
    perf_issues = analysis.get('performance_issues', [])
    cat_groups = defaultdict(list)
    for issue in perf_issues:
        cat_groups[getattr(issue, 'category', 'unknown')].append(issue)
    cat_summary = {}
    for cat, issues in cat_groups.items():
        cat_summary[cat] = {
            'count': len(issues),
            'max_severity': max(getattr(i, 'severity', 0.0) for i in issues),
            'affected_stages_union': sorted({s for i in issues for s in (getattr(i, 'affected_stages', []) or [])})
        }
    def issue_to_dict(i):
        return {
            'category': getattr(i, 'category', ''),
            'severity': getattr(i, 'severity', 0.0),
            'impact': getattr(i, 'impact', ''),
            'recommendation': getattr(i, 'recommendation', ''),
            'affected_stages': getattr(i, 'affected_stages', []),
            'metrics': getattr(i, 'metrics', {})
        }
    data = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'job_stats': analysis.get('job_stats', {}),
        'performance_issues': [issue_to_dict(i) for i in perf_issues],
        'issue_categories': cat_summary,
        'recommendations': recommendations,
        'code_mappings': code_mappings or [],
        'job_count': len(parsed.get('jobs', [])),
        'stage_count': len(parsed.get('stages', {})),
        'sql_execution_count': len(parsed.get('sql_operations', [])),
    }
    return data

# --- Main CLI ---
def main(args):
    # Load events
    loader = DataLoader(args.s3_config)
    if args.s3_config:
        events = list(loader.load_events_from_s3(args.history_file))
    else:
        events = list(loader.load_events_from_file(args.history_file))
    if not events:
        print("No events loaded. Exiting.")
        return
    # Parse events
    parser_ = EventParser()
    parsed = parser_.parse(events)
    # Analyze
    analyzer = Analyzer()
    analysis = analyzer.analyze(parsed)
    # Recommendations
    recommender = RecommendationEngine()
    recommendations = recommender.generate(analysis, parsed)
    # Optional code mapping
    code_mappings = None
    if getattr(args, 'code_repo', None) and CodeMapper:
        try:
            mapper = CodeMapper(args.code_repo)  # type: ignore[operator]
            code_mappings = mapper.map_jobs_to_code(parsed.get('jobs', []), analysis.get('performance_issues', []))
        except Exception as e:  # pragma: no cover
            print(f"Warning: code mapping failed: {e}")
    # JSON export (write before/independent of HTML)
    if getattr(args, 'json', None):
        try:
            summary = build_json_summary(parsed, analysis, recommendations, code_mappings)
            with open(args.json, 'w') as jf:
                json.dump(summary, jf, indent=2)
            print(f"JSON summary written: {args.json}")
        except Exception as e:
            print(f"Warning: failed to write JSON summary: {e}")
    # Report
    reporter = Reporter()
    reporter.generate(parsed, analysis, recommendations, output_file=args.report, code_mappings=code_mappings)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Spark Application Analyzer")
    parser.add_argument('history_file', help='Path to Spark history log file')
    parser.add_argument('--s3-config', help='Path to S3 config JSON', default=None)
    parser.add_argument('--report', help='Output HTML report file', default='spark_analysis_report.html')
    parser.add_argument('--code-repo', help='Path to source code repository for mapping job call sites to code', default=None)
    parser.add_argument('--streaming', action='store_true', help='Enable streaming (incremental) parsing mode')
    parser.add_argument('--json', help='Optional JSON summary output file', default=None)
    args = parser.parse_args()
    main(args)
