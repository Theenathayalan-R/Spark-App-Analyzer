"""
Comprehensive, extensible Spark Application Analyzer
- Modular design: DataLoader, EventParser, Analyzer, Reporter, RecommendationEngine
- Actionable insights and developer guidance
- PEP8, type hints, docstrings, memory efficiency
"""
import json
import sys
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
# --- DataLoader ---
class DataLoader:
    def load_events_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Load Spark event log from local file."""
        try:
            with open(file_path, 'r') as f:
                return [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            print(f"Error loading from file: {e}")
            return []

    def load_events_from_s3(self, app_id: str) -> List[Dict[str, Any]]:
        """Load Spark event log from S3."""
        try:
            path = f"spark-history/{app_id}"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=path)
            content = response['Body'].read().decode('utf-8')
            return [json.loads(line) for line in content.split('\n') if line.strip()]
        except Exception as e:
            print(f"Error loading from S3: {e}")
            return []
    """Loads Spark history logs from S3 or local file."""
    def __init__(self, config_path: Optional[str] = None):
        self.config = None
        self.s3_client = None
        self.bucket_name = None
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
def main(args):
    # Load events
    loader = DataLoader(args.s3_config)
    if args.s3_config:
        events = loader.load_events_from_s3(args.history_file)
    else:
        events = loader.load_events_from_file(args.history_file)
    if not events:
        print("No events loaded. Exiting.")
        return
    # Parse events

# --- EventParser ---
class EventParser:
    """Parses raw Spark events into structured records."""
    def parse(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse Spark events into jobs, stages, tasks, sql, metrics."""
        jobs = []
        sql_map = {}
        for event in events:
            if event.get('Event') == 'SparkListenerJobStart':
                jobs.append({
                    'job_id': event.get('Job ID'),
                    'submission_time': event.get('Submission Time'),
                    'stage_ids': event.get('Stage IDs', []),
                    'description': event.get('Properties', {}).get('callSite.short', 'Unknown'),
                    'status': 'RUNNING',
                    'completion_time': None,
                    'duration': 0
                })
            elif event.get('Event') == 'SparkListenerJobEnd':
                job_id = event.get('Job ID')
                for job in jobs:
                    if job['job_id'] == job_id:
                        job['completion_time'] = event.get('Completion Time')
                        job['status'] = event.get('Job Result', {}).get('Result', 'Unknown')
                        if job['completion_time'] and job['submission_time']:
                            job['duration'] = job['completion_time'] - job['submission_time']
            elif event.get('Event') == 'SparkListenerSQLExecutionStart':
                sql_map[event.get('executionId')] = {
                    'execution_id': event.get('executionId'),
                    'description': event.get('description', ''),
                    'start_time': event.get('time'),
                    'end_time': None,
                    'duration': None
                }
            elif event.get('Event') == 'SparkListenerSQLExecutionEnd':
                exec_id = event.get('executionId')
                if exec_id in sql_map:
                    sql_map[exec_id]['end_time'] = event.get('time')
                    if sql_map[exec_id]['start_time'] is not None:
                        sql_map[exec_id]['duration'] = sql_map[exec_id]['end_time'] - sql_map[exec_id]['start_time']
        sql_operations = list(sql_map.values())
        return {
            'jobs': jobs,
            'stages': [],
            'tasks': [],
            'sql_operations': sql_operations,
            'executors': [],
            'metrics': {}
        }

# --- Analyzer ---
class Analyzer:
    """Performs performance and pattern analysis on parsed data."""
    def analyze(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze jobs for stats, slow/failed jobs, bottlenecks."""
        jobs = parsed.get('jobs', [])
        if not jobs:
            return {'job_stats': {}, 'bottlenecks': [], 'patterns': [], 'recommendations': []}

        df = pd.DataFrame(jobs)
        stats = {
            'total_jobs': len(df),
            'avg_duration': float(df['duration'].mean()) if not df.empty else 0,
            'max_duration': float(df['duration'].max()) if not df.empty else 0,
            'min_duration': float(df['duration'].min()) if not df.empty else 0,
            'success_rate': float((df['status'] == 'JobSucceeded').mean() * 100) if not df.empty else 0,
            'failed_jobs': df[df['status'] != 'JobSucceeded'].to_dict('records'),
            'slow_jobs': df[df['duration'] > df['duration'].mean() + df['duration'].std()].to_dict('records') if not df.empty else [],
        }
        # Bottleneck: jobs with duration > 2*avg
        bottlenecks = df[df['duration'] > 2 * df['duration'].mean()].to_dict('records') if not df.empty else []
        return {
            'job_stats': stats,
            'bottlenecks': bottlenecks,
            'patterns': [],
            'recommendations': []
        }

# --- RecommendationEngine ---
class RecommendationEngine:
    """Suggests optimizations and actionable advice."""
    def generate(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recs = []
        stats = analysis.get('job_stats', {})
        if stats.get('failed_jobs'):
            recs.append("Some jobs failed. Check logs for errors and consider increasing executor memory or reviewing input data quality. See: https://spark.apache.org/docs/latest/job-scheduling.html#failure-recovery")
        if stats.get('slow_jobs'):
            recs.append("Several jobs are significantly slower than average. Consider increasing parallelism, tuning partitions, or caching intermediate results. See: https://spark.apache.org/docs/latest/tuning.html")
        if analysis.get('bottlenecks'):
            recs.append("Detected bottleneck jobs (duration > 2x average). Investigate stages for skew, shuffles, or GC overhead. See: https://spark.apache.org/docs/latest/tuning.html#data-skew")
        if not recs:
            recs.append("No major issues detected. Review job durations and resource usage for further optimization. Consider increasing parallelism or tuning shuffle operations for better performance.")
        return recs

# --- Reporter ---
class Reporter:
    """Generates HTML/Markdown reports with links and summaries."""
    def generate(self, parsed: Dict[str, Any], analysis: Dict[str, Any], recommendations: List[str], output_file: str = "spark_analysis_report.html") -> None:
        """Generate a modern HTML report with summary, bottlenecks, and recommendations."""
        stats = analysis.get('job_stats', {})
        bottlenecks = analysis.get('bottlenecks', [])
        failed_jobs = stats.get('failed_jobs', [])
        slow_jobs = stats.get('slow_jobs', [])
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

        html = [
            '<!DOCTYPE html>',
            '<html><head><meta charset="utf-8">',
            '<title>Spark Application Analysis Report</title>',
            '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">',
            '<style>',
            'body { font-family: Inter, Arial, sans-serif; background: #f8f9fa; color: #222; margin: 0; padding: 0; }',
            '.navbar { background: #fff; padding: 1rem 2rem; box-shadow: 0 2px 4px rgba(0,0,0,0.07); position: sticky; top: 0; z-index: 100; }',
            '.nav-links { display: flex; gap: 2rem; margin-top: 1rem; }',
            '.nav-links a { color: #222; text-decoration: none; font-weight: 500; padding: 0.5rem 1rem; border-radius: 8px; transition: background 0.2s; }',
            '.nav-links a:hover { background: #e3eafc; }',
            '.container { max-width: 900px; margin: 2rem auto; background: #fff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); padding: 2rem; }',
            '.summary-cards { display: flex; gap: 2rem; margin-bottom: 2rem; }',
            '.summary-card { flex: 1; background: #f5f6fa; border-radius: 8px; padding: 1.5rem; box-shadow: 0 1px 2px rgba(0,0,0,0.03); text-align: center; }',
            '.section { margin-bottom: 2.5rem; }',
            '.section h2 { margin-top: 0; }',
            '.bottleneck, .failed, .slow { background: #fff3cd; border-left: 4px solid #e67e22; margin: 1rem 0; padding: 1rem; border-radius: 6px; }',
            '.recommendation { background: #e3eafc; border-left: 4px solid #3498db; margin: 1rem 0; padding: 1rem; border-radius: 6px; }',
            '.job-table { width: 100%; border-collapse: collapse; margin-top: 1rem; }',
            '.job-table th, .job-table td { border: 1px solid #eee; padding: 0.5rem 0.75rem; text-align: left; }',
            '.job-table th { background: #f5f6fa; }',
            '</style>',
            '</head><body>',
            '<div class="navbar"><h1><i class="fas fa-chart-line"></i> Spark Application Analysis</h1>',
            '<div class="nav-links">',
            '<a href="#summary">Summary</a>',
            '<a href="#bottlenecks">Bottlenecks</a>',
            '<a href="#failures">Failures</a>',
            '<a href="#recommendations">Recommendations</a>',
            '</div></div>',
            '<div class="container">',
            '<section id="summary" class="section">',
            '<h2>Summary</h2>',
            '<div class="summary-cards">',
            f'<div class="summary-card"><h3>Total Jobs</h3><div>{stats.get("total_jobs", 0)}</div></div>',
            f'<div class="summary-card"><h3>Avg Duration</h3><div>{fmt_sec(stats.get("avg_duration", 0))}</div></div>',
            f'<div class="summary-card"><h3>Success Rate</h3><div>{stats.get("success_rate", 0):.1f}%</div></div>',
            '</div></section>',
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
            '<section id="jobs" class="section">',
            '<h2>All Jobs</h2>',
            '<table class="job-table"><tr><th>Job ID</th><th>Status</th><th>Start Time</th><th>End Time</th><th>Duration (s)</th><th>Description</th><th>Query</th></tr>',
            ''.join([
                f'<tr><td>{job["job_id"]}</td><td>{job["status"]}</td><td>{fmt_time(job.get("submission_time"))}</td><td>{fmt_time(job.get("completion_time"))}</td><td>{fmt_sec(job["duration"])}</td><td>{job["description"]}</td><td><code>{find_sql_for_job(job, sql_ops)}</code></td></tr>'
                for job in parsed.get('jobs', [])
            ]),
            '</table></section>',
            '</div></body></html>'
        ]
        html_content = ''.join(html)
        with open(output_file, "w") as f:
            f.write(html_content)
        print(f"Report generated: {output_file}")

# --- Main CLI ---
def main(args):
    # Load events
    loader = DataLoader(args.s3_config)
    if args.s3_config:
        events = loader.load_events_from_s3(args.history_file)
    else:
        events = loader.load_events_from_file(args.history_file)
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
    recommendations = recommender.generate(analysis)
    # Report
    reporter = Reporter()
    reporter.generate(parsed, analysis, recommendations, output_file=args.report)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Spark Application Analyzer")
    parser.add_argument('history_file', help='Path to Spark history log file')
    parser.add_argument('--s3-config', help='Path to S3 config JSON', default=None)
    parser.add_argument('--report', help='Output HTML report file', default='spark_analysis_report.html')
    args = parser.parse_args()
    main(args)
