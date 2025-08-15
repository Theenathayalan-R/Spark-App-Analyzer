"""
Interactive Dashboard module for real-time visualization of Spark metrics
"""
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List
import time
import random

class SparkDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)
        self.metrics_data: Dict[str, Dict] = {}
        self._last_refresh = time.time()
        self.setup_layout()
        # Register callbacks
        try:  # pragma: no cover
            self._register_callbacks()
        except Exception:
            pass

    # Helper forward declarations (no-op to satisfy static analysis)
    def _simulate_metric_increment(self, metric: str):  # pragma: no cover
        pass
    def _build_metric_series(self, metric: str):  # pragma: no cover
        return go.Figure()

    def _register_callbacks(self):  # pragma: no cover
        """Register Dash callbacks for periodic refresh and interactions."""
        # Implementation appended later in file (defined below to satisfy type checkers)
        pass
        
    def setup_layout(self):
        """Setup the dashboard layout with interactive components"""
        self.app.layout = html.Div([
            html.H1("Spark Performance Dashboard"),
            
            # Time range selector
            dcc.RangeSlider(
                id='time-slider',
                min=0,
                max=100,
                step=1,
                marks={0: 'Start', 100: 'End'},
                value=[0, 100]
            ),
            
            # Metrics selector
            dcc.Dropdown(
                id='metric-selector',
                options=[
                    {'label': 'Job Duration', 'value': 'duration'},
                    {'label': 'Memory Usage', 'value': 'memory'},
                    {'label': 'CPU Usage', 'value': 'cpu'},
                    {'label': 'Shuffle Size', 'value': 'shuffle'}
                ],
                value='duration'
            ),
            
            # Main metrics graph
            dcc.Graph(id='main-graph'),
            
            # Performance issues panel
            html.Div([
                html.H3("Performance Issues"),
                html.Div(id='issues-panel')
            ]),
            
            # Stage timeline
            dcc.Graph(id='stage-timeline'),
            
            # Resource utilization heatmap
            dcc.Graph(id='resource-heatmap'),
            # Auto refresh interval (simulated streaming)
            dcc.Interval(id='refresh-interval', interval=3000, n_intervals=0)
        ])
        
    def update_metrics(self, new_metrics: Dict):
        """Update dashboard with new metrics data"""
        self.metrics_data.update(new_metrics)
        
    @staticmethod
    def create_stage_timeline(stages: List[Dict]) -> go.Figure:
        """Create Gantt chart for stage execution timeline"""
        df = pd.DataFrame(stages)
        fig = px.timeline(
            df,
            x_start='start_time',
            x_end='end_time',
            y='stage_id',
            color='status',
            title='Stage Execution Timeline'
        )
        return fig
    
    @staticmethod
    def create_resource_heatmap(metrics: Dict) -> go.Figure:
        """Create heatmap of resource utilization across executors"""
        executor_metrics = metrics.get('executors', {})
        if not executor_metrics:
            return go.Figure()
        # Expect structure: executors -> list of samples [{'cpu':..,'mem':..}]
        samples_len = max((len(v) for v in executor_metrics.values()), default=0)
        times = list(range(samples_len))
        executors = list(executor_metrics.keys())
        # Use memory utilization if present else cpu
        utilization_matrix = []
        for e in executors:
            samples = executor_metrics[e]
            row = []
            for i in range(samples_len):
                if i < len(samples):
                    sample = samples[i]
                    row.append(sample.get('mem', sample.get('cpu', 0)))
                else:
                    row.append(0)
            utilization_matrix.append(row)
        fig = go.Figure(data=go.Heatmap(z=utilization_matrix, x=times, y=executors, colorscale='Viridis'))
        fig.update_layout(title='Executor Memory/CPU Utilization Heatmap', xaxis_title='Sample', yaxis_title='Executor')
        return fig
    
    def run(self, host: str = 'localhost', port: int = 8050):
        """Run the dashboard server"""
        self.app.run(host=host, port=port)

    def _register_callbacks(self):
        @self.app.callback(
            Output('main-graph', 'figure'),
            Output('issues-panel', 'children'),
            Output('stage-timeline', 'figure'),
            Output('resource-heatmap', 'figure'),
            Input('refresh-interval', 'n_intervals'),
            Input('metric-selector', 'value')
        )
        def refresh(n_intervals: int, metric: str):  # pragma: no cover (UI)
            self._simulate_metric_increment(metric)
            issues_children = []
            issues = self.metrics_data.get('issues', [])
            for iss in issues[:5]:
                issues_children.append(html.Div(f"[{iss.get('category')}] {iss.get('impact')}", style={'margin':'4px 0'}))
            main_fig = self._build_metric_series(metric)
            stage_list = self.metrics_data.get('stages') or []
            if isinstance(stage_list, dict):  # normalize
                stage_list = list(stage_list.values())
            timeline_fig = self.create_stage_timeline(stage_list) if stage_list else go.Figure()
            heatmap_fig = self.create_resource_heatmap(self.metrics_data.get('resource_history', {})) if self.metrics_data.get('resource_history') else go.Figure()
            return main_fig, issues_children, timeline_fig, heatmap_fig
    
    def _simulate_metric_increment(self, metric: str):  # pragma: no cover
        series = self.metrics_data.setdefault('series', {}).setdefault(metric, [])
        series.append({ 't': len(series), 'v': random.uniform(0, 100) })
        if len(series) > 200:
            series.pop(0)

    def _build_metric_series(self, metric: str):  # pragma: no cover
        series = self.metrics_data.get('series', {}).get(metric, [])
        if not series:
            return go.Figure()
        df = pd.DataFrame(series)
        fig = px.line(df, x='t', y='v', title=f'Metric: {metric}')
        return fig
    
def create_performance_summary(metrics: Dict) -> html.Div:
    """Create a summary of current performance metrics"""
    return html.Div([
        html.H4("Performance Summary"),
        html.Ul([
            html.Li(f"Active Jobs: {metrics.get('active_jobs', 0)}"),
            html.Li(f"Completed Stages: {metrics.get('completed_stages', 0)}"),
            html.Li(f"Failed Tasks: {metrics.get('failed_tasks', 0)}"),
            html.Li(f"Average Job Duration: {metrics.get('avg_job_duration', 0):.2f}s"),
            html.Li(f"Memory Usage: {metrics.get('memory_usage', 0):.1f}%"),
            html.Li(f"Disk Spill: {metrics.get('disk_spill', 0)/(1024*1024):.2f} MB")
        ])
    ])
