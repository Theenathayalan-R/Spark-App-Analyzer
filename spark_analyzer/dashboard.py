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

class SparkDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)
        self.metrics_data = {}
        self.setup_layout()
        
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
            dcc.Graph(id='resource-heatmap')
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
        # Convert metrics to matrix form
        # Rows: Executors, Columns: Time intervals
        # Values: Resource utilization
        
        executor_metrics = metrics.get('executors', {})
        times = list(range(len(next(iter(executor_metrics.values())))))
        executors = list(executor_metrics.keys())
        
        utilization_matrix = [
            [executor_metrics[e].get(t, 0) for t in times]
            for e in executors
        ]
        
        fig = go.Figure(data=go.Heatmap(
            z=utilization_matrix,
            x=times,
            y=executors,
            colorscale='RdYlBu_r'
        ))
        
        fig.update_layout(
            title='Resource Utilization Heatmap',
            xaxis_title='Time',
            yaxis_title='Executor ID'
        )
        
        return fig
    
    def run(self, host: str = 'localhost', port: int = 8050):
        """Run the dashboard server"""
        self.app.run(host=host, port=port)

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
