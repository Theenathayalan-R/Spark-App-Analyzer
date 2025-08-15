"""
Root Cause Analysis module for automatically identifying performance bottlenecks
"""
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class PerformanceIssue:
    category: str  # e.g., 'memory', 'shuffle', 'skew'
    severity: float  # 0.0 to 1.0
    impact: str
    recommendation: str
    affected_stages: List[int]
    metrics: Dict[str, Any]  # Additional metrics for detailed analysis

class RootCauseAnalyzer:
    def __init__(self):
        self.known_patterns = self._load_performance_patterns()
        self.history: List[Dict] = []  # Store historical metrics for trend analysis
        self._last_critical_path: List[int] = []
        
    def analyze_bottlenecks(self, metrics: Dict) -> List[PerformanceIssue]:
        """Automatically identify root causes of performance issues"""
        issues = []
        self.history.append(metrics)  # Store for trend analysis
        # Critical path & memory pressure analyzed after stages parsed
        # Analyze data skew
        skew_issues = self._detect_data_skew(metrics)
        if skew_issues:
            issues.extend(skew_issues)
        # Analyze shuffle patterns
        shuffle_issues = self._detect_shuffle_bottlenecks(metrics)
        if shuffle_issues:
            issues.extend(shuffle_issues)
        # Analyze resource utilization
        resource_issues = self._detect_resource_bottlenecks(metrics)
        if resource_issues:
            issues.extend(resource_issues)
        # Analyze performance trends
        trend_issues = self._detect_performance_trends()
        if trend_issues:
            issues.extend(trend_issues)
        # Analyze stage dependencies
        dependency_issues = self._detect_stage_dependencies(metrics)
        if dependency_issues:
            issues.extend(dependency_issues)
        # Analyze GC impact
        gc_issues = self._detect_gc_issues(metrics)
        if gc_issues:
            issues.extend(gc_issues)
        # Memory pressure model
        mem_pressure = self._detect_memory_pressure(metrics)
        if mem_pressure:
            issues.extend(mem_pressure)
        # Critical path
        crit_path_issue = self._detect_critical_path(metrics)
        if crit_path_issue:
            issues.append(crit_path_issue)
        return sorted(issues, key=lambda x: x.severity, reverse=True)
        
    def _detect_performance_trends(self) -> List[PerformanceIssue]:
        """Analyze performance trends over time"""
        issues = []
        if len(self.history) < 2:
            return issues
            
        # Analyze job duration trends
        durations = [self._get_avg_job_duration(m) for m in self.history]
        trend = self._calculate_trend(durations)
        
        if trend > 0.1:  # Performance degrading
            trend_metrics = {
                'trend_coefficient': float(trend),
                'current_duration': float(durations[-1]),
                'initial_duration': float(durations[0]),
                'degradation_rate': float((durations[-1] - durations[0]) / len(durations))
            }
            issues.append(PerformanceIssue(
                category='performance_trend',
                severity=min(abs(trend), 1.0),
                impact='Performance is degrading over time',
                recommendation=self._get_trend_recommendations(trend_metrics),
                affected_stages=[],
                metrics=trend_metrics
            ))
            
        return issues
        
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate the trend coefficient for a series of values"""
        if not values:
            return 0.0
        x = np.arange(len(values))
        y = np.array(values)
        z = np.polyfit(x, y, 1)
        mean_val = np.mean(values)
        return float(z[0] / mean_val) if mean_val != 0 else 0.0
        
    def _get_avg_job_duration(self, metrics: Dict) -> float:
        """Calculate average job duration from metrics"""
        jobs = metrics.get('jobs', [])
        durations = [j.get('duration', 0.0) for j in jobs]
        return float(np.mean(durations)) if durations else 0.0
        
    def _get_trend_recommendations(self, metrics: Dict) -> str:
        """Generate recommendations based on performance trends"""
        degradation_rate = metrics['degradation_rate']
        total_change = metrics['current_duration'] - metrics['initial_duration']
        
        if degradation_rate > 0.5:
            return ("Critical performance degradation detected. Review recent changes, "
                   "check for data growth patterns, and consider scaling resources.")
        elif total_change > 0:
            return ("Gradual performance degradation observed. Monitor resource usage, "
                   "review data skew patterns, and optimize heavy operations.")
        else:
            return "Performance is stable. Continue monitoring for changes."
            
    def _detect_stage_dependencies(self, metrics: Dict) -> List[PerformanceIssue]:
        """Analyze stage dependency patterns for bottlenecks"""
        issues = []
        stages = metrics.get('stages', {})
        
        # Build dependency graph
        dependencies = defaultdict(list)
        for stage_id, stage_data in stages.items():
            for parent_id in stage_data.get('parent_ids', []):
                dependencies[parent_id].append(stage_id)
                
        # Find stages with many dependencies
        for stage_id, deps in dependencies.items():
            if len(deps) > 3:  # Stage has many dependent stages
                dep_metrics = {
                    'dependent_count': float(len(deps)),
                    'stage_duration': float(stages.get(stage_id, {}).get('duration', 0))
                }
                issues.append(PerformanceIssue(
                    category='stage_dependency',
                    severity=min(len(deps) / 10.0, 1.0),
                    impact=f'Stage {stage_id} has many dependent stages ({len(deps)})',
                    recommendation='Consider optimizing this stage as it affects many downstream operations',
                    affected_stages=[stage_id] + deps,
                    metrics=dep_metrics
                ))
        return issues
        
    def _detect_gc_issues(self, metrics: Dict) -> List[PerformanceIssue]:
        """Analyze garbage collection impact"""
        issues = []
        gc_metrics = metrics.get('gc', {})
        
        for executor_id, gc_data in gc_metrics.items():
            gc_time = float(gc_data.get('gc_time', 0))
            total_time = float(gc_data.get('executor_time', 0))
            
            if total_time > 0:
                gc_ratio = gc_time / total_time
                if gc_ratio > 0.1:  # GC takes >10% of time
                    gc_metrics = {
                        'gc_ratio': gc_ratio,
                        'gc_time': gc_time,
                        'total_time': total_time
                    }
                    issues.append(PerformanceIssue(
                        category='gc_overhead',
                        severity=gc_ratio,
                        impact=f'High GC overhead in executor {executor_id} ({gc_ratio:.1%})',
                        recommendation=self._get_gc_recommendations(gc_metrics),
                        affected_stages=[],
                        metrics=gc_metrics
                    ))
        return issues
        
    def _get_gc_recommendations(self, metrics: Dict) -> str:
        """Generate GC optimization recommendations"""
        gc_ratio = metrics['gc_ratio']
        
        if gc_ratio > 0.2:
            return ("Critical: Increase executor memory, adjust spark.memory.fraction, "
                   "and consider using G1GC with -XX:+UseG1GC")
        elif gc_ratio > 0.1:
            return ("Review memory usage patterns and consider increasing "
                   "spark.memory.fraction if persistent")
        else:
            return "Monitor GC patterns for potential memory pressure"
    
    def _detect_data_skew(self, metrics: Dict) -> List[PerformanceIssue]:
        """Detect data skew by analyzing task duration distributions"""
        issues = []
        for stage_id, tasks in metrics.get('tasks', {}).items():
            durations = [t.get('duration', 0) for t in tasks]
            if durations:
                mean_val = float(np.mean(durations)) if durations else 0.0
                if len(durations) < 2 or mean_val <= 0:
                    continue  # Not enough data to assess skew safely
                cv = float(np.std(durations) / mean_val) if mean_val else 0.0
                if cv > 0.5:  # High coefficient of variation indicates skew
                    skew_metrics = {
                        'coefficient_of_variation': float(cv),
                        'max_duration': float(max(durations)),
                        'min_duration': float(min(durations)),
                        'avg_duration': float(mean_val)
                    }
                    issues.append(PerformanceIssue(
                        category='data_skew',
                        severity=min(float(cv), 1.0),
                        impact=f'Stage {stage_id} shows significant data skew (CV: {cv:.2f})',
                        recommendation=self._get_skew_recommendations(float(cv), tasks),
                        affected_stages=[stage_id],
                        metrics=skew_metrics
                    ))
        return issues
    
    def _detect_shuffle_bottlenecks(self, metrics: Dict) -> List[PerformanceIssue]:
        """Identify shuffle-related performance issues"""
        issues = []
        shuffle_metrics = metrics.get('shuffle', {})
        
        for stage_id, shuffle_data in shuffle_metrics.items():
            spill_size = shuffle_data.get('disk_spill_size', 0)
            shuffle_read = shuffle_data.get('shuffle_read_bytes', 0)
            shuffle_write = shuffle_data.get('shuffle_write_bytes', 0)
            
            if spill_size > 1e9:  # More than 1GB spill
                spill_metrics = {
                    'spill_size': float(spill_size),
                    'shuffle_read': float(shuffle_read),
                    'shuffle_write': float(shuffle_write),
                    'spill_ratio': float(spill_size / (shuffle_read + shuffle_write)) if (shuffle_read + shuffle_write) > 0 else 0.0
                }
                issues.append(PerformanceIssue(
                    category='shuffle_spill',
                    severity=float(min(spill_size / 1e10, 1.0)),  # Scale based on spill size
                    impact=f'High shuffle spill in stage {stage_id} ({spill_size/(1024**3):.2f} GB)',
                    recommendation=self._get_shuffle_recommendations(spill_metrics),
                    affected_stages=[stage_id],
                    metrics=spill_metrics
                ))
        return issues
        
    def _get_shuffle_recommendations(self, metrics: Dict) -> str:
        """Generate specific shuffle optimization recommendations"""
        spill_ratio = metrics['spill_ratio']
        spill_size = metrics['spill_size']
        
        recommendations = []
        if spill_ratio > 0.5:
            recommendations.append("Increase spark.shuffle.memoryFraction")
        if spill_size > 5e9:  # 5GB
            recommendations.append("Consider reducing partition count or implementing partial aggregation")
        if len(recommendations) == 0:
            recommendations.append("Monitor shuffle patterns and adjust memory settings if needed")
            
        return "; ".join(recommendations)
        
    def _get_skew_recommendations(self, cv: float, tasks: List[Dict]) -> str:
        """Generate specific recommendations based on skew severity"""
        if cv > 0.8:
            return ("High priority: Use salting for key distribution, consider custom partitioning, "
                   "or implement skew hint for join operations")
        elif cv > 0.6:
            return ("Medium priority: Consider using range partitioning or adjusting "
                   "spark.sql.adaptive.skewJoin.enabled=true")
        else:
            return "Monitor skew patterns and consider implementing custom partitioning if it increases"
    
    def _detect_resource_bottlenecks(self, metrics: Dict) -> List[PerformanceIssue]:
        """Identify resource utilization bottlenecks"""
        issues = []
        executor_metrics = metrics.get('executors', {})
        
        # Check CPU utilization
        cpu_utils = [float(e.get('cpu_usage', 0)) for e in executor_metrics.values()]
        if cpu_utils:
            cpu_util = float(np.mean(cpu_utils))
            if cpu_util > 0.85:
                cpu_metrics = {
                    'avg_cpu_util': cpu_util,
                    'max_cpu_util': float(np.max(cpu_utils)),
                    'min_cpu_util': float(np.min(cpu_utils)),
                    'std_cpu_util': float(np.std(cpu_utils))
                }
                issues.append(PerformanceIssue(
                    category='cpu_bottleneck',
                    severity=cpu_util,
                    impact=f'High CPU utilization across executors ({cpu_util:.1%})',
                    recommendation=self._get_cpu_recommendations(cpu_metrics),
                    affected_stages=[],
                    metrics=cpu_metrics
                ))
            
        # Check memory utilization
        mem_utils = [float(e.get('memory_usage', 0)) for e in executor_metrics.values()]
        if mem_utils:
            mem_util = float(np.mean(mem_utils))
            if mem_util > 0.9:
                mem_metrics = {
                    'avg_mem_util': mem_util,
                    'max_mem_util': float(np.max(mem_utils)),
                    'min_mem_util': float(np.min(mem_utils)),
                    'std_mem_util': float(np.std(mem_utils))
                }
                issues.append(PerformanceIssue(
                    category='memory_bottleneck',
                    severity=mem_util,
                    impact=f'High memory utilization across executors ({mem_util:.1%})',
                    recommendation=self._get_memory_recommendations(mem_metrics),
                    affected_stages=[],
                    metrics=mem_metrics
                ))
            
        return issues
        
    def _get_cpu_recommendations(self, metrics: Dict) -> str:
        """Generate CPU optimization recommendations"""
        avg_util = metrics['avg_cpu_util']
        std_util = metrics['std_cpu_util']
        
        if avg_util > 0.95:
            return ("Critical: Increase executor cores immediately or optimize CPU-intensive operations. "
                   "Consider enabling dynamic allocation.")
        elif std_util > 0.2:
            return ("High CPU variance detected. Review task distribution and consider "
                   "implementing custom partitioning for better balance.")
        else:
            return ("Consider increasing executor cores or optimizing CPU-intensive operations. "
                   "Monitor for sustained high utilization.")
            
    def _get_memory_recommendations(self, metrics: Dict) -> str:
        """Generate memory optimization recommendations"""
        avg_util = metrics['avg_mem_util']
        max_util = metrics['max_mem_util']
        
        if max_util > 0.95:
            return ("Critical: Increase executor memory immediately. Consider using spill to disk "
                   "or implementing partial aggregations.")
        elif avg_util > 0.9:
            return ("High memory pressure. Review caching strategy and consider increasing "
                   "spark.memory.fraction or implementing data skipping.")
        else:
            return ("Monitor memory usage patterns and optimize memory-intensive operations "
                   "if utilization continues to increase.")
    
    def _load_performance_patterns(self) -> Dict:
        """Load known performance issue patterns and signatures"""
        return {
            'spill_heavy': {
                'pattern': 'high_disk_spill_ratio',
                'threshold': 0.3
            },
            'cpu_bound': {
                'pattern': 'high_cpu_wait_time',
                'threshold': 0.8
            },
            'memory_pressure': {
                'pattern': 'high_gc_time',
                'threshold': 0.2
            }
        }
    
    def _detect_memory_pressure(self, metrics: Dict) -> List[PerformanceIssue]:
        """Estimate memory pressure by comparing spill size to shuffle volume and GC ratio."""
        issues: List[PerformanceIssue] = []
        shuffle = metrics.get('shuffle', {})
        gc = metrics.get('gc', {})
        avg_gc_ratio = 0.0
        ratios = []
        for ex, data in gc.items():
            t = float(data.get('executor_time', 0) or 0)
            if t > 0:
                ratios.append(float(data.get('gc_time', 0))/t)
        if ratios:
            avg_gc_ratio = float(np.mean(ratios))
        for stage_id, sdata in shuffle.items():
            read_w = float(sdata.get('shuffle_read_bytes', 0) + sdata.get('shuffle_write_bytes', 0))
            spill = float(sdata.get('disk_spill_size', 0))
            if read_w <= 0:
                continue
            spill_ratio = spill / read_w
            if spill_ratio > 0.6 or (spill_ratio > 0.3 and avg_gc_ratio > 0.15):
                issues.append(PerformanceIssue(
                    category='memory_pressure_model',
                    severity=min(1.0, (spill_ratio + avg_gc_ratio)/1.5),
                    impact=f'Stage {stage_id} shows high spill vs shuffle volume (spill_ratio={spill_ratio:.2f}, avg_gc={avg_gc_ratio:.2%})',
                    recommendation='Increase executor memory / tune partitions; consider caching & adjust spark.memory.fraction',
                    affected_stages=[stage_id],
                    metrics={'spill_ratio': spill_ratio, 'avg_gc_ratio': avg_gc_ratio, 'spill_bytes': spill, 'shuffle_bytes': read_w}
                ))
        return issues

    def _detect_critical_path(self, metrics: Dict) -> Optional[PerformanceIssue]:
        """Compute longest stage dependency path weighted by stage durations."""
        stages: Dict[int, Dict[str, Any]] = metrics.get('stages', {})  # type: ignore
        if not stages:
            return None
        # Build DAG: parent -> children
        children = defaultdict(list)
        for sid, sdata in stages.items():
            for p in sdata.get('parent_ids', []):
                children[p].append(sid)
        # Memoized DFS
        memo: Dict[int, Tuple[float, List[int]]] = {}
        def dfs(node: int) -> Tuple[float, List[int]]:
            if node in memo:
                return memo[node]
            dur = float(stages.get(node, {}).get('duration', 0) or 0)
            best = (dur, [node])
            for ch in children.get(node, []):
                c_dur, c_path = dfs(ch)
                if dur + c_dur > best[0]:
                    best = (dur + c_dur, [node] + c_path)
            memo[node] = best
            return best
        # Roots = stages without parents
        roots = [sid for sid, sdata in stages.items() if not sdata.get('parent_ids')]
        if not roots:
            return None
        best_overall = (0.0, [])
        for r in roots:
            total, path = dfs(r)
            if total > best_overall[0]:
                best_overall = (total, path)
        if best_overall[0] <= 0 or len(best_overall[1]) < 2:
            return None
        self._last_critical_path = best_overall[1]
        return PerformanceIssue(
            category='critical_path',
            severity=min(1.0, best_overall[0] / (best_overall[0] + 1_000)),  # normalized simple
            impact=f'Critical path duration {best_overall[0]:.0f} ms across stages {best_overall[1]}',
            recommendation='Optimize earliest long stages in the critical path to reduce end-to-end time',
            affected_stages=best_overall[1],
            metrics={'path': best_overall[1], 'path_duration_ms': float(best_overall[0])}
        )
