"""
PySpark Code Pattern Analyzer for detecting common anti-patterns and performance issues
"""
from typing import Dict, List, Optional
from dataclasses import dataclass
import re

@dataclass
class PySparkIssue:
    pattern: str
    severity: str  # 'HIGH', 'MEDIUM', 'LOW'
    category: str  # 'memory', 'performance', 'best_practice'
    description: str
    recommendation: str
    code_fix: str
    affected_jobs: List[int]
    estimated_impact: str

class PySparkCodeAnalyzer:
    """Analyzes PySpark usage patterns and detects common anti-patterns"""
    
    def __init__(self):
        self.antipatterns = self._load_antipatterns()
    
    def analyze_pyspark_patterns(self, parsed_data: Dict) -> List[PySparkIssue]:
        """Analyze parsed Spark data for PySpark-specific issues"""
        issues = []
        
        sql_operations = parsed_data.get('sql_operations', [])
        jobs = parsed_data.get('jobs', [])
        metrics = parsed_data.get('metrics', {})
        
        collect_issues = self._detect_collect_abuse(sql_operations, jobs, metrics)
        issues.extend(collect_issues)
        
        cache_issues = self._detect_missing_cache(sql_operations, jobs)
        issues.extend(cache_issues)
        
        udf_issues = self._detect_inefficient_udfs(sql_operations)
        issues.extend(udf_issues)
        
        shuffle_issues = self._detect_shuffle_problems(metrics, jobs)
        issues.extend(shuffle_issues)
        
        memory_issues = self._detect_memory_pressure(metrics, jobs)
        issues.extend(memory_issues)
        
        gc_issues = self._detect_gc_overhead(metrics, jobs)
        issues.extend(gc_issues)
        
        return sorted(issues, key=lambda x: self._severity_score(x.severity), reverse=True)
    
    def _detect_collect_abuse(self, sql_ops: List[Dict], jobs: List[Dict], metrics: Dict) -> List[PySparkIssue]:
        """Detect potential collect() abuse on large datasets"""
        issues = []
        
        for job in jobs:
            job_desc = job.get('description', '').lower()
            job_id = job.get('job_id')
            
            if 'collect' in job_desc and job.get('duration', 0) > 10000:  # > 10 seconds
                issues.append(PySparkIssue(
                    pattern='collect_abuse',
                    severity='HIGH',
                    category='memory',
                    description=f'Job {job_id} uses collect() and takes {job["duration"]/1000:.1f}s to complete',
                    recommendation='Avoid collect() on large datasets. Use take(), first(), or write to storage instead.',
                    code_fix='''# Instead of:
result = df.collect()  # Brings all data to driver

sample = df.take(100)  # Get limited sample
df.write.mode("overwrite").parquet("output_path")  # Write to storage''',
                    affected_jobs=[job_id],
                    estimated_impact='High - Can cause driver OOM and performance degradation'
                ))
        
        return issues
    
    def _detect_missing_cache(self, sql_ops: List[Dict], jobs: List[Dict]) -> List[PySparkIssue]:
        """Detect DataFrames that are likely reused but not cached"""
        issues = []
        
        job_descriptions = {}
        for job in jobs:
            desc = job.get('description', '')
            if desc and desc != 'Unknown':
                if desc not in job_descriptions:
                    job_descriptions[desc] = []
                job_descriptions[desc].append(job)
        
        for desc, job_list in job_descriptions.items():
            if len(job_list) > 1:
                total_duration = sum(job.get('duration', 0) for job in job_list)
                if total_duration > 20000:  # > 20 seconds total
                    job_ids = [job.get('job_id') for job in job_list]
                    issues.append(PySparkIssue(
                        pattern='missing_cache',
                        severity='MEDIUM',
                        category='performance',
                        description=f'Multiple jobs ({len(job_list)}) with similar operations suggest DataFrame reuse without caching',
                        recommendation='Cache frequently accessed DataFrames to avoid recomputation.',
                        code_fix='''# Add caching for reused DataFrames:
df_filtered = df.filter(condition).cache()  # Cache after expensive operations
result1 = df_filtered.groupBy("col1").count()
result2 = df_filtered.groupBy("col2").sum("value")

df_filtered.unpersist()''',
                        affected_jobs=job_ids,
                        estimated_impact='Medium - Reduces redundant computation and improves performance'
                    ))
        
        return issues
    
    def _detect_inefficient_udfs(self, sql_ops: List[Dict]) -> List[PySparkIssue]:
        """Detect potential inefficient UDF usage"""
        issues = []
        
        for sql_op in sql_ops:
            query = sql_op.get('description', '').upper()
            exec_id = sql_op.get('execution_id')
            
            if any(pattern in query for pattern in ['UDF', 'FUNCTION', 'PYTHON']):
                issues.append(PySparkIssue(
                    pattern='inefficient_udf',
                    severity='MEDIUM',
                    category='performance',
                    description=f'Query {exec_id} may be using UDFs where built-in functions could be more efficient',
                    recommendation='Replace Python UDFs with built-in Spark SQL functions when possible.',
                    code_fix='''# Instead of Python UDF:
from pyspark.sql.functions import udf
@udf("string")
def custom_upper(s):
    return s.upper() if s else None
df.withColumn("upper_col", custom_upper("col"))

from pyspark.sql.functions import upper
df.withColumn("upper_col", upper("col"))''',
                    affected_jobs=[],
                    estimated_impact='Medium - Built-in functions are optimized and avoid Python serialization overhead'
                ))
        
        return issues
    
    def _detect_shuffle_problems(self, metrics: Dict, jobs: List[Dict]) -> List[PySparkIssue]:
        """Detect shuffle-related performance issues"""
        issues = []
        shuffle_metrics = metrics.get('shuffle', {})
        
        for stage_id, shuffle_data in shuffle_metrics.items():
            spill_size = shuffle_data.get('disk_spill_size', 0)
            shuffle_read = shuffle_data.get('shuffle_read_bytes', 0)
            shuffle_write = shuffle_data.get('shuffle_write_bytes', 0)
            
            if spill_size > 100 * 1024 * 1024:  # > 100MB spill
                issues.append(PySparkIssue(
                    pattern='excessive_shuffle_spill',
                    severity='HIGH',
                    category='memory',
                    description=f'Stage {stage_id} has excessive shuffle spill ({spill_size/(1024**2):.1f} MB)',
                    recommendation='Optimize shuffle operations to reduce memory pressure.',
                    code_fix='''# Reduce shuffle spill:
spark.conf.set("spark.executor.memory", "8g")

spark.conf.set("spark.shuffle.memoryFraction", "0.4")

df.repartition(200, "key_column")  # Better partition distribution

from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")''',
                    affected_jobs=[],
                    estimated_impact='High - Reduces disk I/O and improves shuffle performance'
                ))
        
        return issues
    
    def _detect_memory_pressure(self, metrics: Dict, jobs: List[Dict]) -> List[PySparkIssue]:
        """Detect memory pressure issues"""
        issues = []
        memory_metrics = metrics.get('memory', {})
        
        for executor_id, mem_data in memory_metrics.items():
            heap_memory = mem_data.get('JVMHeapMemory', 0)
            spilled_bytes = mem_data.get('DiskBytesSpilled', 0)
            
            if spilled_bytes > 50 * 1024 * 1024:  # > 50MB spilled
                issues.append(PySparkIssue(
                    pattern='memory_pressure',
                    severity='HIGH',
                    category='memory',
                    description=f'Executor {executor_id} shows memory pressure with {spilled_bytes/(1024**2):.1f} MB spilled to disk',
                    recommendation='Increase executor memory or optimize memory usage patterns.',
                    code_fix='''# Address memory pressure:
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryFraction", "0.8")

df.select("needed_columns_only")  # Avoid selecting unnecessary columns
df.filter(condition).cache()     # Filter early and cache

df.withColumn("id", col("id").cast("int"))  # Use smaller data types when possible''',
                    affected_jobs=[],
                    estimated_impact='High - Prevents OOM errors and improves stability'
                ))
        
        return issues
    
    def _detect_gc_overhead(self, metrics: Dict, jobs: List[Dict]) -> List[PySparkIssue]:
        """Detect high GC overhead issues"""
        issues = []
        gc_metrics = metrics.get('gc', {})
        
        for executor_id, gc_data in gc_metrics.items():
            gc_time = gc_data.get('gc_time', 0)
            executor_time = gc_data.get('executor_time', 0)
            
            if executor_time > 0:
                gc_ratio = gc_time / executor_time
                if gc_ratio > 0.1:  # > 10% GC time
                    severity = 'HIGH' if gc_ratio > 0.2 else 'MEDIUM'
                    issues.append(PySparkIssue(
                        pattern='high_gc_overhead',
                        severity=severity,
                        category='memory',
                        description=f'Executor {executor_id} has high GC overhead ({gc_ratio:.1%} of execution time)',
                        recommendation='Optimize memory usage and GC settings to reduce garbage collection overhead.',
                        code_fix='''# Reduce GC overhead:
spark.conf.set("spark.executor.memory", "8g")

spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")

spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200")

df.mapPartitions(lambda partition: process_partition(partition))  # Process in batches''',
                        affected_jobs=[],
                        estimated_impact=f'{"High" if severity == "HIGH" else "Medium"} - Reduces CPU overhead from garbage collection'
                    ))
        
        return issues
    
    def _severity_score(self, severity: str) -> int:
        """Convert severity to numeric score for sorting"""
        return {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}.get(severity, 0)
    
    def _load_antipatterns(self) -> Dict:
        """Load known PySpark anti-patterns"""
        return {
            'collect_large_dataset': {
                'description': 'Using collect() on large datasets',
                'impact': 'Driver OOM, performance degradation'
            },
            'missing_cache': {
                'description': 'Not caching frequently accessed DataFrames',
                'impact': 'Redundant computation, slower performance'
            },
            'inefficient_udf': {
                'description': 'Using Python UDFs instead of built-in functions',
                'impact': 'Serialization overhead, slower execution'
            },
            'poor_partitioning': {
                'description': 'Suboptimal partitioning strategy',
                'impact': 'Data skew, uneven resource utilization'
            },
            'unnecessary_shuffle': {
                'description': 'Operations causing unnecessary shuffles',
                'impact': 'Network overhead, slower performance'
            }
        }
