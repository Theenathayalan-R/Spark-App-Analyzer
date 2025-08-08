"""
SQL Query Optimization module for analyzing and improving Spark SQL performance
"""
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class QueryOptimization:
    query_id: str
    original_plan: str
    optimized_plan: str
    estimated_improvement: float
    recommendations: List[str]

class SQLOptimizer:
    def __init__(self):
        self.query_patterns = self._load_query_patterns()
    
    def analyze_query(self, query_metrics: Dict) -> QueryOptimization:
        """Analyze SQL query execution plan and suggest optimizations"""
        plan = query_metrics.get('query_plan', '')
        query = query_metrics.get('description', '')
        
        optimizations = []
        
        # Analyze query pattern
        if 'GROUP BY' in query.upper():
            optimizations.extend(self._analyze_aggregation(query))
            
        if 'JOIN' in query.upper():
            optimizations.extend(self._analyze_joins(query))
            
        if 'WHERE' in query.upper():
            optimizations.extend(self._analyze_filters(query))
            
        # Check for cartesian products
        if self._has_cartesian_product(plan):
            optimizations.append(
                "Add explicit JOIN conditions to avoid expensive cartesian products"
            )
        
        # Advanced optimizations based on query pattern
        if self._is_complex_aggregation(query):
            optimizations.append(
                "Consider using two-phase aggregation for better performance:\n" +
                "1. First phase: Partial aggregation with coarse grouping\n" +
                "2. Second phase: Final aggregation with desired grouping"
            )
            
        if self._has_potential_data_skew(query):
            optimizations.append(
                "Enable adaptive query execution and skew join handling:\n" +
                "SET spark.sql.adaptive.enabled=true\n" +
                "SET spark.sql.adaptive.skewJoin.enabled=true"
            )
        
        # Check for filter pushdown opportunities
        if not self._has_filter_pushdown(plan):
            optimizations.append(
                "Optimize filter placement:\n" +
                "1. Push filters before joins when possible\n" +
                "2. Use partition pruning for partitioned tables\n" +
                "3. Consider creating bloom filters for selective joins"
            )
        
        return QueryOptimization(
            query_id=query_metrics.get('query_id', ''),
            original_plan=plan,
            optimized_plan=self._generate_optimized_plan(plan),
            estimated_improvement=self._estimate_improvement(query, optimizations),
            recommendations=optimizations
        )
    
    def _has_cartesian_product(self, plan: str) -> bool:
        """Check if query plan contains cartesian products"""
        return 'Cross Join' in plan
    
    def _has_shuffle_join(self, plan: str) -> bool:
        """Check if query plan contains shuffle joins"""
        return 'SortMergeJoin' in plan or 'ShuffleHashJoin' in plan
    
    def _has_filter_pushdown(self, plan: str) -> bool:
        """Check if query plan shows filter pushdown"""
        return 'PushedFilters' in plan
    
    def _generate_optimized_plan(self, original_plan: str) -> str:
        """Generate an optimized version of the query plan"""
        # Implementation would include query plan optimization logic
        return original_plan  # Placeholder
    
    def _estimate_improvement(self, plan: str, optimizations: List[str]) -> float:
        """Estimate potential performance improvement"""
        improvement = 0.0
        
        if 'broadcast' in ' '.join(optimizations).lower():
            improvement += 0.3  # 30% improvement for broadcast joins
            
        if 'filter' in ' '.join(optimizations).lower():
            improvement += 0.2  # 20% improvement for filter pushdown
            
        return min(improvement, 0.9)  # Cap at 90% improvement
    
    def _analyze_aggregation(self, query: str) -> List[str]:
        """Analyze and optimize aggregation operations"""
        recommendations = []
        
        if 'COUNT(DISTINCT' in query or 'COUNT(DISTINCT' in query:
            recommendations.append(
                "For COUNT DISTINCT, consider using approx_count_distinct() " +
                "for better performance with minimal accuracy impact"
            )
            
        if 'GROUP BY' in query and ('SUM(' in query or 'AVG(' in query):
            recommendations.append(
                "For grouped aggregations:\n" +
                "1. SET spark.sql.shuffle.partitions based on data volume\n" +
                "2. Consider pre-aggregating at a coarser level first"
            )
            
        return recommendations
    
    def _analyze_joins(self, query: str) -> List[str]:
        """Analyze and optimize join operations"""
        recommendations = []
        
        if 'JOIN' in query:
            recommendations.append(
                "Optimize joins:\n" +
                "1. Order tables from smallest to largest in joins\n" +
                "2. Use broadcast hints for small tables (< 10GB)\n" +
                "3. Consider using bucketing for frequently joined columns"
            )
            
        if query.count('JOIN') > 2:
            recommendations.append(
                "For multi-table joins:\n" +
                "1. Optimize join order using cost-based optimization\n" +
                "2. SET spark.sql.cbo.enabled=true\n" +
                "3. Ensure table statistics are up to date"
            )
            
        return recommendations
    
    def _analyze_filters(self, query: str) -> List[str]:
        """Analyze and optimize filter conditions"""
        recommendations = []
        
        if 'WHERE' in query and ('OR' in query or 'IN' in query):
            recommendations.append(
                "For complex filter conditions:\n" +
                "1. Use IN clauses instead of multiple OR conditions\n" +
                "2. Consider creating bloom filters for selective joins"
            )
            
        if 'LIKE' in query:
            recommendations.append(
                "For LIKE patterns:\n" +
                "1. Avoid leading wildcards when possible\n" +
                "2. Consider using predicate pushdown-friendly operators"
            )
            
        return recommendations
    
    def _is_complex_aggregation(self, query: str) -> bool:
        """Detect complex aggregation patterns"""
        complex_patterns = [
            'GROUP BY.*HAVING',
            'COUNT.*DISTINCT',
            'GROUP BY.*ORDER BY',
            'WINDOW'
        ]
        return any(pattern.lower() in query.lower() for pattern in complex_patterns)
    
    def _has_potential_data_skew(self, query: str) -> bool:
        """Detect potential data skew situations"""
        skew_patterns = [
            'GROUP BY',
            'JOIN.*ON',
            'ORDER BY',
            'PARTITION BY'
        ]
        return any(pattern.lower() in query.lower() for pattern in skew_patterns)
    
    def _load_query_patterns(self) -> Dict:
        """Load known query patterns and their optimizations"""
        return {
            'broadcast_join': {
                'pattern': 'SELECT * FROM large_table JOIN small_table',
                'optimization': 'SELECT /*+ BROADCAST(small_table) */ * FROM large_table JOIN small_table'
            },
            'filter_pushdown': {
                'pattern': 'SELECT * FROM (SELECT * FROM table) t WHERE condition',
                'optimization': 'SELECT * FROM table WHERE condition'
            },
            'complex_agg': {
                'pattern': 'GROUP BY.*HAVING',
                'optimization': 'Apply two-phase aggregation'
            },
            'data_skew': {
                'pattern': 'JOIN.*ON',
                'optimization': 'Enable adaptive query execution'
            }
        }
