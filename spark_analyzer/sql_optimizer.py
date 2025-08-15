"""
SQL Query Optimization module for analyzing and improving Spark SQL performance
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json
import re

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
        # Operator severity heuristics (simplistic scoring)
        self.operator_cost = {
            'CartesianProduct': 0.9,
            'SortMergeJoin': 0.6,
            'BroadcastHashJoin': 0.2,
            'ShuffledHashJoin': 0.5,
            'Exchange': 0.4,
            'WholeStageCodegen': 0.1,
            'AdaptiveSparkPlan': 0.0,
            'AQEShuffleRead': 0.2,
            'SkewJoin': 0.7
        }
        self.hotspot_limit = 5

    def analyze_query(self, query_metrics: Dict) -> QueryOptimization:
        """Analyze SQL query execution plan and suggest optimizations (logical + physical)."""
        plan = query_metrics.get('query_plan', '')
        query = query_metrics.get('description', '')
        spark_plan_info = query_metrics.get('spark_plan_info') or query_metrics.get('sparkPlanInfo')

        optimizations: List[str] = []

        # Logical query pattern heuristics ---------------------------------
        q_upper = query.upper()
        if 'GROUP BY' in q_upper:
            optimizations.extend(self._analyze_aggregation(query))
        if 'JOIN' in q_upper:
            optimizations.extend(self._analyze_joins(query))
        if 'WHERE' in q_upper:
            optimizations.extend(self._analyze_filters(query))
        if self._is_complex_aggregation(query):
            optimizations.append(
                "Consider two-phase aggregation (partial + final) to reduce shuffle volume"
            )
        if self._has_potential_data_skew(query):
            optimizations.append(
                "Potential skew risk: enable AQE skew handling (spark.sql.adaptive.enabled=true, spark.sql.adaptive.skewJoin.enabled=true)"
            )

        # Physical plan / operator analysis --------------------------------
        plan_json = self._maybe_parse_plan_json(plan, spark_plan_info)
        physical_findings, suggested_hints = self._analyze_physical_plan(plan_json, plan)
        optimizations.extend(physical_findings)

        # Operator hotspot ranking (if plan JSON present)
        if plan_json:
            hotspots, join_reorder = self._rank_operator_hotspots(plan_json)
            if hotspots:
                pretty = ', '.join([f"{h['name']} (cost={h['cost']:.2f})" for h in hotspots])
                optimizations.append(f"Top operator hotspots: {pretty}")
            if join_reorder:
                optimizations.append(join_reorder)

        # Filter pushdown (string heuristic retained)
        if not self._has_filter_pushdown(plan):
            optimizations.append(
                "Filter pushdown opportunity: move selective filters before joins / enable predicate pushdown"
            )

        # Deduplicate recommendations
        dedup = []
        seen = set()
        for rec in optimizations:
            key = rec.strip()
            if key not in seen:
                seen.add(key)
                dedup.append(rec)

        optimized_plan = self._generate_optimized_plan(plan, suggested_hints)
        return QueryOptimization(
            query_id=query_metrics.get('query_id', ''),
            original_plan=plan,
            optimized_plan=optimized_plan,
            estimated_improvement=self._estimate_improvement(query, dedup),
            recommendations=dedup
        )

    # ---------------- Physical plan helpers -----------------
    def _maybe_parse_plan_json(self, plan_text: str, plan_obj: Optional[Any]) -> Optional[Dict[str, Any]]:
        if isinstance(plan_obj, dict):
            return plan_obj
        if isinstance(plan_obj, str):
            try:
                return json.loads(plan_obj)
            except Exception:
                pass
        # Try to parse textual plan that starts with '{'
        if isinstance(plan_text, str) and plan_text.strip().startswith('{'):
            try:
                return json.loads(plan_text)
            except Exception:
                return None
        return None

    def _collect_operators(self, node: Dict[str, Any], acc: List[Dict[str, Any]]):
        if not isinstance(node, dict):
            return
        op_name = node.get('nodeName') or node.get('name') or ''
        if op_name:
            acc.append(node)
        for child in node.get('children', []):
            self._collect_operators(child, acc)

    def _extract_numeric(self, d: Dict[str, Any], keys: List[str]) -> float:
        for k in keys:
            if k in d and isinstance(d[k], (int, float)):
                return float(d[k])
        # nested metrics dictionary patterns
        metrics = d.get('metrics') or {}
        if isinstance(metrics, dict):
            for k in keys:
                if k in metrics and isinstance(metrics[k], (int, float)):
                    return float(metrics[k])
        return 0.0

    def _rank_operator_hotspots(self, plan_json: Dict[str, Any]):
        ops: List[Dict[str, Any]] = []
        self._collect_operators(plan_json, ops)
        enriched = []
        for o in ops:
            name = (o.get('nodeName') or o.get('name') or '').strip()
            if not name:
                continue
            rows = self._extract_numeric(o, ['numOutputRows', 'outputRows', 'rowCount'])
            dur = self._extract_numeric(o, ['durationMs', 'timeMs', 'duration'])
            # basic cost heuristic: (rows weight + duration weight) * operator weight
            weight = self.operator_cost.get(name.split()[0], 0.3)
            cost = (rows ** 0.5) * (1 + dur/1000.0) * (1 + weight)
            enriched.append({'name': name.split()[0], 'rows': rows, 'duration_ms': dur, 'cost': cost, 'node': o})
        enriched.sort(key=lambda x: x['cost'], reverse=True)
        hotspots = enriched[:self.hotspot_limit]
        # Join reorder suggestion
        join_nodes = [e for e in enriched if 'Join' in e['name']]
        reorder_msg = None
        if len(join_nodes) >= 2:
            # Heuristic: if first (highest cost) join's small side is not broadcastable but a later smaller join exists
            # derive child row sizes
            def child_rows(node_dict):
                sizes = []
                for ch in node_dict.get('children', []):
                    sizes.append(self._extract_numeric(ch, ['numOutputRows','outputRows','rowCount']))
                return sizes
            high = join_nodes[0]
            high_children = child_rows(high['node'])
            later_small = min((j for j in join_nodes[1:] if j['rows'] > 0), key=lambda x: x['rows'], default=None)
            if later_small and high_children:
                small_side = min(high_children)
                # if a later join outputs far fewer rows than the small side of the hottest join, suggest reordering
                if later_small['rows'] * 2 < small_side:
                    reorder_msg = ("Join reorder opportunity: reorder joins to process smaller dimension joins earlier "
                                    "or broadcast small tables to reduce intermediate row counts")
        return hotspots, reorder_msg

    def _analyze_physical_plan(self, plan_json: Optional[Dict[str, Any]], plan_text: str) -> Tuple[List[str], List[str]]:
        findings: List[str] = []
        hints: List[str] = []
        if not plan_json:
            # Fallback lightweight regex based scan of textual plan
            exch_count = plan_text.count('Exchange')
            if 'CartesianProduct' in plan_text:
                findings.append("Cartesian product detected – add join predicate or broadcast to avoid explosive row counts")
            if exch_count > 1:
                findings.append(f"Multiple Exchange operators ({exch_count}) – review partitioning and avoid redundant repartitions")
            return findings, hints

        # Collect operators
        ops: List[Dict[str, Any]] = []
        self._collect_operators(plan_json, ops)
        names = [o.get('nodeName') or o.get('name') for o in ops]
        name_counts = {n: names.count(n) for n in set(names) if n}

        # Cartesian product
        if name_counts.get('CartesianProduct'):
            findings.append("Cartesian product detected – ensure proper join conditions (could massively inflate data)")
        # Excessive exchanges
        exch = name_counts.get('Exchange', 0)
        if exch > 3:
            findings.append(f"High shuffle/exchange count ({exch}) – consolidate repartition operations or cache reused DataFrames")
        # Skew join detection
        if name_counts.get('SkewJoin') or name_counts.get('AQEShuffleRead'):
            findings.append("Skew mitigation active (AQE) – verify skew parameters; remaining skew may need salting")
        # SortMergeJoin improvement hints
        if name_counts.get('SortMergeJoin') and name_counts.get('BroadcastHashJoin', 0) == 0:
            findings.append("SortMergeJoin present without any broadcast joins – check if smaller side qualifies for broadcast")
            hints.append("/*+ BROADCAST(small_dim) */")
        # Memory pressure pattern
        if name_counts.get('SortMergeJoin', 0) + name_counts.get('ShuffledHashJoin', 0) > 3:
            findings.append("Multiple heavy shuffle joins – increase parallelism or enable AQE for join re-optimization")
        # WholeStageCodegen absence (optional)
        if not name_counts.get('WholeStageCodegen') and len(ops) > 5:
            findings.append("WholeStageCodegen absent – possible codegen fallback; review UDFs or complex expressions")
        return findings, hints

    # ---------------- Existing logical heuristics -----------------
    def _has_cartesian_product(self, plan: str) -> bool:
        return 'Cross Join' in plan or 'CartesianProduct' in plan

    def _has_shuffle_join(self, plan: str) -> bool:
        return 'SortMergeJoin' in plan or 'ShuffleHashJoin' in plan

    def _has_filter_pushdown(self, plan: str) -> bool:
        return 'PushedFilters' in plan

    def _generate_optimized_plan(self, original_plan: str, hints: List[str]) -> str:
        if not hints:
            return original_plan
        # Simple injection of hints at top (placeholder)
        return f"-- Applied Hints: {' '.join(hints)}\n{original_plan}"

    def _estimate_improvement(self, plan: str, optimizations: List[str]) -> float:
        improvement = 0.0
        joined = ' '.join(optimizations).lower()
        if 'broadcast' in joined:
            improvement += 0.25
        if 'filter pushdown' in joined:
            improvement += 0.15
        if 'cartesian' in joined:
            improvement += 0.2
        if 'shuffle' in joined:
            improvement += 0.1
        return min(improvement, 0.9)

    def _analyze_aggregation(self, query: str) -> List[str]:
        recommendations: List[str] = []
        if 'COUNT(DISTINCT' in query.upper():
            recommendations.append(
                "COUNT DISTINCT: consider approx_count_distinct() for large cardinality sets"
            )
        if 'GROUP BY' in query.upper() and ( 'SUM(' in query.upper() or 'AVG(' in query.upper() ):
            recommendations.append(
                "Aggregation tuning: adjust spark.sql.shuffle.partitions and pre-aggregate if possible"
            )
        return recommendations

    def _analyze_joins(self, query: str) -> List[str]:
        recommendations: List[str] = []
        if 'JOIN' in query.upper():
            recommendations.append(
                "Join optimization: order tables from smallest to largest; broadcast small (<10GB); consider bucketing for frequent joins"
            )
        if query.upper().count('JOIN') > 2:
            recommendations.append(
                "Multi-join chain: enable CBO (spark.sql.cbo.enabled=true) and refresh table stats"
            )
        return recommendations

    def _analyze_filters(self, query: str) -> List[str]:
        recommendations: List[str] = []
        q = query.upper()
        if 'WHERE' in q and (' OR ' in q or ' IN ' in q):
            recommendations.append(
                "Complex predicates: prefer IN over multiple OR; bloom filters for selective joins"
            )
        if 'LIKE' in q:
            recommendations.append(
                "LIKE usage: avoid leading wildcards; consider column indexing / predicate pushdown"
            )
        return recommendations

    def _is_complex_aggregation(self, query: str) -> bool:
        patterns = [r'GROUP BY.+HAVING', r'COUNT\s*\(\s*DISTINCT', r'GROUP BY.+ORDER BY', r'WINDOW']
        q = query.upper()
        return any(re.search(p, q, re.IGNORECASE) for p in patterns)

    def _has_potential_data_skew(self, query: str) -> bool:
        skew_patterns = ['GROUP BY', 'JOIN', 'ORDER BY', 'PARTITION BY']
        q = query.upper()
        return any(p in q for p in skew_patterns)

    def _load_query_patterns(self) -> Dict:
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
