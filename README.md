# Spark Application Analyzer

A Python toolkit for post‑hoc analysis of Apache Spark application history logs. It surfaces root causes, SQL & code anti‑patterns, and generates a rich HTML performance report. Optional source code correlation maps Spark job/stage call‑sites back to repository lines for faster remediation.

## 1. Key Features
### Core Analysis
- Job / stage statistics (durations, success rate, slow & failed jobs)
- Bottleneck detection (jobs >2× mean duration)
- Task variance → data skew detection (coefficient of variation)
- Shuffle metrics & spill diagnostics
- GC overhead analysis per executor
- Stage dependency fan‑out detection
- Critical path detection across stage DAG (longest weighted chain)

### Advanced Root Cause ( `spark_analyzer/root_cause.py` )
Produces structured `PerformanceIssue` objects with category, severity, impact, recommendation, affected stages, and metrics. Implemented categories:
- data_skew
- shuffle_spill
- gc_overhead
- stage_dependency
- performance_trend
- memory_pressure_model (spill ratio + GC pressure heuristic)
- critical_path (end‑to‑end path duration)
- cpu_bottleneck / memory_bottleneck (utilization heuristics)

### PySpark Code Pattern Detection (`pyspark_code_analyzer.py`)
Static / heuristic detection of:
- Large `collect()` misuse
- Reused DataFrames without caching
- Inefficient Python UDFs (prefer built‑ins / SQL functions)
- Memory pressure indicators (spill + GC)

### SQL Optimizer (heuristic)
- Physical plan JSON parsing (if present) to flag heavy operators
- Join strategy / broadcast hints suggestions
- Hotspot operator ranking (e.g. multiple shuffles / exchanges)
- Cartesian / unnecessary Exchange detection
- Skew & adaptive execution hints (config suggestions)

### Code → Log Correlation (`spark_analyzer/code_mapper.py`)
- Extracts file:line call‑sites from job descriptions (common Spark callSite formats)
- Matches custom annotations in repo for explicit stage mapping
- Produces syntax‑highlighted snippets (Python / Scala / Java / SQL) in report
- Associates related performance issue categories per job

### Recommendation Engine
- Severity scoring `(sev=0.xx)` derived from issue severity / heuristic weight
- De‑duplication retaining highest severity variant
- Inline Spark config snippet suggestions per category

### Reporting
- Modern single HTML file (no external state) containing summary, issues, recommendations, job table, optional code mapping section.
- Machine‑readable JSON summary export (`--json output.json`).

### Robustness
- Generator-based log loading for large files
- Malformed line tolerance (first 5 warned, remainder counted silently)
- Optional S3 fetch (simple object retrieval)
- Safe when optional metrics (GC / memory) absent

## 2. Installation
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# Optional editable install
pip install -e .
```
Python 3.9+ recommended (tests run under 3.13 locally). PySpark is only required if you extend runtime introspection; current analysis runs on exported history JSON without Spark runtime.

## 3. Quick Start (Local File)
```bash
python spark_analyzer_modular.py tests/data/sample_spark_history.json --report report.html
open report.html  # macOS convenience
```

## 4. CLI Usage
```bash
python spark_analyzer_modular.py <history_file_or_s3_app_id> \
  [--s3-config s3_config.json] \
  [--report out.html] \
  [--code-repo /path/to/source/root] \
  [--json summary.json] \
  [--streaming]
```
Arguments:
- history_file: Path to Spark event log (plain text JSON per line)
- --s3-config: Enable S3 retrieval (treat positional arg as app id/key)
- --report: Output HTML filename (default: spark_analysis_report.html)
- --json: Optional JSON summary output (schema below)
- --code-repo: Root of repository for call‑site & annotation mapping
- --streaming: Placeholder flag (future incremental aggregation); currently reads file once via generator

### S3 Config File Example (`s3_config.json`)
```json
{
  "access_key": "YOUR_KEY",
  "secret_key": "YOUR_SECRET",
  "endpoint_url": "https://s3.example.com",
  "bucket_name": "my-spark-history"
}
```

## 5. Code Mapping Annotations
Add any of these in source files to force stage correlation:
```python
# SPARK_STAGE: 12
# SPARK_STAGES: 12, 13
@spark_stage(44)
```
Automatic call‑site patterns extracted from job descriptions (examples):
```
my_job at pipeline.py:120
com.example.Job.run(Job.scala:45)
/abs/path/etl_task.py:77
```
The "Code Mapping" section lists each mapped job, related issue categories, and a syntax‑highlighted snippet around the line.

## 6. SQL Optimizer Demo
Run standalone heuristic analysis of SQL executions present in the history log:
```bash
python run_sql_optimizer.py
```

## 7. Programmatic API
```python
from spark_analyzer_modular import DataLoader, EventParser, Analyzer, RecommendationEngine, Reporter

loader = DataLoader()
events = list(loader.load_events_from_file('tests/data/sample_spark_history.json'))
parsed = EventParser().parse(events)
analysis = Analyzer().analyze(parsed)
recs = RecommendationEngine().generate(analysis, parsed)
Reporter().generate(parsed, analysis, recs, output_file='my_report.html')
```
`analysis['performance_issues']` is a list of `PerformanceIssue` dataclasses.

## 8. HTML Report Sections
- Summary metrics (jobs, avg duration, success rate, counts of issues)
- PySpark code issues (severity color coded)
- Performance issues (root cause categories with affected stages)
- Category summary table (per issue category counts & max severity)
- Bottleneck jobs
- Failed jobs
- Recommendations (severity‑scored, de‑duplicated, config hints)
- Code Mapping (optional)
- Complete job table (with overlapping SQL descriptions)

### 8.1 JSON Summary Schema
Produced when `--json <file>` is specified.

Top-level structure:
```jsonc
{
  "generated_at": "UTC timestamp",
  "job_stats": {"total_jobs": int, "avg_duration": float, ...},
  "performance_issues": [
    {
      "category": "data_skew|shuffle_spill|...",
      "severity": 0.0-1.0,
      "impact": "human readable impact",
      "recommendation": "actionable guidance",
      "affected_stages": [stage_ids],
      "metrics": { "coefficient_of_variation": 0.73, ... }
    }
  ],
  "issue_categories": {
    "data_skew": {"count": 2, "max_severity": 0.91, "affected_stages_union": [3,5]},
    "shuffle_spill": {"count": 1, "max_severity": 0.60, "affected_stages_union": [7] }
  },
  "recommendations": ["(sev=0.85) [DATA_SKEW-AGG] ..."],
  "code_mappings": [
    {"job_id": 12, "file": "etl/pipeline.py", "line": 120, "language": "python", "related_issue_categories": ["data_skew"], "snippet": "..."}
  ],
  "job_count": 15,
  "stage_count": 42,
  "sql_execution_count": 4
}
```
Notes:
- `metrics` object fields vary by category (e.g. skew: coefficient_of_variation; memory_pressure_model: spill_ratio, avg_gc_ratio; critical_path: path, path_duration_ms).
- `recommendations` list already severity‑scored and aggregated.
- `code_mappings` present only if `--code-repo` used and matches found.

## 9. Running Tests
```bash
pip install -r requirements.txt
pytest -q
```
Test coverage spans root cause detection (skew, memory pressure, critical path), SQL optimizer heuristics & hotspots, PySpark pattern analyzer, property-based skew validation (Hypothesis), and dashboard smoke test.

## 10. Troubleshooting
| Symptom | Cause | Mitigation |
|---------|-------|------------|
| Missing stages | Truncated / partial event log | Re-export from Spark history server / verify upload integrity |
| Many malformed line warnings | Mixed / corrupted log file | Use the original unmodified event log; ignore benign JSON ordering differences |
| No SQL operations | SQL listener disabled | Ensure Spark UI events enabled (default) |
| Code mapping miss | Pattern not matched | Add explicit `# SPARK_STAGE:` annotation |
| High spill, low skew | Wide joins / large aggregations | Tune partitions, increase memory fraction, broadcast smaller side |
| GC overhead warnings | Memory pressure / fragmentation | Increase executor memory, adjust `spark.memory.fraction`, reduce cached data |

Malformed lines beyond first 5 are silently counted (`DataLoader.malformed_lines`).

## 11. Roadmap
Implemented (recent iteration):
- Critical path detection
- Memory pressure model (spill vs shuffle volume + GC ratio)
- Physical plan JSON inspection & operator hotspot ranking
- Recommendation severity scoring & de‑duplication with config snippets
- Code → log mapping with multi‑pattern regex + annotations
- Robust malformed line handling & generator loading
- JSON summary export (machine readable)

Planned / Pending:
- CI exit codes based on severity thresholds
- Compressed log ingestion (.gz / .lz4)
- Streaming incremental aggregation & eviction of per‑task detail
- Executor timeline visualization with actual metrics (currently prototype dashboard)
- Enriched memory model (peak vs configured memory estimation)
- LICENSE file committed (MIT)

## 12. Limitations
- Heuristics (skew threshold CV>0.5, spill thresholds) may need tuning per workload.
- No direct integration with live Spark REST APIs (offline log focus).
- Streaming flag is a placeholder; all events currently consumed once into memory after generator collection.
- SQL plan cost estimation is qualitative (no cardinality modeling yet).

## 13. Contributing
PRs welcome: please add / update tests for new heuristics. Run `pytest -q` before submission. Keep README and docstrings synchronized with feature changes.

## 14. Housekeeping
Do not commit generated large reports or synthetic large history files. Add a `.gitignore` rule for `*.html` report artifacts if needed.

## 15. License
Planned: MIT. A `LICENSE` file will be added; treat current code under MIT unless otherwise noted.

---
Questions / ideas? Open an issue with sample event snippet (remove sensitive data) plus expected insight.
