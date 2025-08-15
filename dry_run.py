"""Deprecated script.

`dry_run.py` referenced a legacy `SparkApplicationAnalyzer` which no longer exists.
Use the main entry point instead:

    python spark_analyzer_modular.py tests/data/sample_spark_history.json --report report.html

This stub is kept temporarily for backward compatibility and will be removed in a future release.
"""

if __name__ == '__main__':
    print("dry_run.py is deprecated. Use spark_analyzer_modular.py instead.")
