# Spark Application Analyzer

A sophisticated Python tool for comprehensive analysis and optimization of Apache Spark applications, featuring advanced root cause analysis, SQL query optimization, and real-time performance monitoring.

## Key Features

### Root Cause Analysis
- Automated detection of performance bottlenecks
- Data skew analysis with coefficient of variation metrics
- Shuffle pattern optimization
- Resource utilization monitoring (CPU, Memory, GC)
- Performance trend analysis with historical data
- Stage dependency analysis
- Garbage collection impact assessment

### SQL Query Optimization
- Automated query plan analysis
- Join optimization recommendations
- Aggregation performance tuning
- Filter operation optimization
- Data skew detection and mitigation
- Resource-aware query planning
- Adaptive execution suggestions

### Core Features
- Flexible data loading from S3 or local files
- Comprehensive event parsing for all Spark operations
- Interactive visual reports using Plotly
- Smart recommendations engine
- Modular architecture for easy extension
- Type-hinted codebase following PEP 8 guidelines
- Real-time performance monitoring

## Architecture

The application implements a sophisticated modular architecture with specialized components:

### Core Components

1. **DataLoader**
   - S3 and local file ingestion with robust error handling
   - Streaming support for real-time analysis
   - Configurable data source adapters

2. **EventParser**
   - Comprehensive Spark event processing
   - Real-time event streaming capabilities
   - Custom event type handlers

3. **RootCauseAnalyzer**
   - Advanced bottleneck detection algorithms
   - Historical trend analysis
   - Pattern-based issue identification
   - Resource utilization monitoring
   - Stage dependency analysis
   - GC impact assessment

4. **SQLOptimizer**
   - Query plan analysis and optimization
   - Join strategy recommendations
   - Aggregation performance tuning
   - Filter operation optimization
   - Data skew detection
   - Resource-aware planning

5. **RecommendationEngine**
   - Context-aware optimization suggestions
   - Priority-based recommendation sorting
   - Implementation guidance
   - Impact assessment

6. **Reporter**
   - Interactive Plotly visualizations
   - Real-time performance dashboards
   - Customizable reporting templates
   - Export capabilities

## Prerequisites

### System Requirements
- Python 3.x
- Memory: 4GB+ recommended for large history files
- Storage: Space for history files and generated reports

### Required Dependencies
```
pandas>=1.3.0    # Data processing and analysis
boto3>=1.20.0    # AWS S3 interaction
plotly>=5.3.0    # Interactive visualizations
numpy>=1.20.0    # Numerical computations
```

### S3 Configuration (Optional)
- AWS credentials or compatible S3 endpoint
- Configured S3 bucket with Spark history files
- S3 configuration file (`s3_config.json`):
  ```json
  {
      "access_key": "your-access-key",
      "secret_key": "your-secret-key",
      "endpoint_url": "your-s3-endpoint",
      "region": "your-region"         # Optional
  }
  ```

### Spark Environment
- Compatible with Spark 2.x and 3.x history files
- Support for both local and distributed Spark deployments
- Access to Spark history server logs

## Setup

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. (Recommended) Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

4. Ensure you are using Python 3.8 or newer (tested with Python 3.8+).

3. Configure S3 access (optional) in `s3_config.json`:
```json
{
    "access_key": "your-access-key",
    "secret_key": "your-secret-key",
    "endpoint_url": "your-s3-endpoint"
}
```

## Usage

### Core Analysis Module (spark_analyzer_modular.py)

The `spark_analyzer_modular.py` is the main analysis engine that provides comprehensive Spark application analysis:

```bash
# Analyze S3 source with custom configuration
python spark_analyzer_modular.py --s3-config s3_config.json --history-file your-app-id

# Analyze local history file
python spark_analyzer_modular.py --history-file path/to/history.json

# Generate custom report
python spark_analyzer_modular.py --history-file path/to/history.json --report custom_report.html
```

#### Quickstart Example

```bash
# Minimal example: Analyze a local Spark history file and generate a report
python spark_analyzer_modular.py --history-file tests/data/sample_spark_history.json --report my_report.html
```

#### Troubleshooting
- **S3 errors:** Check your `s3_config.json` and AWS credentials.
- **Memory issues:** For large files, ensure your system has sufficient RAM or use chunked processing options.
- **Missing dependencies:** Reinstall with `pip install -r requirements.txt` inside your virtual environment.

#### Key Components

1. **DataLoader**
   ```python
   from spark_analyzer_modular import DataLoader
   
   # Initialize with S3 config
   loader = DataLoader('s3_config.json')
   
   # Load from S3 or local file
   events = loader.load_events_from_s3('app-id')
   # or
   events = loader.load_events_from_file('history.json')
   ```

2. **EventParser**
   ```python
   from spark_analyzer_modular import EventParser
   
   parser = EventParser()
   parsed_data = parser.parse(events)
   # Returns structured data including:
   # - Jobs with timing and status
   # - SQL operations
   # - Stage information
   # - Task metrics
   ```

3. **Analyzer**
   ```python
   from spark_analyzer_modular import Analyzer
   
   analyzer = Analyzer()
   analysis = analyzer.analyze(parsed_data)
   # Provides:
   # - Job statistics
   # - Bottleneck detection
   # - Performance patterns
   ```

4. **RecommendationEngine**
   ```python
   from spark_analyzer_modular import RecommendationEngine
   
   engine = RecommendationEngine()
   recommendations = engine.generate(analysis)
   # Generates actionable recommendations for:
   # - Failed jobs
   # - Performance bottlenecks
   # - Resource utilization
   ```

5. **Reporter**
   ```python
   from spark_analyzer_modular import Reporter
   
   reporter = Reporter()
   reporter.generate(parsed_data, analysis, recommendations)
   # Creates interactive HTML report with:
   # - Performance summary
   # - Bottleneck analysis
   # - Job timelines
   # - SQL query analysis
   ```

#### Generated Report Features
- Interactive performance dashboard
- Job execution timelines
- SQL query analysis
- Resource utilization charts
- Bottleneck identification
- Actionable recommendations

### Additional Analysis Tools

### Advanced Analysis Features

1. **Root Cause Analysis**
   ```python
   from spark_analyzer.root_cause import RootCauseAnalyzer
   
   analyzer = RootCauseAnalyzer()
   issues = analyzer.analyze_bottlenecks(metrics)
   for issue in issues:
       print(f"Category: {issue.category}")
       print(f"Severity: {issue.severity:.2f}")
       print(f"Impact: {issue.impact}")
       print(f"Recommendation: {issue.recommendation}")
   ```

2. **SQL Query Optimization**
   ```python
   from spark_analyzer.sql_optimizer import SQLOptimizer
   
   optimizer = SQLOptimizer()
   optimization = optimizer.analyze_query(query_metrics)
   print("Recommendations:", optimization.recommendations)
   print("Estimated Improvement:", optimization.estimated_improvement)
   ```

3. **Performance Monitoring**
   ```bash
   # Launch real-time dashboard
   python run_dashboard.py --port 8050
   ```

The analysis process includes:
1. Comprehensive event log parsing
2. Multi-dimensional performance analysis
3. Root cause identification
4. SQL query optimization
5. Real-time monitoring
6. Interactive visualizations
7. Detailed recommendations with implementation guidance

## Testing

### Test Suite Organization

```bash
tests/
├── test_spark_analyzer_modular.py   # Core analyzer tests
├── test_root_cause.py              # Root cause analysis tests
├── test_sql_optimizer.py           # SQL optimization tests
└── data/
    ├── sample_spark_history.json   # Sample events
    ├── large_spark_history.json    # Performance testing
    └── test_s3_config.json        # S3 config template
```

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test module
python -m pytest tests/test_spark_analyzer_modular.py

# Run with coverage report
python -m pytest --cov=spark_analyzer tests/
```

### Test Coverage

#### Unit Tests
- DataLoader
  - S3 connectivity and error handling
  - Local file loading
  - Configuration parsing

- EventParser
  - Event type identification
  - Job/stage/task parsing
  - SQL operation tracking
  - Metric aggregation

- Analyzer
  - Performance metrics calculation
  - Bottleneck detection
  - Pattern recognition
  - Statistical analysis

- RecommendationEngine
  - Context-aware suggestions
  - Priority-based sorting
  - Implementation guidance

- Reporter
  - HTML report generation
  - Visualization accuracy
  - Interactive features

#### Integration Tests
- End-to-end analysis pipeline
- Cross-component interaction
- Data flow validation
- Performance benchmarks

#### Edge Cases
- Large history files (>1GB)
- Malformed event data
- Network timeouts
- Resource constraints
- Invalid configurations

## Analysis Capabilities

The analyzer provides comprehensive insights through multiple analysis dimensions:

### 1. Performance Analytics
- **Job Analysis**
  - Duration distributions and trends
  - Resource utilization patterns
  - Bottleneck identification
  - Stage dependencies

- **SQL Operations**
  - Query plan optimization
  - Join strategy analysis
  - Aggregation performance
  - Filter efficiency

- **Resource Utilization**
  - CPU/Memory usage patterns
  - GC impact assessment
  - Shuffle behavior
  - Data skew detection

### 2. Visualization Components
- **Interactive Dashboards**
  - Real-time performance monitoring
  - Historical trend analysis
  - Resource utilization charts
  - SQL query performance

- **Performance Reports**
  - Detailed bottleneck analysis
  - Query optimization recommendations
  - Resource utilization insights
  - Trend visualizations

### 3. Optimization Recommendations
- **Performance Tuning**
  - Resource configuration
  - Query optimization
  - Data distribution
  - Memory management

- **Implementation Guidance**
  - Step-by-step solutions
  - Configuration examples
  - Best practices
  - Impact assessment

## Contributing


Contributions are welcome! Please follow these guidelines:

### Development Process
1. Fork the repository
2. Create a feature branch
3. Implement changes with tests and docstrings
4. Ensure all code follows PEP 8 and includes type hints
5. Run linting/formatting tools (e.g., `flake8`, `black .`)
6. Submit a pull request

### Code of Conduct
Be respectful and constructive. See `CODE_OF_CONDUCT.md` if available.

### Testing Guidelines
- Maintain or improve test coverage
- Add tests for new features and edge cases
- Use `pytest` for running tests
- Include type hints and docstrings for all new code

### Extending the Analyzer

To add new metrics or visualizations:
1. Extend the `EventParser` class for new event types
2. Add analysis logic in the `Analyzer` class
3. Update or add new visualizations in `Reporter`
4. Add rules or patterns to `RecommendationEngine` for new recommendations
5. Add or update tests in the `tests/` directory

#### Example: Adding a New Metric
```python
# In spark_analyzer/enhanced_analyzer.py
class EnhancedAnalyzer(Analyzer):
    def analyze_new_metric(self, parsed_data: dict) -> dict:
        """Analyze and return new metric results."""
        # ...implementation...
        return {"new_metric": value}
```

Update the `Reporter` to visualize the new metric, and add tests in `tests/`.

---

For more details, see inline comments and docstrings in the codebase.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Apache Spark community
- Contributors and maintainers
- Users who provided valuable feedback

2. **Performance Metrics**
   - Job execution statistics
   - Stage-level bottleneck analysis
   - SQL operation efficiency metrics
   - Memory and resource utilization

3. **Optimization Recommendations**
   - Data partitioning strategies
   - Resource configuration suggestions
   - SQL query optimization tips
   - Caching and persistence recommendations

## Development

To extend the analyzer:

1. **Adding New Metrics**
   - Extend the `EventParser` class for new event types
   - Add analysis logic in the `Analyzer` class
   - Update visualization code in `Reporter`

2. **Custom Recommendations**
   - Add rules to `RecommendationEngine`
   - Implement new analysis patterns
   - Update the reporting format

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

1. Executor memory configuration
2. Partition optimization
3. Data caching strategies
4. Join operation improvements
5. Resource allocation adjustments
