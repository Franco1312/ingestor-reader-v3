# Ingestor Reader v3

ETL pipeline system following Clean Architecture principles with a plugin-based design for flexible data ingestion, processing, and loading.

## ETL Pipeline Flow

The pipeline processes data through five sequential stages:

1. **Extract**: Retrieves raw data from configured sources (HTTP endpoints, files, etc.)
2. **Parse**: Converts raw data into structured format based on dataset-specific parsing rules
3. **Normalize**: Standardizes data structure, applies timezone conversions, and handles primary keys
4. **Transform**: Enriches data with metadata (unit, frequency, collection_date) from configuration
5. **Load**: Persists processed data to the configured destination

All stages are optional except Extract. The pipeline executes only the stages configured for each dataset.

## Key Features

### Plugin Architecture

Each ETL stage uses a plugin system, allowing different implementations per dataset:
- **Extractors**: Data source adapters (HTTP, file, etc.)
- **Parsers**: Dataset-specific parsing logic
- **Normalizers**: Data standardization and timezone handling
- **Transformers**: Metadata enrichment and data transformation
- **Loaders**: Destination adapters

Plugins are registered in the `PluginRegistry` and selected via configuration.

### Incremental Updates

The pipeline supports incremental processing through state management:
- Tracks the last processed date per series
- Only processes new data since the last run
- State persisted via file or S3 backends

### Distributed Locking

Prevents concurrent ETL executions using lock managers:
- DynamoDB-based distributed locks
- Configurable lock timeouts
- Automatic lock release on completion or failure

### Configuration-Driven

Each dataset has a YAML configuration file defining:
- Source location and format
- Parsing rules and series mappings
- Normalization settings (timezone, primary keys)
- Transformation metadata (unit, frequency)
- State and lock management backends

## Project Structure

```
src/
├── domain/              # Core interfaces (ports)
├── application/         # Use cases and orchestration
└── infrastructure/      # Plugin implementations and adapters
    ├── plugins/         # Plugin implementations by type
    ├── state_managers/  # State persistence backends
    └── lock_managers/   # Lock management backends

config/
└── datasets/            # Dataset YAML configurations

tests/                   # Test suite with builder patterns
```

## Setup

1. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install in development mode:
```bash
pip install -e ".[dev]"
```

## Usage

Run an ETL pipeline for a dataset:
```bash
python main.py <dataset_id>
```

Example:
```bash
python main.py bcra_infomondia_series
```

## Important Considerations

### Configuration Files

- Dataset configurations are located in `config/datasets/`
- Each YAML file must define the dataset structure and processing rules
- The `parse_config` section contains series mappings with metadata (unit, frequency)
- State and lock configurations are optional but recommended for production

### Data Flow

- **Extract** returns raw bytes
- **Parse** receives raw bytes and returns structured data points
- **Normalize** receives parsed data and returns normalized data with timezone-aware timestamps
- **Transform** receives normalized data and enriches it with metadata from config
- **Load** receives transformed data for persistence

### State Management

- State tracks the maximum `obs_time` per `internal_series_code`
- Used by parsers to filter incremental data
- Saved after normalization stage completes
- File-based state is suitable for local development; S3 for distributed environments

### Lock Management

- Locks prevent concurrent ETL runs for the same dataset
- Lock key defaults to `etl:{dataset_id}` but can be customized
- Lock timeout defaults to 300 seconds
- DynamoDB locks require proper AWS credentials and table configuration

### Adding New Plugins

1. Implement the interface from `src/domain/interfaces.py`
2. Register the plugin in the appropriate `__init__.py` under `src/infrastructure/plugins/`
3. Reference the plugin name in dataset configuration

### Testing

- Tests use builder patterns for creating test data and mocks
- All components are tested in isolation
- Integration tests verify end-to-end pipeline execution
- Run tests with: `pytest tests/ -v`

## Architecture Principles

- **Clean Architecture**: Separation of domain, application, and infrastructure layers
- **Dependency Inversion**: High-level modules depend on abstractions (interfaces)
- **Plugin Pattern**: Extensible design allowing new data sources and processors
- **Configuration-Driven**: Behavior controlled via YAML files, not code changes
