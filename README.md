# DynamoDB Mirror

High-performance DynamoDB to SQLite mirroring service with real-time streaming updates.

## Features

- **SQLite Storage**: Lightweight, serverless database with JSON support
- **Real-time Streaming**: Processes DynamoDB streams for live updates
- **Low Resource Usage**: Configurable polling intervals to minimize CPU, network, and memory usage
- **Connection Pooling**: Optimized AWS SDK usage prevents connection pool exhaustion
- **Table Mapping**: Maps DynamoDB table names to clean SQLite names
- **Automatic Indexing**: Creates indexes on primary keys and GSI attributes
- **Initial Load**: Performs full table scan only when SQLite table is empty
- **Multi-threading**: Concurrent shard processing with resource-aware throttling

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Mirror default bosphorus tables (requires AWS credentials, very low resource usage)
./run

# More responsive (check every 30 seconds)
./run --poll-interval 30

# High responsiveness (check every 5 seconds)
./run --poll-interval 5

# Mirror specific tables
./run --tables table1 table2

# Run tests
./run test

# Validate configuration
./run validate
```

## Default Table Mappings

The service automatically maps bosphorus DynamoDB tables to clean SQLite names:

| DynamoDB Table | SQLite Table |
|----------------|-------------|
| `daz3-bosphorus-LendingTable` | `lending` |
| `daz3-bosphorus-onboarding` | `onboarding` |
| `daz3-bosphorus-OnboardingDocument` | `onboarding_document` |
| `daz3-bosphorus-OnboardingReferenceData` | `onboarding_reference_data` |

*Stage prefix (`daz3`) is read from `~/work/bosphorus-middleware/.sst/stage`*

## Configuration

### AWS Credentials

Configure AWS credentials using one of:
- AWS CLI: `aws configure`
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- IAM roles (when running on EC2)

### Database Location

SQLite database is stored at `./output/dynamo_mirror.db` by default. Use `--db-path` to customize:

```bash
./run --db-path /path/to/database.db
```

## Architecture

### Data Flow

1. **Table Discovery**: Describes DynamoDB tables for schema information
2. **SQLite Setup**: Creates tables with JSON columns and expression indexes
3. **Initial Load**: Scans DynamoDB tables only if SQLite tables are empty
4. **Stream Processing**: Multi-threaded shard watchers for real-time updates

### Storage Format

Items are stored as JSON in SQLite with indexes on key attributes:

```sql
CREATE TABLE lending (
    item_data TEXT NOT NULL
);

CREATE INDEX lending_id_idx ON lending (json_extract(item_data, '$.id'));
```

### Threading Model & Resource Usage

- Main thread manages initialization and coordination
- Stream manager thread per table monitors for new shards
- Shard watcher thread per shard processes records

**Default Performance (Very Low Resource Usage):**
- Stream polling: Every 60 seconds
- Shard discovery: Every 10 minutes
- Updates appear within 60 seconds
- Minimal CPU, network, and memory impact

## Advanced Usage

### Custom Table Selection

```bash
# Mirror specific tables with original names
./run --tables MyTable1 MyTable2 --no-mapping

# Mirror with custom region
./run --region us-west-2
```

### Monitoring

Enable verbose logging to see detailed operations:

```bash
./run --verbose
```

### Testing

Run comprehensive test suite:

```bash
./run test
```

Tests use real SQLite databases (no mocking) to ensure accurate validation.

## Requirements

- Python 3.8+
- AWS credentials with DynamoDB access
- Dependencies: `boto3`, `colorama`

## Development

Project follows strict Python standards:

- All code in `src/` directory
- Every `.py` file has corresponding `_test.py`
- Real database testing (no mocks)
- Comprehensive logging with colored output
- Type hints throughout