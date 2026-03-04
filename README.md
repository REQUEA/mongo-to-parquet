# MongoDB to Parquet Exporter

Production-ready Python script for exporting MongoDB data to Parquet format with intelligent resource management, error handling, and incremental export capabilities.

## Features

### Core Capabilities
- ✅ **Replica Set Support**: Native support for MongoDB replica sets with configurable read preferences
- ✅ **Intelligent Partitioning**: Automatic date-based partitioning (year/month/day)
- ✅ **Memory Safety**: Adaptive batch sizing based on available system resources
- ✅ **Incremental Export**: Skip already exported data, resume from checkpoints
- ✅ **Error Resilience**: Retry logic, error logging, continue-on-error modes
- ✅ **Structured Logging**: JSON logs with metrics and progress tracking
- ✅ **High Compression**: Zstd compression with configurable levels
- ✅ **Data Validation**: Parquet file integrity checks and document count verification

### Advanced Features
- 📊 Progress tracking with real-time metrics (docs/sec, MB/sec)
- 🔄 Automatic checkpoint management for long-running exports
- 🎯 Flexible filtering: date ranges, custom queries per collection
- 🚀 Parallel processing support (configurable workers)
- 🧪 Dry-run mode for testing configurations
- 📈 Compression ratio reporting
- 🔍 Sample-based data validation

## Installation

### Using UV (Recommended)

```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

### Using pip

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Requirements File

If you need a `requirements.txt`:

```
pymongo>=4.6.0
pyarrow>=14.0.0
pyyaml>=6.0.1
psutil>=5.9.0
python-json-logger>=2.0.7
tenacity>=8.2.3
tqdm>=4.66.0
pandas>=2.1.0
```

## Quick Start

### 1. Configure MongoDB Connection

Edit `config.yaml`:

```yaml
mongodb:
  uri: "mongodb://user:pass@host1:27017,host2:27017,host3:27017/admin?replicaSet=rs0"
  read_preference: "secondaryPreferred"
```

### 2. Configure Export Settings

```yaml
export:
  output_dir: "./parquet_output"
  databases: ["iot_data", "sensor_logs"]  # or ["*"] for all
  
filters:
  date_field: "timestamp"
  start_date: "2023-01-01"
  end_date: "2024-01-01"
```

### 3. Run Export

```bash
# Standard export
python mongo_to_parquet.py

# With custom config
python mongo_to_parquet.py -c /path/to/config.yaml

# Dry-run to test configuration
python mongo_to_parquet.py --dry-run
```

## Configuration Guide

### MongoDB Connection

```yaml
mongodb:
  # Replica set connection string
  uri: "mongodb://user:pass@host1:27017,host2:27017/admin?replicaSet=rs0"
  
  # Read preference: primary, primaryPreferred, secondary, secondaryPreferred, nearest
  # Recommended: secondaryPreferred (reduces load on primary)
  read_preference: "secondaryPreferred"
  
  connection_timeout_ms: 30000
  socket_timeout_ms: 300000
  max_pool_size: 10
```

**URI Format Examples:**

```bash
# With authentication
mongodb://username:password@host1:27017,host2:27017,host3:27017/admin?replicaSet=rs0

# Without authentication (local dev)
mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0

# With SSL/TLS
mongodb://user:pass@host1:27017,host2:27017/admin?replicaSet=rs0&ssl=true&tlsCAFile=/path/to/ca.pem
```

### Database & Collection Selection

```yaml
export:
  # Export all databases except system databases
  databases: ["*"]
  exclude_databases: ["admin", "config", "local"]
  
  # OR: Export specific databases
  # databases: ["iot_data", "sensor_logs"]
  
  collections:
    # Default for all databases
    "*":
      collections: ["*"]
      exclude_collections: ["temp_*", "test_*"]
    
    # Specific database configuration
    iot_data:
      collections: ["sensors", "devices", "readings"]
      exclude_collections: []
```

### Date Filtering

```yaml
filters:
  # Field name containing timestamp
  date_field: "timestamp"
  
  # Export data from 2023
  start_date: "2023-01-01"
  end_date: "2024-01-01"
  
  # Custom filters per collection
  custom_filters:
    sensors:
      status: "active"
      type: { $in: ["temperature", "humidity"] }
```

**Date Format Examples:**

```yaml
start_date: "2023-01-01"                    # Date only
start_date: "2023-01-01 00:00:00"          # Date and time
start_date: "2023-01-01T00:00:00Z"         # ISO 8601
```

### Performance Tuning

```yaml
performance:
  # Automatic batch sizing based on available memory
  batch_size: "auto"  # OR: 10000
  
  # Use up to 50% of system memory
  max_memory_percent: 0.5
  
  # Parallel workers (auto = CPU count)
  max_workers: "auto"  # OR: 4
  
  # Target Parquet file size
  target_parquet_size_mb: 256
  
  # Compression settings
  compression: "zstd"
  compression_level: 9  # 1-22, higher = better compression
```

**Batch Size Guidelines:**

| Available RAM | Recommended batch_size | Max Memory % |
|---------------|------------------------|--------------|
| 4 GB          | 5,000 - 10,000        | 0.3          |
| 8 GB          | 10,000 - 20,000       | 0.4          |
| 16 GB         | 20,000 - 50,000       | 0.5          |
| 32+ GB        | 50,000 - 100,000      | 0.6          |

### Incremental Export

```yaml
incremental:
  # Enable incremental mode
  enabled: true
  
  # WARNING: Set to true to delete existing Parquet files
  overwrite_existing: false
  
  # Checkpoint file for resume capability
  checkpoint_file: "./export_checkpoint.json"
  
  # Resume from checkpoint on restart
  resume_on_restart: true
```

**Behavior:**
- When `enabled: true` and `overwrite_existing: false`:
  - Skips partitions that already have Parquet files
  - Perfect for annual exports (run once per year)
- When `overwrite_existing: true`:
  - Deletes and recreates all Parquet files
  - Use with caution!

### Error Handling

```yaml
error_handling:
  # Retry failed operations
  max_retries: 3
  retry_delay_seconds: 5
  
  # Continue processing other collections on error
  continue_on_error: true
  
  # Stop on first error (fail-fast mode)
  fail_fast: false
  
  # Log failed documents
  log_failed_documents: true
  error_log_file: "./failed_documents.jsonl"
```

### Logging

```yaml
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: "json"  # json or text
  file: "./mongo_export.log"
  console: true
  
  # Progress metrics
  enable_metrics: true
  metrics_interval_seconds: 30
```

**Log Output Example (JSON):**

```json
{
  "timestamp": "2024-02-12T10:30:00.000Z",
  "level": "INFO",
  "message": "Export metrics",
  "elapsed_seconds": 120.5,
  "documents_processed": 1500000,
  "documents_exported": 1500000,
  "mb_written": 450.2,
  "docs_per_second": 12448.13,
  "mb_per_second": 3.74,
  "memory_percent": 45.2
}
```

## Output Structure

### Directory Layout

```
parquet_output/
├── iot_data/
│   ├── sensors/
│   │   ├── year=2023/
│   │   │   ├── month=01/
│   │   │   │   ├── day=01/
│   │   │   │   │   ├── data_1707741000000.parquet
│   │   │   │   │   └── data_1707741300000.parquet
│   │   │   │   ├── day=02/
│   │   │   │   └── day=03/
│   │   │   ├── month=02/
│   │   │   └── month=03/
│   │   └── year=2024/
│   └── devices/
└── sensor_logs/
```

### Parquet File Properties

- **Compression**: Zstd level 9 (typically 3-5x compression)
- **Statistics**: Enabled for efficient querying
- **Dictionary encoding**: Enabled for string columns
- **Schema**: Automatically inferred from MongoDB documents
- **Partitioning**: Year/Month/Day for optimal query performance

## Usage Examples

### Example 1: Full Historical Export (2023 Data)

```yaml
# config.yaml
export:
  output_dir: "./archives/2023"
  databases: ["*"]
  exclude_databases: ["admin", "config", "local"]

filters:
  date_field: "created_at"
  start_date: "2023-01-01"
  end_date: "2024-01-01"

incremental:
  enabled: true
  overwrite_existing: false
```

```bash
python mongo_to_parquet.py -c config.yaml
```

### Example 2: Specific Collections Only

```yaml
export:
  databases: ["iot_production"]
  collections:
    iot_production:
      collections: ["sensor_readings", "device_events"]
      exclude_collections: []

filters:
  date_field: "timestamp"
  start_date: "2023-01-01"
  end_date: "2024-01-01"
```

### Example 3: High-Memory Server (32GB+ RAM)

```yaml
performance:
  batch_size: 100000
  max_memory_percent: 0.7
  max_workers: 8
  target_parquet_size_mb: 512
```

### Example 4: Low-Memory Environment (4GB RAM)

```yaml
performance:
  batch_size: 5000
  max_memory_percent: 0.3
  max_workers: 2
  target_parquet_size_mb: 128
```

### Example 5: Custom Filtering

```yaml
filters:
  date_field: "timestamp"
  start_date: "2023-01-01"
  end_date: "2024-01-01"
  
  custom_filters:
    sensor_readings:
      sensor_type: { $in: ["temperature", "humidity", "pressure"] }
      quality_flag: "valid"
    
    device_events:
      event_type: { $ne: "heartbeat" }
      severity: { $gte: 3 }
```

## Operational Guide

### Pre-Export Checklist

1. **Verify MongoDB Connection**
   ```bash
   # Test connectivity
   mongosh "mongodb://host1:27017,host2:27017/?replicaSet=rs0"
   ```

2. **Check Available Disk Space**
   ```bash
   # Rule of thumb: Parquet files are ~30-40% of MongoDB size with zstd
   df -h /path/to/output_dir
   ```

3. **Verify Date Field Exists**
   ```javascript
   // In mongosh
   db.collection_name.findOne({}, {timestamp: 1})
   ```

4. **Test with Dry Run**
   ```bash
   python mongo_to_parquet.py --dry-run
   ```

### Monitoring During Export

1. **Watch Logs**
   ```bash
   tail -f mongo_export.log | jq .
   ```

2. **Monitor System Resources**
   ```bash
   # Terminal 1: CPU and Memory
   htop
   
   # Terminal 2: Disk I/O
   iostat -x 5
   ```

3. **Check Progress**
   ```bash
   # Count exported files
   find parquet_output -name "*.parquet" | wc -l
   
   # Total size
   du -sh parquet_output/
   ```

### Post-Export Verification

1. **Verify Document Counts**
   ```python
   import pyarrow.parquet as pq
   
   # Read partition
   table = pq.read_table("parquet_output/db/collection/year=2023/")
   print(f"Documents: {len(table)}")
   ```

2. **Check Compression Ratio**
   ```bash
   # Compare MongoDB collection size vs Parquet
   # In mongosh:
   db.collection_name.stats().size
   
   # In shell:
   du -sh parquet_output/db/collection/
   ```

3. **Validate Data Integrity**
   ```python
   import pyarrow.parquet as pq
   
   # Read and validate
   table = pq.read_table("parquet_output/db/collection/year=2023/month=01/day=01/")
   df = table.to_pandas()
   
   # Check schema
   print(df.dtypes)
   
   # Check for nulls
   print(df.isnull().sum())
   ```

### Cleanup After Export

Once Parquet files are verified, you can clean up MongoDB:

```javascript
// In mongosh - DELETE HISTORICAL DATA
db.collection_name.deleteMany({
  timestamp: {
    $gte: ISODate("2023-01-01T00:00:00Z"),
    $lt: ISODate("2024-01-01T00:00:00Z")
  }
})

// Compact collection to reclaim space
db.runCommand({ compact: "collection_name" })
```

**⚠️ WARNING**: Always verify Parquet files before deleting MongoDB data!

## Troubleshooting

### Issue: Out of Memory Errors

**Symptoms:**
```
MemoryError: Unable to allocate array
Process killed (OOM)
```

**Solutions:**
1. Reduce batch size:
   ```yaml
   performance:
     batch_size: 5000
     max_memory_percent: 0.3
   ```

2. Disable parallel workers:
   ```yaml
   performance:
     max_workers: 1
   ```

3. Export one database at a time:
   ```yaml
   export:
     databases: ["single_database"]
   ```

### Issue: MongoDB Connection Timeout

**Symptoms:**
```
ServerSelectionTimeoutError: No servers found
```

**Solutions:**
1. Verify replica set status:
   ```bash
   mongosh --eval "rs.status()"
   ```

2. Check network connectivity:
   ```bash
   telnet host1 27017
   ```

3. Increase timeout:
   ```yaml
   mongodb:
     connection_timeout_ms: 60000
     socket_timeout_ms: 600000
   ```

### Issue: Slow Export Performance

**Symptoms:**
- Low docs/second rate
- High CPU on MongoDB primary

**Solutions:**
1. Use secondary read preference:
   ```yaml
   mongodb:
     read_preference: "secondary"
   ```

2. Increase batch size:
   ```yaml
   performance:
     batch_size: 50000
   ```

3. Create index on date field:
   ```javascript
   db.collection_name.createIndex({ timestamp: 1 })
   ```

### Issue: Parquet Files Too Large

**Symptoms:**
- Individual files exceed 1GB
- Slow query performance

**Solutions:**
1. Reduce target file size:
   ```yaml
   performance:
     target_parquet_size_mb: 128
   ```

2. Reduce batch size (forces more frequent flushes):
   ```yaml
   performance:
     batch_size: 10000
   ```

### Issue: Resume After Crash

The script automatically resumes from the last checkpoint:

```bash
# Check checkpoint status
cat export_checkpoint.json | jq .

# Resume export
python mongo_to_parquet.py
```

To force a fresh start:
```bash
rm export_checkpoint.json
python mongo_to_parquet.py
```

## Performance Benchmarks

Typical performance on standard hardware:

| Hardware | Docs/sec | MB/sec | Compression Ratio |
|----------|----------|--------|-------------------|
| 4 CPU, 8GB RAM | 5,000 - 10,000 | 2-4 | 3.5x |
| 8 CPU, 16GB RAM | 15,000 - 25,000 | 6-10 | 3.8x |
| 16 CPU, 32GB RAM | 30,000 - 50,000 | 12-20 | 4.2x |

**Factors affecting performance:**
- Document size and complexity
- MongoDB server load
- Network latency to MongoDB
- Disk I/O speed
- Number of collections
- Index availability on date field

## Best Practices

### 1. Annual Export Strategy

```yaml
# January 2025: Export 2024 data
filters:
  start_date: "2024-01-01"
  end_date: "2025-01-01"

# January 2026: Export 2025 data
filters:
  start_date: "2025-01-01"
  end_date: "2026-01-01"
```

### 2. Verify Before Delete

```bash
# 1. Export to Parquet
python mongo_to_parquet.py

# 2. Verify document counts
python verify_export.py

# 3. Backup MongoDB (just in case)
mongodump --db=iot_data --out=backup_before_delete

# 4. Delete from MongoDB
python cleanup_mongodb.py

# 5. Compact collections
python compact_collections.py
```

### 3. Monitoring Production Exports

```bash
# Run in background with logging
nohup python mongo_to_parquet.py > export.out 2>&1 &

# Monitor progress
watch -n 5 'tail -20 mongo_export.log | jq "select(.message == \"Export metrics\")"'
```

### 4. Resource Isolation

For production exports, consider:
- Running on a dedicated export server
- Using MongoDB secondary for reads
- Scheduling during low-traffic hours
- Setting CPU/memory limits with systemd or cgroups

## Security Considerations

### 1. Credential Management

**Don't hardcode credentials in config.yaml:**

```yaml
# BAD
mongodb:
  uri: "mongodb://admin:password123@host:27017/..."

# GOOD - Use environment variables
mongodb:
  uri: "${MONGODB_URI}"
```

```bash
# Set environment variable
export MONGODB_URI="mongodb://admin:${MONGO_PASSWORD}@host:27017/..."
python mongo_to_parquet.py
```

### 2. File Permissions

```bash
# Restrict config file access
chmod 600 config.yaml

# Restrict output directory
chmod 700 parquet_output/
```

### 3. Network Security

- Use SSL/TLS for MongoDB connections
- Restrict access to MongoDB replica set
- Use VPN or SSH tunneling for remote exports

## Advanced Usage

### Programmatic Usage

```python
from mongo_to_parquet import MongoToParquetExporter

# Initialize exporter
exporter = MongoToParquetExporter("config.yaml")

# Run export
exporter.export()

# Access statistics
print(exporter.stats)
```

### Custom Validation

```python
# After export, validate custom business rules
import pyarrow.parquet as pq

table = pq.read_table("parquet_output/db/collection/")
df = table.to_pandas()

# Validate
assert df["sensor_value"].between(0, 100).all(), "Invalid sensor values"
assert df["timestamp"].is_monotonic_increasing, "Timestamps not ordered"
```

## FAQ

**Q: Can I export data without a date field?**

A: Yes, set `date_field: null` in config. All documents will be exported with current date partitioning.

**Q: What happens if the script crashes mid-export?**

A: If `incremental.resume_on_restart: true`, the script will resume from the last checkpoint. Already exported partitions are skipped.

**Q: Can I export to S3 or cloud storage?**

A: Currently, exports to local filesystem only. After export, use `aws s3 sync` or similar tools to transfer to cloud storage.

**Q: How do I query the Parquet files?**

A: Use DuckDB, Apache Spark, pandas, or any Parquet-compatible tool:

```python
import duckdb

# Query with SQL
result = duckdb.query("""
    SELECT sensor_type, AVG(value) as avg_value
    FROM 'parquet_output/iot_data/sensors/**/*.parquet'
    WHERE year = 2023 AND month = 6
    GROUP BY sensor_type
""").df()
```

**Q: Can I run multiple exports in parallel?**

A: Yes, but use different config files with different output directories and checkpoint files.

## Support & Contributing

- Report issues: Create GitHub issue with logs and config (redact credentials!)
- Feature requests: Open GitHub discussion
- Security issues: Email security@company.com

## License

MIT License - See LICENSE file for details

## Changelog

### Version 1.0.0 (2024-02-12)
- Initial production release
- Replica set support
- Date-based partitioning
- Incremental export
- JSON logging
- Auto resource management
- Comprehensive error handling
