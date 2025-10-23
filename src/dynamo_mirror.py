"""
Mirror DynamoDB tables to SQLite database with real-time streaming updates.

This service handles the complete lifecycle of mirroring DynamoDB tables to a local
SQLite database, including initial table scans and ongoing stream processing for
real-time updates.
"""

import json
import sqlite3
import threading
import time
import logging
import re
import queue
from decimal import Decimal
from typing import Dict, List, Set, Optional, Any, Callable
from dataclasses import dataclass
from pathlib import Path

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)


class APICallQueue:
    """
    Serializes all AWS API calls through a single queue to prevent connection pool exhaustion.

    All AWS API calls go through this queue and are executed one at a time by a single worker thread.
    """

    def __init__(self, region: str):
        self.region = region
        self._queue = queue.Queue()
        self._session = boto3.Session()

        # Single clients - no connection pool config needed since only one thread uses them
        self._dynamodb_client = self._session.client("dynamodb", region_name=region)
        self._dynamodb_resource = self._session.resource("dynamodb", region_name=region)
        self._streams_client = self._session.client("dynamodbstreams", region_name=region)

        # Start single worker thread
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()
        logger.info("Started single-threaded AWS API worker")

    def _worker(self):
        """Single worker thread that processes all AWS API calls sequentially."""
        while True:
            try:
                func, args, kwargs, result_queue = self._queue.get()
                try:
                    result = func(*args, **kwargs)
                    result_queue.put(("success", result))
                except Exception as e:
                    result_queue.put(("error", e))
                finally:
                    self._queue.task_done()
            except Exception as e:
                logger.error(f"API worker thread error: {e}")

    def _call_api(self, func: Callable, *args, **kwargs):
        """Queue an API call and wait for result."""
        result_queue = queue.Queue()
        self._queue.put((func, args, kwargs, result_queue))

        status, result = result_queue.get()
        if status == "error":
            raise result
        return result

    def describe_table(self, table_name: str):
        """Queue table description call."""
        return self._call_api(self._dynamodb_client.describe_table, TableName=table_name)

    def scan_table(self, table_name: str, **kwargs):
        """Queue table scan call."""
        table = self._dynamodb_resource.Table(table_name)
        return self._call_api(table.scan, **kwargs)

    def list_tables(self, **kwargs):
        """Queue list tables call."""
        return self._call_api(self._dynamodb_client.list_tables, **kwargs)

    def get_paginator(self, operation_name: str):
        """Get paginator (this returns immediately, actual calls are queued when paginator is used)."""
        return self._dynamodb_client.get_paginator(operation_name)

    def describe_stream(self, stream_arn: str, **kwargs):
        """Queue stream description call."""
        return self._call_api(self._streams_client.describe_stream, StreamArn=stream_arn, **kwargs)

    def get_shard_iterator(self, stream_arn: str, shard_id: str, iterator_type: str):
        """Queue shard iterator call."""
        return self._call_api(
            self._streams_client.get_shard_iterator,
            StreamArn=stream_arn, ShardId=shard_id, ShardIteratorType=iterator_type
        )

    def get_records(self, shard_iterator: str):
        """Queue get records call."""
        return self._call_api(self._streams_client.get_records, ShardIterator=shard_iterator)


class DynamoDBEncoder(json.JSONEncoder):
    """
    JSON encoder that handles DynamoDB-specific types for SQLite storage.

    DynamoDB uses special types like Decimal and sets that aren't JSON serializable.
    This encoder converts them to standard Python types suitable for SQLite.
    """

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, set):
            return list(o)
        return super().default(o)


@dataclass
class TableMapping:
    """Maps DynamoDB table names to local SQLite table names."""
    dynamodb_name: str
    sqlite_name: str


class SQLiteAdapter:
    """
    Manages SQLite database operations for DynamoDB mirroring with minimal resource usage.

    Uses a single connection per adapter instance to avoid connection pool exhaustion
    while maintaining thread safety through careful connection management.
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = None
        self._lock = threading.Lock()
        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database with optimized settings for minimal resource usage."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=1000")  # Reduced cache size
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=67108864")  # 64MB mmap
        self._connection = conn

    def get_connection(self) -> sqlite3.Connection:
        """Get the shared database connection with thread safety."""
        if self._connection is None:
            with self._lock:
                if self._connection is None:
                    self._init_database()
        return self._connection

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def create_table_and_indexes(self, table_name: str, indexed_attrs: Set[str]) -> None:
        """
        Create SQLite table for DynamoDB items if it doesn't exist.

        Each item is stored as JSON in a single column with indexes on key attributes
        for efficient querying and primary key operations.
        """
        conn = self.get_connection()
        with self._lock:
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    item_data TEXT NOT NULL
                )
            ''')

            for attr in indexed_attrs:
                index_name = f"{table_name}_{attr}_idx"
                conn.execute(f'''
                    CREATE INDEX IF NOT EXISTS "{index_name}"
                    ON "{table_name}" (json_extract(item_data, '$.{attr}'))
                ''')

            conn.commit()

    def upsert_item(self, table_name: str, item: Dict[str, Any], key_attrs: List[str]) -> None:
        """
        Insert or update item in SQLite table using delete-then-insert strategy.

        This approach avoids unique constraint issues on expression indexes
        while ensuring only one record per primary key exists.
        """
        json_str = json.dumps(item, cls=DynamoDBEncoder)

        conn = self.get_connection()
        with self._lock:
            if key_attrs:
                conditions = []
                params = []
                for attr in key_attrs:
                    value = item.get(attr)
                    value_str = "" if value is None else str(value)
                    conditions.append(f"json_extract(item_data, '$.{attr}') = ?")
                    params.append(value_str)

                where_clause = " AND ".join(conditions)
                conn.execute(f'DELETE FROM "{table_name}" WHERE {where_clause}', params)

            conn.execute(f'INSERT INTO "{table_name}" (item_data) VALUES (?)', (json_str,))
            conn.commit()

    def delete_item(self, table_name: str, keys: Dict[str, Any]) -> None:
        """Delete item from SQLite table matching the provided primary key values."""
        if not keys:
            return

        conn = self.get_connection()
        with self._lock:
            conditions = []
            params = []
            for attr, value in keys.items():
                value_str = "" if value is None else str(value)
                conditions.append(f"json_extract(item_data, '$.{attr}') = ?")
                params.append(value_str)

            where_clause = " AND ".join(conditions)
            conn.execute(f'DELETE FROM "{table_name}" WHERE {where_clause}', params)
            conn.commit()

    def count_items(self, table_name: str) -> int:
        """Count total items in the specified table."""
        conn = self.get_connection()
        cursor = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        return cursor.fetchone()[0]

    def discover_entity_types(self, table_name: str) -> Set[str]:
        """
        Discover all unique entity types from __edb_e__ field in a table.

        Returns a set of entity type names found in the table data.
        """
        conn = self.get_connection()
        cursor = conn.execute(f'''
            SELECT DISTINCT json_extract(item_data, '$.__edb_e__') as entity_type
            FROM "{table_name}"
            WHERE json_extract(item_data, '$.__edb_e__') IS NOT NULL
        ''')

        entity_types = set()
        for row in cursor.fetchall():
            entity_type = row[0]
            if entity_type and isinstance(entity_type, str):
                entity_types.add(entity_type)

        return entity_types

    def get_entity_fields(self, table_name: str, entity_type: str) -> Dict[str, str]:
        """
        Discover all unique fields used by a specific entity type with their JSON paths.

        Returns a dict mapping field names to their JSON extraction paths.
        Handles nested objects by creating flattened field names.
        """
        conn = self.get_connection()
        cursor = conn.execute(f'''
            SELECT item_data
            FROM "{table_name}"
            WHERE json_extract(item_data, '$.__edb_e__') = ?
            LIMIT 100
        ''', (entity_type,))

        field_paths = {}
        for row in cursor.fetchall():
            try:
                item_data = json.loads(row[0])
                if isinstance(item_data, dict):
                    self._extract_field_paths(item_data, "$", field_paths)
            except (json.JSONDecodeError, TypeError):
                continue

        return field_paths

    def _extract_field_paths(self, obj: Any, path: str, field_paths: Dict[str, str], max_depth: int = 3) -> None:
        """
        Recursively extract field paths from nested JSON objects.

        Creates flattened field names for nested objects (e.g., 'link_type' for 'link.type').
        Limits depth to avoid excessive nesting in views.
        """
        if not isinstance(obj, dict) or path.count('.') > max_depth:
            return

        for key, value in obj.items():
            current_path = f"{path}.{key}" if path != "$" else f"$.{key}"

            if isinstance(value, dict) and len(value) <= 10:  # Only flatten small objects
                # Add flattened nested fields
                self._extract_field_paths(value, current_path, field_paths, max_depth)
            else:
                # Add this field directly
                if path == "$":
                    field_name = key
                else:
                    # Create flattened name: link.type -> link_type
                    field_name = path[2:].replace(".", "_") + "_" + key

                field_paths[field_name] = current_path

    def create_entity_view(self, base_table_name: str, entity_type: str) -> None:
        """
        Create or recreate a flattened view for a specific entity type.

        The view extracts all fields from JSON data into columns, handling nested objects
        by flattening them with dot notation (e.g., link.type -> link_type).
        """
        sanitized_entity_type = self._sanitize_sql_identifier(entity_type)
        view_name = f"{base_table_name}_{sanitized_entity_type}"

        # Get all field paths used by this entity type
        field_paths = self.get_entity_fields(base_table_name, entity_type)

        if not field_paths:
            logger.warning(f"No fields found for entity type {entity_type} in {base_table_name}")
            return

        # Build SELECT clause with field mappings
        select_fields = []
        for field_name, json_path in sorted(field_paths.items()):
            # Convert field names to snake_case for SQL columns
            sql_column = self._to_snake_case(field_name)

            select_fields.append(f"    item_data ->> '{json_path}' AS \"{sql_column}\"")

        select_clause = ",\n".join(select_fields)

        conn = self.get_connection()
        with self._lock:
            # Drop existing view if it exists
            conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')

            # Create new view
            create_view_sql = f'''
CREATE VIEW "{view_name}" AS
SELECT
{select_clause}
FROM "{base_table_name}"
WHERE item_data ->> '$.__edb_e__' = '{entity_type}'
'''

            conn.execute(create_view_sql)
            conn.commit()

            logger.info(f"Created view {view_name} with {len(select_fields)} fields")

    def _to_snake_case(self, camel_str: str) -> str:
        """
        Convert camelCase or PascalCase to snake_case for SQL column names.

        Handles special cases, preserves existing underscores, and sanitizes
        field names to be valid SQL identifiers.
        """
        # Handle special fields that should remain unchanged
        if camel_str.startswith('__') and camel_str.endswith('__'):
            return camel_str.replace('__', '')

        # First, sanitize any problematic characters
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', camel_str)

        # Insert underscores before uppercase letters (but not at the start)
        result = re.sub(r'(?<!^)(?=[A-Z])', '_', sanitized).lower()

        # Clean up any double underscores and handle edge cases
        result = re.sub(r'_+', '_', result)
        result = result.strip('_')

        # Ensure the identifier doesn't start with a number
        if result and result[0].isdigit():
            result = f"field_{result}"

        return result if result else "unnamed_field"

    def _sanitize_sql_identifier(self, name: str) -> str:
        """
        Sanitize a string to be a valid SQL identifier by replacing invalid characters.

        Converts hyphens and other invalid characters to underscores, then applies
        snake_case conversion for consistency.
        """
        # Replace hyphens and other invalid SQL identifier characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)

        # Apply standard snake_case conversion
        return self._to_snake_case(sanitized)

    def refresh_entity_views(self, table_name: str) -> None:
        """
        Refresh all entity-based views for a table by discovering current entity types.

        This should be called periodically or after significant data changes to ensure
        views reflect the current schema of each entity type.
        """
        entity_types = self.discover_entity_types(table_name)

        logger.info(f"Refreshing views for {len(entity_types)} entity types in {table_name}")

        for entity_type in entity_types:
            try:
                self.create_entity_view(table_name, entity_type)
            except Exception as e:
                logger.error(f"Failed to create view for entity {entity_type}: {e}")

    def create_union_view(self, table_names: List[str]) -> None:
        """
        Create an 'all' view that unions all table data with source table information.

        The view adds a 'source_table' column to identify which table each item came from.
        This provides a single interface to query across all mirrored tables.
        """
        if not table_names:
            logger.warning("No tables provided for union view")
            return

        conn = self.get_connection()
        with self._lock:
            # Drop existing view if it exists
            conn.execute('DROP VIEW IF EXISTS "all"')

            # Build UNION ALL query with source table information
            union_parts = []
            for table_name in table_names:
                union_parts.append(
                    f"SELECT item_data, '{table_name}' as source_table FROM \"{table_name}\""
                )

            union_query = " UNION ALL ".join(union_parts)

            create_view_sql = f'CREATE VIEW "all" AS {union_query}'

            conn.execute(create_view_sql)
            conn.commit()

            logger.info(f"Created 'all' view unioning {len(table_names)} tables")


class DynamoDBClient:
    """
    Handles DynamoDB operations using serialized API call queue to prevent connection exhaustion.

    All AWS API calls are queued and executed one at a time by a single worker thread.
    """

    def __init__(self, api_queue: APICallQueue):
        self.api_queue = api_queue

    def describe_table(self, table_name: str) -> Dict[str, Any]:
        """Get complete table description including schema and stream configuration."""
        try:
            response = self.api_queue.describe_table(table_name)
            return response["Table"]
        except Exception as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    def scan_all_items(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Perform complete table scan returning all items.

        Handles pagination transparently and returns deserialized items
        ready for insertion into SQLite.
        """
        items = []

        try:
            response = self.api_queue.scan_table(table_name)
            items.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = self.api_queue.scan_table(table_name, ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))

            logger.info(f"Scanned {len(items)} items from table {table_name}")
            return items
        except Exception as e:
            logger.error(f"Failed to scan table {table_name}: {e}")
            raise

    def list_stage_tables(self, stage_prefix: str) -> List[str]:
        """
        Discover all DynamoDB tables that match the stage prefix pattern.

        Returns tables of the form {stage_prefix}-bosphorus-* or {stage_prefix}-*
        """
        try:
            paginator = self.api_queue.get_paginator('list_tables')
            all_tables = []

            for page in paginator.paginate():
                all_tables.extend(page.get('TableNames', []))

            # Filter tables that match the stage pattern
            stage_tables = []
            patterns = [
                f"{stage_prefix}-bosphorus-",  # Primary pattern
                f"{stage_prefix}-"              # Fallback pattern
            ]

            for table in all_tables:
                for pattern in patterns:
                    if table.startswith(pattern):
                        stage_tables.append(table)
                        break

            logger.info(f"Found {len(stage_tables)} tables matching stage {stage_prefix}")
            for table in sorted(stage_tables):
                logger.info(f"  {table}")

            return sorted(stage_tables)
        except Exception as e:
            logger.error(f"Failed to list stage tables: {e}")
            raise


class StreamProcessor:
    """
    Processes DynamoDB streams for real-time table updates using serialized API calls.

    All AWS API calls are queued and executed one at a time to prevent connection exhaustion.
    """

    def __init__(self, sqlite_adapter: SQLiteAdapter, deserializer: TypeDeserializer, api_queue: APICallQueue):
        self.sqlite_adapter = sqlite_adapter
        self.deserializer = deserializer
        self.api_queue = api_queue

    def _deserialize_item(self, dynamodb_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB typed item to plain Python dictionary."""
        return {key: self.deserializer.deserialize(value) for key, value in dynamodb_item.items()}

    def _list_all_shards(self, stream_arn: str, last_shard_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Recursively retrieve all shards for a stream, handling pagination."""
        try:
            if last_shard_id is None:
                response = self.api_queue.describe_stream(stream_arn)
            else:
                response = self.api_queue.describe_stream(stream_arn, ExclusiveStartShardId=last_shard_id)

            shards = response["StreamDescription"].get("Shards", [])
            last = response["StreamDescription"].get("LastEvaluatedShardId")

            if last:
                shards.extend(self._list_all_shards(stream_arn, last))

            return shards
        except Exception as e:
            logger.error(f"Failed to list shards for stream {stream_arn}: {e}")
            raise

    def _list_open_shards(self, stream_arn: str) -> List[Dict[str, Any]]:
        """Return only open shards (those without ending sequence numbers)."""
        all_shards = self._list_all_shards(stream_arn)
        return [
            shard for shard in all_shards
            if shard.get("SequenceNumberRange", {}).get("EndingSequenceNumber") is None
        ]

    def _get_shard_iterator(self, stream_arn: str, shard_id: str, iterator_type: str = "LATEST") -> str:
        """Obtain shard iterator for processing records."""
        try:
            response = self.api_queue.get_shard_iterator(stream_arn, shard_id, iterator_type)
            return response["ShardIterator"]
        except Exception as e:
            logger.error(f"Failed to get shard iterator for {shard_id}: {e}")
            raise

    def _process_stream_record(self, table_name: str, key_attrs: List[str], record: Dict[str, Any]) -> None:
        """Apply single stream record to SQLite table based on event type."""
        try:
            event_name = record.get("eventName")
            dynamodb_data = record.get("dynamodb", {})

            if event_name in ("INSERT", "MODIFY"):
                new_image = dynamodb_data.get("NewImage")
                if new_image:
                    item = self._deserialize_item(new_image)
                    self.sqlite_adapter.upsert_item(table_name, item, key_attrs)
                    logger.debug(f"Processed {event_name} for {table_name}")

            elif event_name == "REMOVE":
                keys_image = dynamodb_data.get("Keys")
                if keys_image:
                    keys = self._deserialize_item(keys_image)
                    self.sqlite_adapter.delete_item(table_name, keys)
                    logger.debug(f"Processed REMOVE for {table_name}")

        except Exception as e:
            logger.error(f"Failed to process stream record for {table_name}: {e}")
            raise

    def _watch_shard(self, stream_arn: str, shard_id: str, table_name: str, key_attrs: List[str],
                     poll_interval: int = 60) -> None:
        """
        Continuously poll shard for records with configurable interval for low resource usage.

        Uses longer polling intervals by default to minimize CPU, network, and memory usage
        while still maintaining reasonable update latency.
        """
        logger.info(f"Starting shard watcher for {shard_id} on table {table_name} "
                    f"with {poll_interval}s intervals")

        try:
            shard_iterator = self._get_shard_iterator(stream_arn, shard_id)

            while shard_iterator:
                response = self.api_queue.get_records(shard_iterator)

                records = response.get("Records", [])
                if records:
                    logger.debug(f"Processing {len(records)} records from shard {shard_id}")
                    for record in records:
                        self._process_stream_record(table_name, key_attrs, record)

                shard_iterator = response.get("NextShardIterator")

                # Use longer sleep intervals to reduce resource usage
                time.sleep(poll_interval)

        except Exception as e:
            logger.error(f"Shard watcher failed for {shard_id}: {e}")
            raise

    def start_stream_manager(self, stream_arn: str, table_name: str, key_attrs: List[str],
                             poll_interval: int = 60, shard_check_interval: int = 600) -> None:
        """
        Start background thread to manage all shard watchers for a stream with minimal resource usage.

        Uses configurable intervals to reduce CPU and network overhead while maintaining
        reasonable responsiveness to new data and shard changes.
        """
        def stream_manager():
            logger.info(f"Starting low-impact stream manager for {table_name} "
                        f"(poll: {poll_interval}s, shard check: {shard_check_interval}s)")
            watchers: Dict[str, threading.Thread] = {}

            try:
                while True:
                    try:
                        open_shards = self._list_open_shards(stream_arn)

                        for shard in open_shards:
                            shard_id = shard.get("ShardId")

                            if shard_id and shard_id not in watchers:
                                watcher_thread = threading.Thread(
                                    target=self._watch_shard,
                                    args=(stream_arn, shard_id, table_name, key_attrs, poll_interval),
                                    daemon=True
                                )
                                watchers[shard_id] = watcher_thread
                                watcher_thread.start()
                                logger.info(f"Started low-impact watcher for shard {shard_id}")

                        # Clean up dead threads
                        watchers = {sid: thread for sid, thread in watchers.items() if thread.is_alive()}

                    except Exception as e:
                        logger.warning(f"Stream manager encountered error (will retry): {e}")

                    # Check for new shards much less frequently to reduce API calls
                    time.sleep(shard_check_interval)

            except Exception as e:
                logger.error(f"Stream manager failed for {table_name}: {e}")
                raise

        manager_thread = threading.Thread(target=stream_manager, daemon=True)
        manager_thread.start()
        return manager_thread


def extract_index_attributes(table_desc: Dict[str, Any]) -> Set[str]:
    """
    Extract all attribute names that should be indexed in SQLite.

    Includes primary key attributes and all GSI key attributes to ensure
    efficient querying on important fields.
    """
    attributes: Set[str] = set()

    # Primary key attributes
    for key_def in table_desc.get("KeySchema", []):
        attributes.add(key_def["AttributeName"])

    # GSI key attributes
    for gsi in table_desc.get("GlobalSecondaryIndexes", []) or []:
        for key_def in gsi.get("KeySchema", []):
            attributes.add(key_def["AttributeName"])

    return attributes


def extract_primary_key_attributes(table_desc: Dict[str, Any]) -> List[str]:
    """
    Extract ordered list of primary key attributes for upsert operations.

    Returns partition key first, then sort key if present, matching
    DynamoDB's key schema ordering.
    """
    key_schema = table_desc.get("KeySchema", [])

    # Sort by KeyType to ensure HASH (partition) comes before RANGE (sort)
    sorted_keys = sorted(key_schema, key=lambda k: k.get("KeyType", ""))

    return [key_def["AttributeName"] for key_def in sorted_keys]


def perform_initial_load(sqlite_adapter: SQLiteAdapter, dynamodb_client: DynamoDBClient,
                         table_name: str, sqlite_table_name: str, key_attrs: List[str]) -> None:
    """
    Load all existing items from DynamoDB table to SQLite if table is empty.

    Only performs full scan if SQLite table has no existing data to avoid
    unnecessary work on restarts.
    """
    if sqlite_adapter.count_items(sqlite_table_name) > 0:
        logger.info(f"Table {sqlite_table_name} already has data, skipping initial load")
        return

    logger.info(f"Starting initial load for table {table_name}")
    items = dynamodb_client.scan_all_items(table_name)

    for item in items:
        sqlite_adapter.upsert_item(sqlite_table_name, item, key_attrs)

    logger.info(f"Initial load complete for {table_name}: {len(items)} items loaded")


def mirror_dynamodb_tables(table_mappings: Optional[List[TableMapping]], region: str,
                           db_path: str, poll_interval: int = 60,
                           shard_check_interval: int = 600, enable_electro: bool = True,
                           stage_prefix: Optional[str] = None) -> None:
    """
    Main entry point for DynamoDB to SQLite mirroring service.

    Orchestrates the complete mirroring process including initial loads
    and ongoing stream processing for real-time updates.
    """
    logger.info("Starting DynamoDB mirror service with serialized API calls")

    # Initialize single API queue to serialize all AWS calls
    api_queue = APICallQueue(region)
    sqlite_adapter = SQLiteAdapter(db_path)
    dynamodb_client = DynamoDBClient(api_queue)
    stream_processor = StreamProcessor(sqlite_adapter, TypeDeserializer(), api_queue)

    # Auto-discover tables if none provided
    if not table_mappings and stage_prefix:
        logger.info(f"Auto-discovering tables for stage: {stage_prefix}")
        table_mappings = discover_and_map_stage_tables(dynamodb_client, stage_prefix)
        if not table_mappings:
            logger.warning(f"No tables found for stage {stage_prefix}")
            return
    elif not table_mappings:
        logger.error("No table mappings provided and no stage prefix for auto-discovery")
        raise ValueError("Must provide either table_mappings or stage_prefix for auto-discovery")

    stream_threads = []

    for mapping in table_mappings:
        logger.info(f"Setting up mirroring for {mapping.dynamodb_name} -> {mapping.sqlite_name}")

        try:
            # Get table metadata
            table_desc = dynamodb_client.describe_table(mapping.dynamodb_name)
            indexed_attrs = extract_index_attributes(table_desc)
            key_attrs = extract_primary_key_attributes(table_desc)

            # Setup SQLite table and indexes
            sqlite_adapter.create_table_and_indexes(mapping.sqlite_name, indexed_attrs)

            # Perform initial load
            perform_initial_load(sqlite_adapter, dynamodb_client,
                                 mapping.dynamodb_name, mapping.sqlite_name, key_attrs)

            # Create entity views if electro is enabled
            if enable_electro:
                sqlite_adapter.refresh_entity_views(mapping.sqlite_name)

            # Start stream processing if enabled
            stream_spec = table_desc.get("StreamSpecification", {})
            if stream_spec.get("StreamEnabled"):
                stream_arn = table_desc.get("LatestStreamArn")
                if stream_arn:
                    stream_thread = stream_processor.start_stream_manager(
                        stream_arn, mapping.sqlite_name, key_attrs,
                        poll_interval, shard_check_interval
                    )
                    stream_threads.append(stream_thread)
                    logger.info(f"Stream processing enabled for {mapping.dynamodb_name}")
                else:
                    logger.warning(f"No stream ARN found for {mapping.dynamodb_name}")
            else:
                logger.info(f"Streams not enabled for {mapping.dynamodb_name}")

        except Exception as e:
            logger.error(f"Failed to setup mirroring for {mapping.dynamodb_name}: {e}")
            raise

    # Create union view for all tables
    table_names = [mapping.sqlite_name for mapping in table_mappings]
    sqlite_adapter.create_union_view(table_names)

    logger.info("DynamoDB mirror service started successfully")

    if stream_threads:
        logger.info(f"Running with low-impact settings: {poll_interval}s polling, "
                    f"{shard_check_interval}s shard checks")
        logger.info("Service is optimized for minimal resource usage - updates may take up to "
                    f"{poll_interval} seconds to appear")

    # Keep main thread alive while stream processors run
    try:
        while any(t.is_alive() for t in stream_threads):
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down DynamoDB mirror service")
    finally:
        # Clean up database connections
        logger.info("Cleaning up database connections")
        sqlite_adapter.close()


def convert_table_name_to_sqlite(dynamodb_name: str, stage_prefix: str) -> str:
    """
    Convert DynamoDB table name to clean SQLite table name.

    Handles stage prefix removal and converts camelCase/PascalCase to snake_case.
    Examples:
        daz3-bosphorus-LendingTable -> lending_table
        daz3-bosphorus-platformConfig -> platform_config
        daz3-bosphorus-onboarding -> onboarding
    """
    table_name = dynamodb_name

    # Remove stage-bosphorus prefix if present
    bosphorus_prefix = f"{stage_prefix}-bosphorus-"
    if table_name.startswith(bosphorus_prefix):
        table_name = table_name[len(bosphorus_prefix):]
    elif table_name.startswith(f"{stage_prefix}-"):
        # Fallback: remove stage prefix only
        table_name = table_name[len(f"{stage_prefix}-"):]

    # Convert to snake_case
    # Insert underscores before uppercase letters (but not at the start)
    result = re.sub(r'(?<!^)(?=[A-Z])', '_', table_name).lower()

    # Clean up any double underscores
    result = re.sub(r'_+', '_', result)
    result = result.strip('_')

    return result if result else "unknown_table"


def discover_and_map_stage_tables(dynamodb_client: DynamoDBClient, stage_prefix: str) -> List[TableMapping]:
    """
    Automatically discover all tables for a stage and create appropriate mappings.

    Returns a list of TableMapping objects for all discovered stage tables.
    """
    stage_tables = dynamodb_client.list_stage_tables(stage_prefix)

    mappings = []
    for table_name in stage_tables:
        sqlite_name = convert_table_name_to_sqlite(table_name, stage_prefix)
        mappings.append(TableMapping(table_name, sqlite_name))

    return mappings
