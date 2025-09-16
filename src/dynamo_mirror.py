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
from decimal import Decimal
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass
from pathlib import Path

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)


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


class DynamoDBClient:
    """
    Handles DynamoDB operations with optimized resource usage and connection reuse.

    Reuses boto3 sessions and clients to minimize connection overhead and
    memory usage while providing consistent error handling.
    """

    def __init__(self, region: str):
        self.region = region
        # Reuse session for connection pooling
        self.session = boto3.Session()
        self.client = self.session.client("dynamodb", region_name=region)
        self.resource = self.session.resource("dynamodb", region_name=region)

    def describe_table(self, table_name: str) -> Dict[str, Any]:
        """Get complete table description including schema and stream configuration."""
        try:
            response = self.client.describe_table(TableName=table_name)
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
        table = self.resource.Table(table_name)
        items = []

        try:
            response = table.scan()
            items.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))

            logger.info(f"Scanned {len(items)} items from table {table_name}")
            return items
        except Exception as e:
            logger.error(f"Failed to scan table {table_name}: {e}")
            raise


class StreamProcessor:
    """
    Processes DynamoDB streams for real-time table updates with minimal resource overhead.

    Reuses connections and implements configurable polling to reduce CPU, network,
    and memory usage while maintaining data consistency.
    """

    def __init__(self, sqlite_adapter: SQLiteAdapter, deserializer: TypeDeserializer, region: str):
        self.sqlite_adapter = sqlite_adapter
        self.deserializer = deserializer
        # Reuse session for better connection management
        self.session = boto3.Session()
        self.streams_client = self.session.client("dynamodbstreams", region_name=region)

    def _deserialize_item(self, dynamodb_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB typed item to plain Python dictionary."""
        return {key: self.deserializer.deserialize(value) for key, value in dynamodb_item.items()}

    def _list_all_shards(self, stream_arn: str, last_shard_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Recursively retrieve all shards for a stream, handling pagination."""
        try:
            if last_shard_id is None:
                response = self.streams_client.describe_stream(StreamArn=stream_arn)
            else:
                response = self.streams_client.describe_stream(
                    StreamArn=stream_arn,
                    ExclusiveStartShardId=last_shard_id
                )

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
            response = self.streams_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType=iterator_type
            )
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
                response = self.streams_client.get_records(ShardIterator=shard_iterator)

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


def mirror_dynamodb_tables(table_mappings: List[TableMapping], region: str,
                           db_path: str, poll_interval: int = 60,
                           shard_check_interval: int = 600) -> None:
    """
    Main entry point for DynamoDB to SQLite mirroring service.

    Orchestrates the complete mirroring process including initial loads
    and ongoing stream processing for real-time updates.
    """
    logger.info("Starting DynamoDB mirror service")

    # Initialize components with optimized resource usage
    sqlite_adapter = SQLiteAdapter(db_path)
    dynamodb_client = DynamoDBClient(region)
    stream_processor = StreamProcessor(sqlite_adapter, TypeDeserializer(), region)

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
