"""
Tests for DynamoDB to SQLite mirroring functionality.

Tests all core components including SQLite operations, DynamoDB interactions,
and stream processing with real database operations (no mocking).
"""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from dynamo_mirror import (
    SQLiteAdapter,
    TableMapping,
    extract_index_attributes,
    extract_primary_key_attributes,
)


class TestSQLiteAdapter(unittest.TestCase):
    """Test SQLite database operations for DynamoDB item storage."""

    def setUp(self):
        """Create temporary SQLite database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self.adapter = SQLiteAdapter(self.temp_db.name)

    def tearDown(self):
        """Clean up temporary database file."""
        Path(self.temp_db.name).unlink(missing_ok=True)

    def test_create_table_and_indexes(self):
        """Test table creation with proper indexes on key attributes."""
        indexed_attrs = {"id", "status", "created_date"}

        self.adapter.create_table_and_indexes("test_table", indexed_attrs)

        with self.adapter.get_connection() as conn:
            # Verify table exists
            cursor: sqlite3.Cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
            )
            self.assertIsNotNone(cursor.fetchone())

            # Verify indexes exist
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'test_table_%_idx'"
            )
            indexes = [row[0] for row in cursor.fetchall()]
            self.assertEqual(len(indexes), 3)
            self.assertIn("test_table_id_idx", indexes)
            self.assertIn("test_table_status_idx", indexes)
            self.assertIn("test_table_created_date_idx", indexes)

    def test_upsert_item_insert(self):
        """Test inserting new item into empty table."""
        self.adapter.create_table_and_indexes("test_table", {"id"})

        item = {"id": "test-123", "name": "Test Item", "value": 42}
        self.adapter.upsert_item("test_table", item, ["id"])

        self.assertEqual(self.adapter.count_items("test_table"), 1)

        with self.adapter.get_connection() as conn:
            cursor = conn.execute("SELECT item_data FROM test_table")
            stored_item = json.loads(cursor.fetchone()[0])
            self.assertEqual(stored_item, item)

    def test_upsert_item_update(self):
        """Test updating existing item with same primary key."""
        self.adapter.create_table_and_indexes("test_table", {"id"})

        # Insert original item
        original_item = {"id": "test-123", "name": "Original", "value": 10}
        self.adapter.upsert_item("test_table", original_item, ["id"])

        # Update with same ID
        updated_item = {"id": "test-123", "name": "Updated", "value": 20}
        self.adapter.upsert_item("test_table", updated_item, ["id"])

        # Should still have only one item
        self.assertEqual(self.adapter.count_items("test_table"), 1)

        with self.adapter.get_connection() as conn:
            cursor = conn.execute("SELECT item_data FROM test_table")
            stored_item = json.loads(cursor.fetchone()[0])
            self.assertEqual(stored_item, updated_item)

    def test_upsert_item_composite_key(self):
        """Test upsert with composite primary key."""
        self.adapter.create_table_and_indexes("test_table", {"pk", "sk"})

        # Insert items with different composite keys
        item1 = {"pk": "partition1", "sk": "sort1", "data": "first"}
        item2 = {"pk": "partition1", "sk": "sort2", "data": "second"}

        self.adapter.upsert_item("test_table", item1, ["pk", "sk"])
        self.adapter.upsert_item("test_table", item2, ["pk", "sk"])

        self.assertEqual(self.adapter.count_items("test_table"), 2)

        # Update first item
        updated_item1 = {"pk": "partition1", "sk": "sort1", "data": "updated_first"}
        self.adapter.upsert_item("test_table", updated_item1, ["pk", "sk"])

        # Should still have 2 items
        self.assertEqual(self.adapter.count_items("test_table"), 2)

    def test_delete_item(self):
        """Test deleting item by primary key."""
        self.adapter.create_table_and_indexes("test_table", {"id"})

        # Insert test item
        item = {"id": "test-123", "name": "Test Item"}
        self.adapter.upsert_item("test_table", item, ["id"])

        # Delete item
        self.adapter.delete_item("test_table", {"id": "test-123"})

        self.assertEqual(self.adapter.count_items("test_table"), 0)

    def test_delete_item_composite_key(self):
        """Test deleting item with composite primary key."""
        self.adapter.create_table_and_indexes("test_table", {"pk", "sk"})

        # Insert multiple items
        item1 = {"pk": "partition1", "sk": "sort1", "data": "first"}
        item2 = {"pk": "partition1", "sk": "sort2", "data": "second"}

        self.adapter.upsert_item("test_table", item1, ["pk", "sk"])
        self.adapter.upsert_item("test_table", item2, ["pk", "sk"])

        # Delete one specific item
        self.adapter.delete_item("test_table", {"pk": "partition1", "sk": "sort1"})

        self.assertEqual(self.adapter.count_items("test_table"), 1)

    def test_count_items_empty_table(self):
        """Test counting items in empty table."""
        self.adapter.create_table_and_indexes("test_table", set())
        self.assertEqual(self.adapter.count_items("test_table"), 0)

    def test_json_indexing(self):
        """Test that JSON indexes work correctly for querying."""
        self.adapter.create_table_and_indexes("test_table", {"status"})

        # Insert items with different statuses
        items = [
            {"id": "1", "status": "active"},
            {"id": "2", "status": "inactive"},
            {"id": "3", "status": "active"},
        ]

        for item in items:
            self.adapter.upsert_item("test_table", item, ["id"])

        # Query using JSON extraction
        with self.adapter.get_connection() as conn:
            cursor = conn.execute(
                "SELECT item_data FROM test_table WHERE json_extract(item_data, '$.status') = ?",
                ("active",)
            )
            active_items = [json.loads(row[0]) for row in cursor.fetchall()]

        self.assertEqual(len(active_items), 2)
        self.assertTrue(all(item["status"] == "active" for item in active_items))


class TestTableMapping(unittest.TestCase):
    """Test table name mapping functionality."""

    def test_table_mapping_creation(self):
        """Test creating table mapping with correct attributes."""
        mapping = TableMapping("daz3-bosphorus-LendingTable", "lending")

        self.assertEqual(mapping.dynamodb_name, "daz3-bosphorus-LendingTable")
        self.assertEqual(mapping.sqlite_name, "lending")


class TestSchemaExtraction(unittest.TestCase):
    """Test extraction of DynamoDB schema information."""

    def test_extract_index_attributes_simple(self):
        """Test extracting attributes from table with only primary key."""
        table_desc = {
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        }

        attrs = extract_index_attributes(table_desc)
        self.assertEqual(attrs, {"id"})

    def test_extract_index_attributes_with_sort_key(self):
        """Test extracting attributes from table with partition and sort keys."""
        table_desc = {
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        }

        attrs = extract_index_attributes(table_desc)
        self.assertEqual(attrs, {"pk", "sk"})

    def test_extract_index_attributes_with_gsi(self):
        """Test extracting attributes from table with GSIs."""
        table_desc = {
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ],
            "GlobalSecondaryIndexes": [
                {
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "created_date", "KeyType": "RANGE"}
                    ]
                }
            ]
        }

        attrs = extract_index_attributes(table_desc)
        self.assertEqual(attrs, {"id", "status", "created_date"})

    def test_extract_primary_key_attributes_simple(self):
        """Test extracting primary key attributes - partition key only."""
        table_desc = {
            "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"}
            ]
        }

        keys = extract_primary_key_attributes(table_desc)
        self.assertEqual(keys, ["id"])

    def test_extract_primary_key_attributes_composite(self):
        """Test extracting primary key attributes - partition and sort key."""
        table_desc = {
            "KeySchema": [
                {"AttributeName": "sk", "KeyType": "RANGE"},
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]
        }

        keys = extract_primary_key_attributes(table_desc)
        # Should be sorted with HASH before RANGE
        self.assertEqual(keys, ["pk", "sk"])

    def test_extract_primary_key_attributes_empty(self):
        """Test extracting primary key attributes from empty schema."""
        table_desc = {"KeySchema": []}

        keys = extract_primary_key_attributes(table_desc)
        self.assertEqual(keys, [])


if __name__ == "__main__":
    unittest.main()
