#!/usr/bin/env python3
"""Kafka to S3 Bronze Streaming Ingestion Worker.

Consumes real-time events from Apache Kafka, buffers records in memory
according to production-grade thresholds (size & time window), formats them as
clean Hive-partitioned CSV files matching the Medallion raw layer, and uploads
directly to AWS S3 Bronze bucket with manual offset commits (at-least-once / idempotent).

Flow:
1. Connects to Kafka topic ('user-events') on bootstrap servers ('localhost:9092').
2. Validates incoming message JSON against the employee schema.
3. Buffers valid employee records into an in-memory queue.
4. When batch_size or flush_interval_seconds threshold is reached:
   - Formats records as standard CSV with header.
   - Generates Hive-partitioned S3 key:
     raw/employees/year=YYYY/month=MM/day=DD/sample_data_stream_{timestamp}_off_{start}_{end}.csv
   - Uploads to S3 Bronze bucket with AES256 server-side encryption.
   - Commits Kafka offsets ONLY upon successful S3 write.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

# Configure rich logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_bronze_ingestion")

REQUIRED_FIELDS = ["emp_id", "name", "department", "salary", "city", "date_of_joining"]
CSV_HEADER = ["emp_id", "name", "department", "salary", "city", "date_of_joining"]


class BronzeBatchBuffer:
    """In-memory buffer that tracks records and offset boundaries."""

    def __init__(self, batch_size: int, flush_interval_seconds: float):
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.records: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.min_offset: Optional[int] = None
        self.max_offset: Optional[int] = None
        self.partition: Optional[int] = None
        self.topic: Optional[str] = None

    def add(self, topic: str, partition: int, offset: int, record: Dict[str, Any]):
        self.records.append(record)
        self.topic = topic
        self.partition = partition
        if self.min_offset is None or offset < self.min_offset:
            self.min_offset = offset
        if self.max_offset is None or offset > self.max_offset:
            self.max_offset = offset

    def should_flush(self) -> bool:
        if not self.records:
            return False
        if len(self.records) >= self.batch_size:
            return True
        if (time.time() - self.last_flush_time) >= self.flush_interval:
            return True
        return False

    def to_csv_string(self) -> str:
        """Convert buffer to standard CSV with header."""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_HEADER, lineterminator="\n")
        writer.writeheader()
        for rec in self.records:
            writer.writerow({k: rec.get(k, "") for k in CSV_HEADER})
        return output.getvalue()

    def reset(self):
        self.records.clear()
        self.last_flush_time = time.time()
        self.min_offset = None
        self.max_offset = None


class KafkaToBronzeWorker:
    """Manages Kafka consumption, S3 uploading, and offset tracking."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        s3_bucket: str,
        s3_prefix: str,
        batch_size: int = 50,
        flush_interval_seconds: float = 15.0,
        aws_region: str = "us-east-1",
        auto_offset_reset: str = "latest",
        max_batches: Optional[int] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip("/")
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.aws_region = aws_region
        self.auto_offset_reset = auto_offset_reset
        self.max_batches = max_batches

        self.running = True
        self.batches_flushed = 0
        self.total_records_ingested = 0

        self.buffer = BronzeBatchBuffer(batch_size, flush_interval_seconds)

        # Initialize AWS S3 client
        logger.info("Initializing AWS S3 Client for bucket: s3://%s...", self.s3_bucket)
        self.s3_client = boto3.client("s3", region_name=self.aws_region)

        # Initialize Kafka Consumer
        logger.info(
            "Connecting to Kafka at %s (Topic: '%s', Group: '%s', Offset: '%s')...",
            self.bootstrap_servers,
            self.topic,
            self.group_id,
            self.auto_offset_reset,
        )
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,  # CRITICAL: Manual commit for exactly-once/idempotent delivery
            value_deserializer=lambda m: self._safe_json_deserialize(m),
            consumer_timeout_ms=1000,  # Poll timeout allows periodic flush checks
        )

    @staticmethod
    def _safe_json_deserialize(raw_bytes: bytes) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(raw_bytes.decode("utf-8"))
        except Exception:
            return None

    def validate_record(self, data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not data or not isinstance(data, dict):
            return None
        # Check required employee fields
        for field in REQUIRED_FIELDS:
            if field not in data:
                return None
        return data

    def flush_to_s3(self) -> bool:
        """Upload current batch buffer to S3 Bronze and commit Kafka offset."""
        if not self.buffer.records:
            return False

        record_count = len(self.buffer.records)
        topic = self.buffer.topic or self.topic
        partition = self.buffer.partition if self.buffer.partition is not None else 0
        min_off = self.buffer.min_offset
        max_off = self.buffer.max_offset

        # Build Hive partition path
        now = datetime.now(timezone.utc)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        date_partition = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"
        filename = f"sample_data_stream_{timestamp_str}_part{partition}_off_{min_off}_to_{max_off}.csv"
        s3_key = f"{self.s3_prefix}/{date_partition}/{filename}"
        s3_uri = f"s3://{self.s3_bucket}/{s3_key}"

        csv_content = self.buffer.to_csv_string().encode("utf-8")

        logger.info(
            "⏳ Flushing batch #%d (%d records, offsets %s..%s) to S3...",
            self.batches_flushed + 1,
            record_count,
            min_off,
            max_off,
        )

        try:
            # 1. Upload to S3
            t0 = time.time()
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_content,
                ContentType="text/csv",
                ServerSideEncryption="AES256",
                Metadata={
                    "kafka-topic": topic,
                    "kafka-partition": str(partition),
                    "kafka-min-offset": str(min_off),
                    "kafka-max-offset": str(max_off),
                    "record-count": str(record_count),
                },
            )
            elapsed = time.time() - t0

            # 2. Commit Kafka offset (next offset to read is max_off + 1)
            tp = TopicPartition(topic, partition)
            self.consumer.commit({tp: OffsetAndMetadata(max_off + 1, None)})

            self.batches_flushed += 1
            self.total_records_ingested += record_count

            logger.info("✅ SUCCESS: Uploaded %d bytes to %s in %.2fs", len(csv_content), s3_uri, elapsed)
            logger.info(
                "📊 Progress: %d total batches flushed | %d total records ingested to Bronze.",
                self.batches_flushed,
                self.total_records_ingested,
            )

            # Reset buffer
            self.buffer.reset()
            return True

        except ClientError as e:
            logger.error("❌ Failed to upload batch to S3: %s", e)
            return False
        except Exception as e:
            logger.error("❌ Unexpected error during batch flush: %s", e)
            return False

    def run(self):
        """Main consumer loop."""
        logger.info("🚀 Starting Kafka to Bronze S3 Streaming Worker...")
        logger.info("   Batch Size Threshold : %d records", self.batch_size)
        logger.info("   Time Flush Threshold : %.1f seconds", self.flush_interval)
        logger.info("   Target S3 Bucket     : s3://%s/%s/", self.s3_bucket, self.s3_prefix)
        logger.info("   Listening for incoming events...")

        try:
            while self.running:
                # Poll messages with 1s timeout
                msg_pack = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)

                for tp, messages in msg_pack.items():
                    for msg in messages:
                        if not self.running:
                            break

                        val = msg.value
                        validated = self.validate_record(val)

                        if validated:
                            self.buffer.add(msg.topic, msg.partition, msg.offset, validated)
                            # Print live micro-tick
                            sys.stdout.write(
                                f"\r📥 Consumed emp_id={validated.get('emp_id')} ({validated.get('name')}, {validated.get('department')}) | Buffer: {len(self.buffer.records)}/{self.batch_size}  "
                            )
                            sys.stdout.flush()
                        else:
                            # Ignored / schema mismatch
                            logger.debug("Skipping unparseable or non-employee record at offset %d", msg.offset)

                        if self.buffer.should_flush():
                            sys.stdout.write("\n")
                            self.flush_to_s3()
                            if self.max_batches and self.batches_flushed >= self.max_batches:
                                logger.info("Reached maximum target batches (%d). Stopping.", self.max_batches)
                                self.running = False
                                break

                # Time-based flush trigger check even when poll returns 0 or few records
                if self.buffer.should_flush():
                    sys.stdout.write("\n")
                    self.flush_to_s3()
                    if self.max_batches and self.batches_flushed >= self.max_batches:
                        logger.info("Reached maximum target batches (%d). Stopping.", self.max_batches)
                        self.running = False

        except KeyboardInterrupt:
            logger.info("\nCaught keyboard interrupt. Gracefully flushing remaining records...")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        if self.buffer.records:
            logger.info("Flushing final %d buffered records before exit...", len(self.buffer.records))
            self.flush_to_s3()
        self.consumer.close()
        logger.info("🛑 Consumer closed. Total records ingested: %d", self.total_records_ingested)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka to S3 Bronze Streaming Consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap server(s)")
    parser.add_argument("--topic", default="user-events", help="Kafka topic to consume")
    parser.add_argument("--group-id", default="bronze-raw-ingestion-worker", help="Kafka consumer group ID")
    parser.add_argument(
        "--s3-bucket",
        default=os.getenv("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012"),
        help="AWS S3 Bronze bucket name",
    )
    parser.add_argument("--s3-prefix", default="raw/employees", help="S3 raw prefix")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records per S3 file")
    parser.add_argument("--flush-interval", type=float, default=15.0, help="Maximum seconds before flushing a batch")
    parser.add_argument("--from-beginning", action="store_true", help="Consume from earliest offset")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after flushing N batches (for testing)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    worker = KafkaToBronzeWorker(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        max_batches=args.max_batches,
    )

    # Register graceful shutdown handlers
    def _sig_handler(signum, frame):
        worker.running = False

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    worker.run()
