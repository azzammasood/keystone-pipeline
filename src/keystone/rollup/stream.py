"""`full` profile input: silver hours replayed into Redpanda as Avro (Phase 4, ADR-026).

GH Archive is an hourly archive, not a firehose, so the stream is a replay: hours are produced
in arrival order (event-time order of their content unless a run says otherwise), each record
in the Confluent wire format (magic byte, schema id, Avro body). The value schema is
`contracts/avro/gharchive_event.avsc`, registered under the topic's subject with BACKWARD
compatibility; a candidate schema is checked against the registry before it is registered.
"""

import io
import json
import struct
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from keystone.rollup.contract import fields, schema_text

SR_CONTENT = "application/vnd.schemaregistry.v1+json"
DEFAULT_BOOTSTRAP = "127.0.0.1:19092"
DEFAULT_REGISTRY = "http://127.0.0.1:18081"
TOPIC = "keystone.gharchive.events.v1"


class IncompatibleSchema(RuntimeError):
    pass


class Registry:
    """The few Schema Registry calls Keystone needs (Redpanda speaks the Confluent API)."""

    def __init__(self, url: str = DEFAULT_REGISTRY, client: httpx.Client | None = None) -> None:
        self.http = client or httpx.Client(base_url=url, timeout=10)

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
        return self.http.request(method, path, json=body, headers={"Content-Type": SR_CONTENT})

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> None:
        self._call("PUT", f"/config/{subject}", {"compatibility": level}).raise_for_status()

    def is_compatible(self, subject: str, schema: str) -> bool:
        """True when `schema` may follow the latest version (or the subject is new)."""
        r = self._call(
            "POST", f"/compatibility/subjects/{subject}/versions/latest", {"schema": schema}
        )
        if r.status_code == 404:  # no versions yet: anything goes
            return True
        r.raise_for_status()
        return bool(r.json()["is_compatible"])

    def register(self, subject: str, schema: str) -> int:
        """Check, then register; returns the schema id (idempotent for an equal schema)."""
        if not self.is_compatible(subject, schema):
            raise IncompatibleSchema(f"{subject}: schema is not BACKWARD compatible")
        r = self._call("POST", f"/subjects/{subject}/versions", {"schema": schema})
        r.raise_for_status()
        return int(r.json()["id"])


def subject_for(topic: str) -> str:
    return f"{topic}-value"  # TopicNameStrategy, what Flink's avro-confluent format expects


def ensure_topic(bootstrap: str, topic: str, partitions: int = 1) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    for fut in admin.create_topics([NewTopic(topic, partitions, 1)]).values():
        fut.result()


def delete_topic(bootstrap: str, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        for fut in admin.delete_topics([topic]).values():
            fut.result()


@dataclass
class ProduceStats:
    files: int = 0
    records: int = 0
    bytes: int = 0
    seconds: float = 0.0


def files_in_event_order(silver_dir: Path) -> list[Path]:
    """Silver events files ordered by the earliest event they hold (the replay order when no
    explicit arrival order is given; E1 file names are Pacific hours, so names do not sort)."""
    import duckdb

    types = sorted(p.as_posix() for p in (silver_dir / "hour_types").glob("*/*.parquet"))
    if not types:
        return []
    rows = duckdb.execute(
        """SELECT any_value(filename) AS f, min(created_at_min) AS m
        FROM read_parquet(?, filename = true) GROUP BY source_file ORDER BY m, f""",
        [types],
    ).fetchall()
    out = []
    for f, _ in rows:
        p = Path(f)
        out.append(p.parent.parent.parent / "events" / p.parent.name / p.name)
    return out


def _rows(path: Path) -> Iterable[dict[str, Any]]:
    """Contract columns of one silver events file; timestamps as epoch milliseconds (floor),
    exactly what DuckDB's projection keeps (`date_trunc('millisecond', ...)`)."""
    cols = [f.name for f in fields()]
    table = pq.read_table(path, columns=cols)
    arrays = []
    for f in fields():
        col = table.column(f.name)
        if f.is_timestamp:
            col = pc.cast(pc.cast(col, pa.timestamp("ms", tz="UTC"), safe=False), pa.int64())
        arrays.append(col)
    yield from pa.Table.from_arrays(arrays, names=cols).to_pylist()


def produce(
    files: list[Path],
    topic: str = TOPIC,
    bootstrap: str = DEFAULT_BOOTSTRAP,
    registry: Registry | None = None,
) -> ProduceStats:
    """Register the contract, then produce every row of `files`, in that order."""
    from confluent_kafka import Producer
    from fastavro import parse_schema, schemaless_writer

    registry = registry or Registry()
    subject = subject_for(topic)
    registry.set_compatibility(subject)
    schema_id = registry.register(subject, schema_text())
    parsed = parse_schema(json.loads(schema_text()))
    header = b"\x00" + struct.pack(">I", schema_id)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )
    stats = ProduceStats()
    started = time.perf_counter()
    errors: list[str] = []

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is not None:
            errors.append(str(err))

    for path in files:
        for row in _rows(path):
            buf = io.BytesIO()
            buf.write(header)
            schemaless_writer(buf, parsed, row)
            value = buf.getvalue()
            key = None if row["repo_id"] is None else str(row["repo_id"]).encode()
            while True:
                try:
                    producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            stats.records += 1
            stats.bytes += len(value)
        producer.poll(0)
        stats.files += 1
    if producer.flush(60) != 0 or errors:
        raise RuntimeError(f"produce failed: {errors[:3] or 'messages left in queue'}")
    stats.seconds = time.perf_counter() - started
    return stats
