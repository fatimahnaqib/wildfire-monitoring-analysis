"""
Dead-letter queue (DLQ) helpers for Kafka pipelines.

Produces JSON envelopes to a dedicated topic so invalid or unrecoverable messages
can be audited and reprocessed without blocking the main consumer.

Kept in sync with airflow/etl/kafka_dlq.py (standalone consumer image build context).
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_MAX_TEXT_PAYLOAD = 512_000
_MAX_B64_PAYLOAD = 2_000_000
_MAX_DETAIL = 8_000


def build_dlq_envelope(
    *,
    source: str,
    failure_reason: str,
    message: Any,
    detail: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a JSON-serializable DLQ envelope from a consumed Kafka message.

    Preserves original offsets for correlation and stores the payload as UTF-8
    text when possible, otherwise base64.
    """
    raw: bytes
    try:
        val = message.value()
        raw = val if isinstance(val, (bytes, bytearray)) else (val or b"")
        if not isinstance(raw, bytes):
            raw = bytes(raw)
    except Exception:
        raw = b""

    payload_text: Optional[str] = None
    payload_b64: Optional[str] = None
    try:
        payload_text = raw.decode("utf-8")
        if len(payload_text) > _MAX_TEXT_PAYLOAD:
            payload_text = payload_text[:_MAX_TEXT_PAYLOAD]
    except UnicodeDecodeError:
        b64 = base64.b64encode(raw).decode("ascii")
        if len(b64) > _MAX_B64_PAYLOAD:
            b64 = b64[:_MAX_B64_PAYLOAD]
        payload_b64 = b64

    env: Dict[str, Any] = {
        "schema_version": 1,
        "source": source,
        "failure_reason": failure_reason,
        "original_topic": message.topic(),
        "original_partition": message.partition(),
        "original_offset": message.offset(),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    if detail:
        env["detail"] = detail[:_MAX_DETAIL]
    if payload_text is not None:
        env["payload_text"] = payload_text
    elif payload_b64 is not None:
        env["payload_b64"] = payload_b64
    if context is not None:
        env["context"] = context
    return env


def produce_dlq_message(
    producer: Any,
    dlq_topic: str,
    envelope: Dict[str, Any],
    *,
    callback: Any = None,
) -> None:
    """Serialize envelope and produce to the DLQ topic."""
    payload = json.dumps(envelope, default=str).encode("utf-8")
    producer.produce(topic=dlq_topic, value=payload, callback=callback)


def publish_dlq_envelopes(
    producer: Any,
    dlq_topic: str,
    envelopes: List[Dict[str, Any]],
    *,
    flush_timeout: float = 30.0,
) -> None:
    """Produce multiple DLQ records and block until they are sent (or timeout)."""
    for env in envelopes:
        produce_dlq_message(producer, dlq_topic, env)
    producer.flush(timeout=flush_timeout)


def publish_single_dlq(
    producer: Any,
    dlq_topic: str,
    envelope: Dict[str, Any],
    *,
    flush_timeout: float = 15.0,
) -> None:
    """Produce one DLQ record and flush."""
    produce_dlq_message(producer, dlq_topic, envelope)
    producer.flush(timeout=flush_timeout)
