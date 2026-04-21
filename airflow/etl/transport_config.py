"""
Optional TLS / SASL settings for PostgreSQL (psycopg2 / libpq) and Confluent Kafka clients.

When environment variables are unset, behaviour matches the previous defaults
(PLAINTEXT Kafka, no extra SSL parameters for Postgres).
"""

from __future__ import annotations

import os
from typing import Any, Dict
from urllib.parse import urlencode


def _strip(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    s = raw.strip()
    return s if s else None


def postgres_connect_kwargs() -> Dict[str, Any]:
    """
    Extra keyword arguments for psycopg2.connect() and connection pools.

    POSTGRES_SSLMODE: libpq sslmode, e.g. require, verify-full, verify-ca.
    POSTGRES_SSL_ROOT_CERT: path to CA file (sslrootcert).
    POSTGRES_SSL_CERT / POSTGRES_SSL_KEY: client certificate paths for mTLS.
    """
    out: Dict[str, Any] = {}
    mode = _strip("POSTGRES_SSLMODE")
    if mode:
        out["sslmode"] = mode
    root = _strip("POSTGRES_SSL_ROOT_CERT")
    if root:
        out["sslrootcert"] = root
    cert = _strip("POSTGRES_SSL_CERT")
    if cert:
        out["sslcert"] = cert
    key = _strip("POSTGRES_SSL_KEY")
    if key:
        out["sslkey"] = key
    return out


def postgres_connection_query_string() -> str:
    """URL query fragment for SQLAlchemy / pandas Postgres URLs (leading ? or empty)."""
    mode = _strip("POSTGRES_SSLMODE")
    if not mode:
        return ""
    params: Dict[str, str] = {"sslmode": mode}
    root = _strip("POSTGRES_SSL_ROOT_CERT")
    if root:
        params["sslrootcert"] = root
    cert = _strip("POSTGRES_SSL_CERT")
    if cert:
        params["sslcert"] = cert
    key = _strip("POSTGRES_SSL_KEY")
    if key:
        params["sslkey"] = key
    return "?" + urlencode(params)


def kafka_common_client_config() -> Dict[str, str]:
    """
    Extra confluent_kafka client config (merge into Producer / Consumer / AdminClient).

    KAFKA_SECURITY_PROTOCOL: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL, ...
    KAFKA_SSL_CA_LOCATION, KAFKA_SSL_CERTIFICATE_LOCATION, KAFKA_SSL_KEY_LOCATION,
    KAFKA_SSL_KEY_PASSWORD: TLS material (paths / secret as used by librdkafka).
    KAFKA_SASL_MECHANISMS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD: SASL.
    """
    cfg: Dict[str, str] = {}
    mapping = (
        ("KAFKA_SECURITY_PROTOCOL", "security.protocol"),
        ("KAFKA_SSL_CA_LOCATION", "ssl.ca.location"),
        ("KAFKA_SSL_CERTIFICATE_LOCATION", "ssl.certificate.location"),
        ("KAFKA_SSL_KEY_LOCATION", "ssl.key.location"),
        ("KAFKA_SSL_KEY_PASSWORD", "ssl.key.password"),
        (
            "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM",
            "ssl.endpoint.identification.algorithm",
        ),
        ("KAFKA_SASL_MECHANISMS", "sasl.mechanisms"),
        ("KAFKA_SASL_USERNAME", "sasl.username"),
        ("KAFKA_SASL_PASSWORD", "sasl.password"),
    )
    for env_key, client_key in mapping:
        val = _strip(env_key)
        if val is not None:
            cfg[client_key] = val
    return cfg


def kafka_config_for_log(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of client config safe to log (passwords redacted)."""
    sensitive = ("password", "secret", "token", "credential")
    out: Dict[str, Any] = {}
    for k, v in cfg.items():
        kl = str(k).lower()
        if any(s in kl for s in sensitive):
            out[k] = "<redacted>" if v not in (None, "") else v
        else:
            out[k] = v
    return out
