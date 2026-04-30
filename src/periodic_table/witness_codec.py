"""Stable ids for witnesses and certificates."""

from __future__ import annotations

from typing import Any

from .complete_definition import stable_hash


def make_witness_id(property_id: str, d: int, mu: int, payload: dict[str, Any]) -> str:
    digest = stable_hash({"property_id": property_id, "d": d, "mu": mu, "payload": payload})[:12]
    safe_property = property_id.replace(".", "_")
    return f"wit_d{d}_mu{mu}_{safe_property}_{digest}"


def make_certificate_id(property_id: str, d: int, mu: int, payload: dict[str, Any]) -> str:
    digest = stable_hash({"property_id": property_id, "d": d, "mu": mu, "payload": payload})[:12]
    safe_property = property_id.replace(".", "_")
    return f"cert_d{d}_mu{mu}_{safe_property}_{digest}"
