#!/usr/bin/env python3
"""Fail closed when an existing AWS Batch resource differs from setup intent."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


def _one(document: dict[str, Any], key: str, label: str) -> dict[str, Any]:
    resources = document.get(key)
    if not isinstance(resources, list) or len(resources) != 1 or not isinstance(resources[0], dict):
        raise ValueError(f"expected exactly one {label}; found {resources!r}")
    return resources[0]


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def validate_compute(document: dict[str, Any], arguments: argparse.Namespace) -> list[str]:
    resource = _one(document, "computeEnvironments", "compute environment")
    compute = resource.get("computeResources")
    if not isinstance(compute, dict):
        return ["computeResources is missing"]

    errors: list[str] = []
    if resource.get("type") != arguments.environment_type:
        errors.append(
            f"type is {resource.get('type')!r}, expected {arguments.environment_type!r}"
        )
    if resource.get("state") != "ENABLED":
        errors.append(f"state is {resource.get('state')!r}, expected 'ENABLED'")
    if resource.get("status") == "INVALID":
        errors.append(f"status is INVALID: {resource.get('statusReason', 'no reason reported')}")
    if compute.get("maxvCpus") != arguments.max_vcpus:
        errors.append(
            f"maxvCpus is {compute.get('maxvCpus')!r}, expected {arguments.max_vcpus}"
        )
    for field, expected in (
        ("type", arguments.compute_type),
        ("allocationStrategy", arguments.allocation_strategy),
        ("minvCpus", arguments.min_vcpus),
        ("instanceRole", arguments.instance_role),
    ):
        if compute.get(field) != expected:
            errors.append(f"{field} is {compute.get(field)!r}, expected {expected!r}")
    for field, expected in (
        ("subnets", _csv_set(arguments.subnets)),
        ("securityGroupIds", _csv_set(arguments.security_groups)),
        ("instanceTypes", _csv_set(arguments.instance_types)),
    ):
        observed = compute.get(field)
        observed_set = set(observed) if isinstance(observed, list) else set()
        if observed_set != expected:
            errors.append(f"{field} is {sorted(observed_set)!r}, expected {sorted(expected)!r}")
    return errors


def validate_queue(document: dict[str, Any], arguments: argparse.Namespace) -> list[str]:
    resource = _one(document, "jobQueues", "job queue")
    errors: list[str] = []
    if resource.get("state") != "ENABLED":
        errors.append(f"state is {resource.get('state')!r}, expected 'ENABLED'")
    if resource.get("status") == "INVALID":
        errors.append(f"status is INVALID: {resource.get('statusReason', 'no reason reported')}")
    if resource.get("priority") != arguments.priority:
        errors.append(f"priority is {resource.get('priority')!r}, expected {arguments.priority}")

    order = resource.get("computeEnvironmentOrder")
    expected_order = [{"order": 1, "computeEnvironment": arguments.compute_environment}]
    if order != expected_order:
        errors.append(f"computeEnvironmentOrder is {order!r}, expected {expected_order!r}")
    return errors


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subparsers = result.add_subparsers(dest="kind", required=True)

    compute = subparsers.add_parser("compute")
    compute.add_argument("json_file", type=Path)
    compute.add_argument("--environment-type", required=True)
    compute.add_argument("--compute-type", required=True)
    compute.add_argument("--allocation-strategy", required=True)
    compute.add_argument("--min-vcpus", type=int, required=True)
    compute.add_argument("--max-vcpus", type=int, required=True)
    compute.add_argument("--instance-role", required=True)
    compute.add_argument("--subnets", required=True)
    compute.add_argument("--security-groups", required=True)
    compute.add_argument("--instance-types", required=True)

    queue = subparsers.add_parser("queue")
    queue.add_argument("json_file", type=Path)
    queue.add_argument("--priority", type=int, default=1)
    queue.add_argument("--compute-environment", required=True)
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        with arguments.json_file.open(encoding="utf-8") as handle:
            document = json.load(handle)
        errors = (
            validate_compute(document, arguments)
            if arguments.kind == "compute"
            else validate_queue(document, arguments)
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Cannot validate existing AWS Batch {arguments.kind} state: {error}", file=sys.stderr)
        return 1

    if errors:
        print(
            f"Existing AWS Batch {arguments.kind} resource differs from the requested contract:",
            file=sys.stderr,
        )
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print("Update/delete the resource explicitly, then rerun setup.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
