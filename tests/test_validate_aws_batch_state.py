from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "deploy" / "validate_aws_batch_state.py"
SPEC = importlib.util.spec_from_file_location("validate_aws_batch_state", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
validate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validate)


class AwsBatchExistingStateTests(unittest.TestCase):
    def test_compute_environment_must_match_requested_capacity_and_network(self) -> None:
        document = {
            "computeEnvironments": [
                {
                    "type": "MANAGED",
                    "state": "ENABLED",
                    "status": "VALID",
                    "computeResources": {
                        "type": "EC2",
                        "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                        "minvCpus": 0,
                        "maxvCpus": 16,
                        "instanceRole": "arn:aws:iam::123456789012:instance-profile/expected",
                        "subnets": ["subnet-b", "subnet-a"],
                        "securityGroupIds": ["sg-one"],
                        "instanceTypes": ["optimal"],
                    },
                }
            ]
        }
        arguments = argparse.Namespace(
            environment_type="MANAGED",
            compute_type="EC2",
            allocation_strategy="BEST_FIT_PROGRESSIVE",
            min_vcpus=0,
            max_vcpus=16,
            instance_role="arn:aws:iam::123456789012:instance-profile/expected",
            subnets="subnet-a,subnet-b",
            security_groups="sg-one",
            instance_types="optimal",
        )

        self.assertEqual(validate.validate_compute(document, arguments), [])
        arguments.max_vcpus = 32
        errors = validate.validate_compute(document, arguments)
        self.assertTrue(any("maxvCpus" in error for error in errors))

        arguments.max_vcpus = 16
        document["computeEnvironments"][0]["type"] = "UNMANAGED"
        document["computeEnvironments"][0]["computeResources"].update(
            {
                "type": "FARGATE",
                "allocationStrategy": "BEST_FIT",
                "minvCpus": 8,
                "instanceRole": "arn:aws:iam::123456789012:instance-profile/unexpected",
            }
        )
        errors = validate.validate_compute(document, arguments)
        for field in ("type", "allocationStrategy", "minvCpus", "instanceRole"):
            self.assertTrue(any(field in error for error in errors), (field, errors))

    def test_invalid_or_disabled_compute_environment_fails_closed(self) -> None:
        document = {
            "computeEnvironments": [
                {
                    "type": "MANAGED",
                    "state": "DISABLED",
                    "status": "INVALID",
                    "statusReason": "bad launch template",
                    "computeResources": {
                        "type": "EC2",
                        "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                        "minvCpus": 0,
                        "maxvCpus": 16,
                        "instanceRole": "arn:aws:iam::123456789012:instance-profile/expected",
                        "subnets": ["subnet-a"],
                        "securityGroupIds": ["sg-one"],
                        "instanceTypes": ["optimal"],
                    },
                }
            ]
        }
        arguments = argparse.Namespace(
            environment_type="MANAGED",
            compute_type="EC2",
            allocation_strategy="BEST_FIT_PROGRESSIVE",
            min_vcpus=0,
            max_vcpus=16,
            instance_role="arn:aws:iam::123456789012:instance-profile/expected",
            subnets="subnet-a",
            security_groups="sg-one",
            instance_types="optimal",
        )

        errors = validate.validate_compute(document, arguments)
        self.assertTrue(any("DISABLED" in error for error in errors))
        self.assertTrue(any("bad launch template" in error for error in errors))

    def test_job_queue_must_be_enabled_and_bound_to_expected_compute_environment(self) -> None:
        expected_arn = "arn:aws:batch:us-west-2:123456789012:compute-environment/example"
        document = {
            "jobQueues": [
                {
                    "state": "ENABLED",
                    "status": "VALID",
                    "priority": 1,
                    "computeEnvironmentOrder": [
                        {"order": 1, "computeEnvironment": expected_arn}
                    ],
                }
            ]
        }
        arguments = argparse.Namespace(priority=1, compute_environment=expected_arn)

        self.assertEqual(validate.validate_queue(document, arguments), [])
        arguments.compute_environment = expected_arn + "-other"
        errors = validate.validate_queue(document, arguments)
        self.assertTrue(any("computeEnvironmentOrder" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
