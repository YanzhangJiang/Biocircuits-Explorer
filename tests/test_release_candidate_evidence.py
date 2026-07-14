import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]


class ReleaseCandidateEvidenceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(
            (ROOT / "schemas/release-candidate-evidence.schema.json").read_text(encoding="utf-8")
        )
        cls.template = json.loads(
            (ROOT / "operations/evidence/release-candidate.template.json").read_text(encoding="utf-8")
        )
        cls.validator = Draft202012Validator(
            cls.schema,
            format_checker=FormatChecker(),
        )

    def test_not_run_template_is_valid(self):
        self.validator.validate(self.template)

    def test_passed_lane_requires_hashed_evidence(self):
        record = copy.deepcopy(self.template)
        record["lanes"]["registry"]["status"] = "passed"
        self.assertTrue(list(self.validator.iter_errors(record)))

        record["lanes"]["registry"]["environment"] = "authorized-registry-alias"
        record["lanes"]["registry"]["rollback"] = {
            "status": "not_applicable",
            "observed_at": None,
            "output_sha256": None,
            "result": "Immutable digests are retained; channel rollback is covered by separate evidence.",
        }
        record["lanes"]["registry"]["evidence"].append(
            {
                "kind": "signature-verification",
                "observed_at": "2026-07-15T00:00:00Z",
                "command": "cosign verify <digest>",
                "output_sha256": "a" * 64,
                "artifacts": ["registry-signature-verification.txt"],
            }
        )
        self.validator.validate(record)

    def test_passed_lane_requires_environment_and_completed_rollback(self):
        record = copy.deepcopy(self.template)
        lane = record["lanes"]["compose_tls"]
        lane["status"] = "passed"
        lane["evidence"].append(
            {
                "kind": "tls-and-readiness",
                "observed_at": "2026-07-15T00:00:00Z",
                "command": "authorized-compose-tls-check",
                "output_sha256": "b" * 64,
                "artifacts": ["compose-tls-check.txt"],
            }
        )
        self.assertTrue(list(self.validator.iter_errors(record)), "environment is still absent")

        lane["environment"] = "authorized-compose-host"
        self.assertTrue(list(self.validator.iter_errors(record)), "rollback is still not_run")

        lane["rollback"] = {
            "status": "passed",
            "observed_at": "2026-07-15T00:10:00Z",
            "output_sha256": "c" * 64,
            "result": "Previous digest restored and ready.",
        }
        self.validator.validate(record)

    def test_passed_evidence_requires_timestamp_artifact_and_rollback_result(self):
        record = copy.deepcopy(self.template)
        lane = record["lanes"]["slurm"]
        lane.update(
            {
                "status": "passed",
                "environment": "authorized-cluster",
                "evidence": [
                    {
                        "kind": "bounded-job",
                        "observed_at": "not-a-timestamp",
                        "command": "authorized-bounded-job",
                        "output_sha256": "4" * 64,
                        "artifacts": [],
                    }
                ],
                "rollback": {
                    "status": "passed",
                    "observed_at": "2026-07-15T00:10:00Z",
                    "output_sha256": "5" * 64,
                },
            }
        )
        self.assertTrue(list(self.validator.iter_errors(record)))

        lane["evidence"][0]["observed_at"] = "2026-07-15T00:00:00Z"
        lane["evidence"][0]["artifacts"] = ["slurm-bounded-job.txt"]
        self.assertTrue(list(self.validator.iter_errors(record)), "rollback result is absent")

        lane["rollback"]["result"] = "Previous adapter restored and bounded fixture passed."
        self.validator.validate(record)

    def test_overall_passed_requires_clean_frozen_identity_and_every_lane_passed(self):
        record = copy.deepcopy(self.template)
        record["overall_status"] = "passed"
        self.assertTrue(list(self.validator.iter_errors(record)))

        record["source"] = {
            "commit": "d" * 40,
            "dirty": False,
            "configuration_sha256": "e" * 64,
            "image_digest": "sha256:" + "f" * 64,
            "macos_artifact_sha256": "1" * 64,
        }
        for name, lane in record["lanes"].items():
            lane.update(
                {
                    "status": "passed",
                    "environment": f"authorized-{name}",
                    "evidence": [
                        {
                            "kind": f"{name}-qualification",
                            "observed_at": "2026-07-15T00:00:00Z",
                            "command": f"authorized-{name}-check",
                            "output_sha256": "2" * 64,
                            "artifacts": [f"{name}-qualification.txt"],
                        }
                    ],
                    "rollback": {
                        "status": "passed",
                        "observed_at": "2026-07-15T00:10:00Z",
                        "output_sha256": "3" * 64,
                        "result": "Prior candidate restored and checked.",
                    },
                }
            )
        self.validator.validate(record)

        record["source"]["dirty"] = True
        self.assertTrue(list(self.validator.iter_errors(record)))


if __name__ == "__main__":
    unittest.main()
