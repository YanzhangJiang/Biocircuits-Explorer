from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SETUP = ROOT / "deploy" / "setup_aws_batch.sh"


class AwsBatchSetupContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.fake_bin = Path(self.temporary.name)
        fake_aws = self.fake_bin / "aws"
        fake_aws.write_text(
            "#!/bin/sh\n"
            "case \"${1:-}:${2:-}\" in\n"
            "  sts:get-caller-identity) echo 123456789012; exit 0 ;;\n"
            "esac\n"
            "exit 0\n",
            encoding="utf-8",
        )
        fake_aws.chmod(0o755)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_setup(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PATH"] = str(self.fake_bin) + os.pathsep + environment["PATH"]
        for key in tuple(environment):
            if key.startswith("BIOCIRCUITS_EXPLORER_AWS_") or key in {
                "BIOCIRCUITS_EXPLORER_IMAGE",
                "AWS_DEFAULT_REGION",
                "AWS_REGION",
            }:
                environment.pop(key)
        return subprocess.run(
            [str(SETUP), "--region", "us-west-2", "--dry-run", *arguments],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_skip_compute_keeps_image_unconfigured_instead_of_using_latest(self) -> None:
        result = self.run_setup("--skip-compute")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("<not configured; compute skipped>", result.stdout)
        self.assertNotIn(":latest", result.stdout + result.stderr)

    def test_compute_requires_an_explicit_immutable_image(self) -> None:
        missing = self.run_setup()
        mutable = self.run_setup(
            "--skip-compute",
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:latest",
        )
        immutable = self.run_setup(
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:0.1.0-0123456789ab",
        )
        mutable_alias = self.run_setup(
            "--skip-compute",
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:dev",
        )
        mutable_alias_with_sha_suffix = self.run_setup(
            "--skip-compute",
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:latest-0123456789ab",
        )
        projected_semver = self.run_setup(
            "--skip-compute",
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:1.2.3-rc.4_build.9-0123456789ab",
        )
        digest = self.run_setup(
            "--skip-compute",
            "--image",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer@sha256:"
            + "a" * 64,
        )

        self.assertEqual(missing.returncode, 2)
        self.assertIn("--image", missing.stderr)
        self.assertEqual(mutable.returncode, 2)
        self.assertIn("version-commit tag", mutable.stderr)
        self.assertEqual(immutable.returncode, 0, immutable.stderr)
        self.assertEqual(mutable_alias.returncode, 2)
        self.assertIn("version-commit tag", mutable_alias.stderr)
        self.assertEqual(mutable_alias_with_sha_suffix.returncode, 2)
        self.assertIn("version-commit tag", mutable_alias_with_sha_suffix.stderr)
        self.assertEqual(projected_semver.returncode, 0, projected_semver.stderr)
        self.assertEqual(digest.returncode, 0, digest.stderr)

    def test_cognito_requires_distinct_callback_and_logout_contracts(self) -> None:
        missing_logout = self.run_setup(
            "--skip-compute",
            "--with-cognito",
            "--cognito-callback-urls",
            "https://app.example/auth-callback.html",
        )
        complete = self.run_setup(
            "--skip-compute",
            "--with-cognito",
            "--cognito-callback-urls",
            "https://app.example/auth-callback.html",
            "--cognito-logout-urls",
            "https://app.example",
        )

        self.assertEqual(missing_logout.returncode, 2)
        self.assertIn("--cognito-logout-urls", missing_logout.stderr)
        self.assertEqual(complete.returncode, 0, complete.stderr)

    def test_value_options_fail_with_usage_instead_of_unbound_variable(self) -> None:
        result = self.run_setup("--skip-compute", "--image", "--no-wait")

        self.assertEqual(result.returncode, 2)
        self.assertIn("Option --image requires a value", result.stderr)
        self.assertNotIn("unbound variable", result.stderr)


if __name__ == "__main__":
    unittest.main()
