from __future__ import annotations

import json
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

    def run_setup(
        self,
        *arguments: str,
        environment_overrides: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PATH"] = str(self.fake_bin) + os.pathsep + environment["PATH"]
        for key in tuple(environment):
            if key.startswith("BIOCIRCUITS_EXPLORER_AWS_") or key in {
                "BIOCIRCUITS_EXPLORER_ARTIFACT_CORS_ORIGINS",
                "BIOCIRCUITS_EXPLORER_IMAGE",
                "AWS_DEFAULT_REGION",
                "AWS_REGION",
            }:
                environment.pop(key)
        environment.update(environment_overrides or {})
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

    def test_artifact_bucket_cors_is_get_head_only(self) -> None:
        source = SETUP.read_text(encoding="utf-8")
        cors_configuration = next(
            line for line in source.splitlines() if 'cors_configuration="{' in line
        )
        policy = json.loads(
            (ROOT / "deploy" / "aws_setup_permissions_policy.json").read_text(
                encoding="utf-8"
            )
        )
        actions = {
            action
            for statement in policy["Statement"]
            for action in statement.get("Action", [])
        }

        self.assertIn("put-bucket-cors", source)
        self.assertIn('\\"AllowedMethods\\":[\\"GET\\",\\"HEAD\\"]', cors_configuration)
        self.assertNotIn("POST", cors_configuration)
        self.assertNotIn("PUT", cors_configuration)
        self.assertNotIn("DELETE", cors_configuration)
        self.assertIn("s3:PutBucketCORS", actions)

    def test_runtime_submitter_policy_can_reconcile_ambiguous_submissions(self) -> None:
        source = SETUP.read_text(encoding="utf-8")
        submitter_policy = source.split("ensure_submitter_policy()", 1)[1].split(
            "ensure_compute_environment()", 1
        )[0]

        for action in (
            "batch:SubmitJob",
            "batch:ListJobs",
            "batch:DescribeJobs",
            "batch:CancelJob",
            "batch:TerminateJob",
        ):
            self.assertIn(f'"{action}"', submitter_policy)

    def test_runtime_env_freezes_batch_region_and_account_identity(self) -> None:
        source = SETUP.read_text(encoding="utf-8")
        example = (ROOT / "deploy" / "aws-runtime.env.example").read_text(
            encoding="utf-8"
        )

        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION=$REGION", source)
        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID=$ACCOUNT_ID", source)
        filter_line = next(
            line for line in source.splitlines() if "grep -Ev" in line
        )
        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION=", filter_line)
        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID=", filter_line)
        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION=us-west-2", example)
        self.assertIn("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID=123456789012", example)

    def test_artifact_cors_defaults_and_explicit_origins_reach_fixture_command(self) -> None:
        defaults = self.run_setup("--skip-compute")
        configured = self.run_setup(
            "--skip-compute",
            environment_overrides={
                "BIOCIRCUITS_EXPLORER_AWS_ARTIFACT_CORS_ORIGINS":
                    "https://env.example"
            },
        )
        cli_override = self.run_setup(
            "--skip-compute",
            "--artifact-cors-origins",
            "https://cli.example,http://127.0.0.1:18088",
            environment_overrides={
                "BIOCIRCUITS_EXPLORER_AWS_ARTIFACT_CORS_ORIGINS":
                    "https://env.example"
            },
        )

        self.assertEqual(defaults.returncode, 0, defaults.stderr)
        self.assertIn(
            "Artifact CORS:     "
            "http://127.0.0.1:8088,http://localhost:8088,http://127.0.0.1:18088",
            defaults.stdout,
        )
        self.assertIn("s3api put-bucket-cors", defaults.stdout)
        self.assertEqual(configured.returncode, 0, configured.stderr)
        self.assertIn("Artifact CORS:     https://env.example", configured.stdout)
        self.assertIn("https://env.example", configured.stdout)
        self.assertEqual(cli_override.returncode, 0, cli_override.stderr)
        self.assertIn(
            "Artifact CORS:     https://cli.example,http://127.0.0.1:18088",
            cli_override.stdout,
        )
        self.assertNotIn("Artifact CORS:     https://env.example", cli_override.stdout)

    def test_cognito_logout_origins_are_merged_into_artifact_cors(self) -> None:
        result = self.run_setup(
            "--skip-compute",
            "--artifact-cors-origins",
            "https://artifacts.example,https://app.example",
            "--with-cognito",
            "--cognito-callback-urls",
            "https://app.example/auth-callback.html",
            "--cognito-logout-urls",
            "https://app.example,http://127.0.0.1:18088",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "Artifact CORS:     "
            "https://artifacts.example,https://app.example,http://127.0.0.1:18088",
            result.stdout,
        )
        self.assertEqual(result.stdout.count("https://app.example"), 2)

    def test_value_options_fail_with_usage_instead_of_unbound_variable(self) -> None:
        result = self.run_setup("--skip-compute", "--image", "--no-wait")

        self.assertEqual(result.returncode, 2)
        self.assertIn("Option --image requires a value", result.stderr)
        self.assertNotIn("unbound variable", result.stderr)


if __name__ == "__main__":
    unittest.main()
