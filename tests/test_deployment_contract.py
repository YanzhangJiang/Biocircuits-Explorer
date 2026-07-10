from __future__ import annotations

import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentContractTests(unittest.TestCase):
    def test_image_liveness_and_compose_readiness_are_distinct(self):
        dockerfile = (ROOT / "deploy/Dockerfile").read_text(encoding="utf-8")
        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")

        healthcheck = re.search(r"(?ms)^HEALTHCHECK\b.*?^\s*CMD\s+(.+)$", dockerfile)
        self.assertIsNotNone(healthcheck)
        self.assertIn("http://127.0.0.1:8088/health", healthcheck.group(1))
        self.assertNotIn("/ready", healthcheck.group(1))

        self.assertIn('http://127.0.0.1:8088/ready', compose)
        self.assertNotIn('http://localhost:8088/"', compose)
        self.assertRegex(
            compose,
            r"(?s)depends_on:\s+.*?julia-app:\s+condition:\s+service_healthy",
        )

    def test_docker_pr_trigger_covers_every_copied_source_tree(self):
        workflow = (ROOT / ".github/workflows/docker.yml").read_text(encoding="utf-8")
        full_ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        for required in (
            '".dockerignore"',
            '"deploy/Dockerfile"',
            '"webapp/**"',
            '"Bnc_julia/**"',
            '"VERSION"',
        ):
            self.assertIn(required, workflow)

        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        for required in (
            "!VERSION",
            "!webapp/Project.toml",
            "!webapp/Manifest.toml",
            "!webapp/*.jl",
            "!webapp/src/**",
            "!webapp/public/**",
            "!webapp/scripts/**",
            "!Bnc_julia/**",
            "!deploy/Dockerfile",
        ):
            self.assertIn(required, dockerignore)

        self.assertNotIn("packages: write", workflow)
        self.assertIn('must equal v${version}', workflow)
        self.assertIn('tag_version=${version//+/_}', workflow)
        self.assertIn("load: true", workflow)
        self.assertIn("Start image and verify runtime contracts", workflow)
        self.assertIn("/api/v1/version", workflow)
        self.assertIn("ci-write-probe", workflow)
        self.assertIn('tags: ["v*"]', full_ci)

    def test_runtime_container_drops_root(self):
        dockerfile = (ROOT / "deploy/Dockerfile").read_text(encoding="utf-8")
        user_position = dockerfile.index("USER rop")
        command_position = dockerfile.index('CMD ["julia"')
        self.assertLess(user_position, command_position)
        mountpoint_position = dockerfile.index("/home/rop/app/webapp/job_store")
        self.assertLess(mountpoint_position, user_position)

        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("job-store-init:", compose)
        self.assertIn('user: "0:0"', compose)
        self.assertIn("chown -R rop:rop /job-store", compose)
        self.assertIn(".biocircuits-owner-v1", compose)
        self.assertRegex(
            compose,
            r"(?s)julia-app:.*?depends_on:.*?job-store-init:\s+condition:\s+service_completed_successfully",
        )
        self.assertIn("ENV JULIA_NUM_THREADS=auto", dockerfile)
        self.assertNotIn('CMD ["julia", "-t", "auto"', dockerfile)

    def test_source_compose_build_receives_release_identity(self):
        dockerfile = (ROOT / "deploy/Dockerfile").read_text(encoding="utf-8")
        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")
        deploy = (ROOT / "deploy/deploy.sh").read_text(encoding="utf-8")
        self.assertIn('ARG BIOCIRCUITS_EXPLORER_VERSION=""', dockerfile)
        self.assertIn("BIOCIRCUITS_EXPLORER_VERSION: ${BIOCIRCUITS_EXPLORER_VERSION:-}", compose)
        self.assertIn('SOURCE_VERSION="$(< "$INSTALL_DIR/VERSION")"', deploy)
        self.assertIn('set_version.sh" --dry-run "$SOURCE_VERSION"', deploy)
        self.assertIn('BIOCIRCUITS_EXPLORER_VERSION="$SOURCE_VERSION"', deploy)
        self.assertIn("BIOCIRCUITS_EXPLORER_REVISION BIOCIRCUITS_EXPLORER_CREATED", deploy)

    def test_proxy_does_not_mix_host_static_assets_with_image_backend(self):
        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")
        nginx = (ROOT / "deploy/nginx.conf").read_text(encoding="utf-8")
        self.assertNotIn("../webapp/public", compose)
        self.assertNotIn("/usr/share/nginx/html/public", nginx)
        self.assertIn("location / {", nginx)
        self.assertIn("proxy_pass http://julia_app", nginx)

    def test_proxy_payload_limit_uses_the_api_error_contract(self):
        nginx = (ROOT / "deploy/nginx.conf").read_text(encoding="utf-8")
        self.assertIn("client_max_body_size 1m", nginx)
        self.assertIn("error_page 413 = @json_payload_too_large", nginx)
        self.assertIn('"code":"request_body_too_large"', nginx)
        self.assertIn('"limit_bytes":1048576', nginx)

    def test_tls_proxy_is_templated_and_deploy_fails_closed(self):
        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")
        nginx = (ROOT / "deploy/nginx.conf").read_text(encoding="utf-8")
        deploy = (ROOT / "deploy/deploy.sh").read_text(encoding="utf-8")
        self.assertIn("/etc/nginx/templates/default.conf.template", compose)
        self.assertIn("${BIOCIRCUITS_EXPLORER_SERVER_NAME}", nginx)
        self.assertIn("Missing readable TLS file", deploy)
        self.assertIn("openssl x509", deploy)
        self.assertIn("-checkend 86400", deploy)
        self.assertIn('CERT_PUBLIC_KEY=', deploy)
        self.assertIn('KEY_PUBLIC_KEY=', deploy)
        self.assertIn("docker compose up -d --wait --wait-timeout 900", deploy)
        self.assertIn("Attempting rollback", deploy)
        self.assertIn("rendered deployment contract", deploy)
        self.assertIn('docker compose -f "$ROLLBACK_CONFIG"', deploy)
        self.assertIn("LEGACY_STATIC_SNAPSHOT", deploy)
        self.assertIn("rewrite_rollback_config.py", deploy)
        self.assertIn('VERSION_TAG="${SOURCE_VERSION//+/_}"', deploy)
        self.assertIn("require_release_image_reference", deploy)
        self.assertNotIn("apt-get upgrade", deploy)
        self.assertNotIn("certbot --nginx", deploy)
        self.assertNotIn("Access the application at: http://$PUBLIC_IP", deploy)
        self.assertIn("git pull --ff-only", deploy)
        self.assertIn("status --porcelain --untracked-files=normal", deploy)
        self.assertIn('set_version.sh" --dry-run', deploy)
        self.assertIn('rev-parse HEAD', deploy)
        self.assertIn("https://127.0.0.1/health", compose)

    def test_compose_configuration_parses_when_plugin_is_available(self):
        docker = shutil.which("docker")
        if docker is None:
            self.skipTest("docker CLI is not installed")
        probe = subprocess.run(
            [docker, "compose", "version"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if probe.returncode != 0:
            self.skipTest("docker compose plugin is not installed")
        result = subprocess.run(
            [docker, "compose", "-f", "deploy/docker-compose.yml", "config", "--quiet"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_deployment_shell_entrypoints_parse(self):
        commands = [
            ("bash", "deploy/build_image.sh"),
            ("bash", "deploy/deploy.sh"),
            ("bash", "deploy/setup_aws_batch.sh"),
            ("bash", "deploy/image_reference.sh"),
            ("bash", "scripts/build_macos_dmg.sh"),
            ("bash", "webapp/start.sh"),
            ("sh", "frontend-swift/scripts/copy_backend_into_app.sh"),
        ]
        for shell, relative in commands:
            with self.subTest(script=relative):
                result = subprocess.run(
                    [shell, "-n", relative],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_local_start_script_defaults_to_loopback_and_propagates_engine_port(self):
        script = (ROOT / "webapp/start.sh").read_text(encoding="utf-8")
        self.assertIn("ROP_HOST:-127.0.0.1", script)
        self.assertIn('export BIOCIRCUITS_EXPLORER_HOST="$HOST" ROP_HOST="$HOST"', script)
        self.assertIn('export BIOCIRCUITS_EXPLORER_PORT="$PORT" ROP_PORT="$PORT"', script)
        self.assertIn("BIOCIRCUITS_EXPLORER_PARENT_PID", script)

    def test_aws_starter_policy_uses_real_s3_actions(self):
        policy = json.loads(
            (ROOT / "deploy/aws_setup_permissions_policy.json").read_text(encoding="utf-8")
        )
        actions = {
            action
            for statement in policy["Statement"]
            for action in statement.get("Action", [])
        }
        self.assertNotIn("s3:HeadBucket", actions)
        self.assertIn("s3:ListBucket", actions)

    def test_cognito_examples_use_the_real_spa_callback(self):
        guide = (ROOT / "deploy/AWS_BATCH.md").read_text(encoding="utf-8")
        self.assertNotIn("/auth/callback", guide)
        self.assertIn("/auth-callback.html", guide)

    def test_aws_examples_do_not_target_a_private_or_mutable_image(self):
        guide = (ROOT / "deploy/AWS_BATCH.md").read_text(encoding="utf-8")
        example = (ROOT / "deploy/aws-runtime.env.example").read_text(encoding="utf-8")
        setup = (ROOT / "deploy/setup_aws_batch.sh").read_text(encoding="utf-8")
        combined = guide + example + setup
        self.assertNotIn("234270344246", combined)
        self.assertNotIn(":latest", combined)
        self.assertIn("BIOCIRCUITS_EXPLORER_IMAGE=", example)

    def test_aws_setup_preserves_operator_env_and_fails_closed_on_wait_timeout(self):
        setup = (ROOT / "deploy/setup_aws_batch.sh").read_text(encoding="utf-8")
        self.assertIn("Preserve operator-owned settings", setup)
        self.assertIn('temporary_env="$(mktemp', setup)
        self.assertIn('mv -f "$temporary_env" "$OUTPUT_ENV_FILE"', setup)
        self.assertIn('runtime_job_queue=""', setup)
        self.assertNotIn("did not reach VALID within the wait window; continuing", setup)
        self.assertGreaterEqual(
            setup.count('did not reach VALID within the wait window." >&2\n    exit 1'),
            2,
        )
        for expected_compute_field in (
            "--environment-type MANAGED",
            "--compute-type EC2",
            "--allocation-strategy BEST_FIT_PROGRESSIVE",
            "--min-vcpus 0",
            '--instance-role "$profile_arn"',
        ):
            self.assertIn(expected_compute_field, setup)

    def test_compose_does_not_claim_an_unmounted_aws_profile(self):
        compose = (ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8")
        self.assertNotIn("AWS_PROFILE", compose)


if __name__ == "__main__":
    unittest.main()
