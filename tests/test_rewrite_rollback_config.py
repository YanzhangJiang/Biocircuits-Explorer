from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "deploy" / "rewrite_rollback_config.py"
SPEC = importlib.util.spec_from_file_location("rewrite_rollback_config", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
rewrite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rewrite)


class RollbackConfigTests(unittest.TestCase):
    def test_previous_proxy_and_static_paths_are_both_snapshotted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "docker-compose.yml"
            path.write_text(
                "services:\n"
                "  nginx:\n"
                "    volumes:\n"
                "      - /opt/Biocircuits-Explorer/deploy/nginx.conf:/etc/nginx/conf.d/default.conf:ro\n"
                "      - /opt/Biocircuits-Explorer/webapp/public:/usr/share/nginx/html/public:ro\n",
                encoding="utf-8",
            )

            rewrite.rewrite_config(
                path,
                [
                    (
                        "/opt/Biocircuits-Explorer/deploy/nginx.conf",
                        "/opt/rollbacks/one/nginx.conf",
                    ),
                    (
                        "/opt/Biocircuits-Explorer/webapp/public",
                        "/opt/rollbacks/one/public",
                    ),
                ],
            )

            rendered = path.read_text(encoding="utf-8")
            self.assertIn("/opt/rollbacks/one/nginx.conf", rendered)
            self.assertIn("/opt/rollbacks/one/public", rendered)
            self.assertNotIn("/opt/Biocircuits-Explorer/webapp/public", rendered)

    def test_missing_source_path_fails_closed_without_modifying_config(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "docker-compose.yml"
            original = "services: {}\n"
            path.write_text(original, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "does not reference"):
                rewrite.rewrite_config(path, [("missing", "snapshot")])

            self.assertEqual(path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
