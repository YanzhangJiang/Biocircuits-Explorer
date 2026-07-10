from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {
    ".jl",
    ".json",
    ".md",
    ".py",
    ".sh",
    ".swift",
    ".toml",
    ".yaml",
    ".yml",
}


def production_files() -> list[Path]:
    roots = [
        ROOT / "webapp/src",
        ROOT / "webapp/scripts/reader",
        ROOT / "webapp/scripts/synth",
        ROOT / "packaging",
        ROOT / "frontend-swift",
        ROOT / "deploy",
        ROOT / "tools/migration_parity",
    ]
    files = [
        path
        for directory in roots
        for path in directory.rglob("*")
        if path.is_file() and (path.suffix in TEXT_SUFFIXES or path.name == "project.pbxproj")
    ]
    files.extend(
        path
        for path in (ROOT / "webapp/scripts").iterdir()
        if path.is_file() and path.suffix in TEXT_SUFFIXES
    )
    return sorted(set(files))


class PortablePathTests(unittest.TestCase):
    def test_production_surfaces_have_no_personal_checkout_roots(self):
        slash = "/"
        forbidden = {
            "macOS user checkout": re.compile(slash + "Users/" + r"[^/\s'\"]+"),
            "personal Linux home": re.compile(slash + "home/" + r"(?!rop(?:/|$))[^/\s'\"]+"),
            "workstation RAID": re.compile(slash + "raid/"),
        }
        findings: list[str] = []
        for path in production_files():
            text = path.read_text(encoding="utf-8")
            for label, pattern in forbidden.items():
                for match in pattern.finditer(text):
                    line = text.count("\n", 0, match.start()) + 1
                    findings.append(f"{path.relative_to(ROOT)}:{line}: {label}")

        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
