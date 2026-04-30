"""Profile constants for the conclusion-only periodic table layer."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

PROFILE_ID = "periodic_d_mu_v0"
PROFILE_VERSION = "0.1.0"
DEFAULT_SIGN_EPS = 1e-9

PROPERTY_IDS = (
    "sign_switch.v1",
    "ultrasensitivity.v1",
    "upper_bound_reachable.v1",
    "turning_dependency.v1",
    "mimo_gain.v1",
    "robustness_rindex.v1",
    "settle_to_zero.v1",
)

STATUS_YES_EXISTENCE = "YES_EXISTENCE"
STATUS_YES_MINIMAL = "YES_MINIMAL"
STATUS_NO_COMPLETE = "NO_COMPLETE"
STATUS_WITNESS_ONLY = "WITNESS_ONLY"
STATUS_UNKNOWN = "UNKNOWN"
STATUS_ERROR = "ERROR"


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(value: Any) -> str:
    return hashlib.sha1(stable_json_dumps(value).encode("utf-8")).hexdigest()


def default_profile() -> dict[str, Any]:
    return {
        "profile_id": PROFILE_ID,
        "profile_version": PROFILE_VERSION,
        "complex_space": {
            "representation": "nonnegative_integer_vector",
            "min_size": 1,
            "max_size_symbol": "mu",
        },
        "binding_grammar": {
            "reaction": "C_a + C_b <-> C_{a+b}",
            "max_product_size_symbol": "mu",
            "include_homomers": True,
            "include_complex_plus_base": True,
            "include_complex_plus_complex": True,
            "reactant_order_matters": False,
        },
        "network_semantics": {
            "candidate": "finite_subgraph_of_allowed_reactions",
            "canonicalize_base_species_permutations": True,
            "minimality_order": [
                "reaction_count",
                "assembly_depth",
                "complex_count",
                "program_length",
            ],
        },
        "slice_semantics": {
            "inputs": "conserved_totals",
            "siso": True,
            "mimo": True,
        },
        "sign_semantics": {
            "eps": DEFAULT_SIGN_EPS,
            "finite_signs": [-1, 0, 1],
            "singular_tokens": ["pos_inf", "neg_inf", "nan"],
            "compression": "run_length_only_keep_zero",
        },
        "storage_policy": {
            "store_full_atlas": False,
            "store_all_path_records": False,
            "store_conclusion_records": True,
            "store_minimal_witnesses": True,
            "store_certificates": True,
        },
    }


def profile_hash(profile: dict[str, Any] | None = None) -> str:
    return stable_hash(default_profile() if profile is None else profile)


def code_commit(repo_root: str | Path | None = None) -> str:
    root = Path(repo_root or Path.cwd())
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.run(
            ["git", "diff", "--quiet"],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode != 0
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        suffix = "-dirty" if dirty or untracked else ""
        return f"{commit}{suffix}"
    except Exception:
        return "unknown"
