import tempfile
import unittest
from pathlib import Path

from src.periodic_table.candidate_search import generate_candidate_networks
from src.periodic_table.complex_generator import allowed_binding_reactions, complex_space
from src.periodic_table.complete_definition import (
    STATUS_INHERITED_WITNESS,
    STATUS_UNKNOWN,
    STATUS_WITNESS_ONLY,
)
from src.periodic_table.dry_run import build_dry_run_records, normalize_properties, write_run_outputs
from src.periodic_table.inheritance import apply_inherited_witnesses
from src.periodic_table.network_generator import canonical_network_string
from src.periodic_table.result_schema import run_config
from src.periodic_table.sign_programs import (
    matrix_sign_pattern,
    rle_keep_zero,
    sign3,
    sign_program_summary,
)


class PeriodicTableSmokeTests(unittest.TestCase):
    def test_sign3_keeps_zero_as_finite_state(self):
        self.assertEqual(sign3(1.0)["sign"], 1)
        self.assertEqual(sign3(-1.0)["sign"], -1)
        self.assertEqual(sign3(0.0)["sign"], 0)
        self.assertEqual(sign3(1e-12, eps=1e-9)["sign"], 0)

    def test_singular_tokens_are_not_zero(self):
        self.assertEqual(sign3(float("inf"))["singular"], "pos_inf")
        self.assertEqual(sign3(float("-inf"))["singular"], "neg_inf")
        self.assertEqual(sign3(float("nan"))["singular"], "nan")
        self.assertFalse(sign3(float("inf"))["finite"])

    def test_rle_keeps_zero(self):
        self.assertEqual(rle_keep_zero([0, -1, 1, 0]), [0, -1, 1, 0])
        self.assertEqual(rle_keep_zero([1, 1, 0, 0, -1]), [1, 0, -1])
        self.assertEqual(rle_keep_zero([1, 0, 1]), [1, 0, 1])

    def test_sign_program_summary(self):
        summary = sign_program_summary([0, -1, 1, 0])
        self.assertTrue(summary["three_state"])
        self.assertTrue(summary["settle_to_zero"])
        self.assertTrue(summary["polarity_reversal"])
        self.assertEqual(summary["max_sign_switch_count"], 3)

    def test_settle_to_zero_is_not_polarity_reversal(self):
        summary = sign_program_summary([1, 0])
        self.assertEqual(summary["sign_word"], "+->0")
        self.assertTrue(summary["settle_to_zero"])
        self.assertIn("terminal_settle_to_zero", summary["mechanism_classes"])
        self.assertFalse(summary["polarity_reversal"])

    def test_zero_mediated_polarity_reversal_is_distinct(self):
        summary = sign_program_summary([1, 0, -1])
        self.assertEqual(summary["sign_word"], "+->0->-")
        self.assertTrue(summary["settle_to_zero"])
        self.assertTrue(summary["polarity_reversal"])
        self.assertIn("zero_mediated_polarity_reversal", summary["mechanism_classes"])
        self.assertIn("three_state_word", summary["mechanism_classes"])

    def test_matrix_sign_pattern_keeps_zero_entries(self):
        pattern = matrix_sign_pattern([[1, 0], [-2, float("inf")]])
        self.assertEqual(pattern["pattern"], [[1, 0], [-1, None]])
        self.assertEqual(pattern["singular"][0]["singular"], "pos_inf")

    def test_complex_space_and_reactions(self):
        self.assertEqual(len(complex_space(2, 3)), 9)
        reactions = allowed_binding_reactions(2, 3)
        labels = {reaction.canonical_string() for reaction in reactions}
        self.assertIn("A + B <-> AB", labels)
        self.assertIn("A + AB <-> AAB", labels)
        self.assertIn("A + A <-> AA", labels)

    def test_canonicalization_invariant_under_permutation(self):
        reaction = next(
            reaction
            for reaction in allowed_binding_reactions(2, 2)
            if reaction.canonical_string() == "A + B <-> AB"
        )
        permuted = reaction.permute((1, 0))
        self.assertEqual(
            canonical_network_string([reaction], 2),
            canonical_network_string([permuted], 2),
        )

    def test_bounded_candidates_are_unique_and_periodic(self):
        candidates = generate_candidate_networks(3, 3, max_reactions=2, limit=50, pair_limit=10)
        canonicals = [candidate.canonical for candidate in candidates]
        self.assertEqual(len(canonicals), len(set(canonicals)))
        self.assertTrue(any("A + B <-> AB" in candidate.reaction_strings for candidate in candidates))
        self.assertTrue(all(candidate.features()["reaction_count"] <= 2 for candidate in candidates))

    def test_single_base_species_includes_homomer_frontier(self):
        candidates = generate_candidate_networks(1, 2)
        self.assertTrue(candidates)
        self.assertTrue(any(candidate.reaction_strings == ["A + A <-> AA"] for candidate in candidates))
        self.assertTrue(all(candidate.input_symbols() == ["tA"] for candidate in candidates))

    def test_single_base_species_without_complexes_is_empty(self):
        self.assertEqual(generate_candidate_networks(1, 1), [])

    def test_multiple_base_species_requirement_remains_enforced(self):
        candidates = generate_candidate_networks(2, 3, max_reactions=2, limit=50, pair_limit=10)
        self.assertTrue(candidates)
        self.assertTrue(
            all(
                "A" in " ".join(candidate.reaction_strings)
                and "B" in " ".join(candidate.reaction_strings)
                for candidate in candidates
            )
        )

    def test_upper_bound_cells_inherit_smaller_witness(self):
        cells = [
            {
                "d": 2,
                "mu": 2,
                "property_id": "settle_to_zero.v1",
                "status": STATUS_WITNESS_ONLY,
                "witness_id": "w1",
                "min_r": 1,
                "min_assembly_depth": 1,
                "strength": {"sign_word": "+->0"},
            },
            {
                "d": 4,
                "mu": 5,
                "property_id": "settle_to_zero.v1",
                "status": STATUS_UNKNOWN,
                "witness_id": None,
                "strength": {},
            },
        ]
        witnesses = [
            {
                "witness_id": "w1",
                "source_metadata": {
                    "features": {"reaction_count": 1, "assembly_depth": 1, "complex_count": 3},
                },
            },
        ]
        updated = apply_inherited_witnesses(cells, witnesses)
        inherited = updated[1]
        self.assertEqual(inherited["status"], STATUS_INHERITED_WITNESS)
        self.assertEqual(inherited["witness_id"], "w1")
        self.assertEqual(inherited["min_r"], 1)
        self.assertEqual(inherited["strength"]["inherited_from_cell"]["d"], 2)

    def test_dry_run_outputs_verify_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            props = normalize_properties("sign_switch.v1,ultrasensitivity.v1")
            config = run_config(
                run_id="test_run",
                d_values=[1, 2],
                mu_values=[1, 2],
                property_ids=props,
                repo_root=Path.cwd(),
                dry_run=True,
            )
            cells, witnesses, certificates = build_dry_run_records(
                run_id="test_run",
                d_values=[1, 2],
                mu_values=[1, 2],
                property_ids=props,
                repo_root=Path.cwd(),
            )
            write_run_outputs(run_dir, config, cells, witnesses, certificates)
            self.assertTrue((run_dir / "cell_results.jsonl.gz").exists())
            self.assertTrue((run_dir / "certificates.jsonl.gz").exists())
            self.assertEqual(len(cells), 8)


if __name__ == "__main__":
    unittest.main()
