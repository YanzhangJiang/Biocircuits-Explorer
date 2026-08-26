import copy
import json
import math
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "ro_field_request"


def _semantic_errors(document):
    errors = []
    domain = document["domain"]
    axis_order = domain["axis_order"]
    axes = domain["axes"]
    if axis_order != [axis["axis_id"] for axis in axes]:
        errors.append("domain.axis_order must equal the axes axis_id sequence")
    if any(axis["bounds"]["lower"] >= axis["bounds"]["upper"] for axis in axes):
        errors.append("every input bound must satisfy lower < upper")

    outputs = document["outputs"]
    if outputs["output_order"] != [item["output_id"] for item in outputs["items"]]:
        errors.append("outputs.output_order must equal the item output_id sequence")

    if document["representation"] == "sampled_grid":
        coordinates = document["sampling"]["axis_coordinates"]
        if len(coordinates) != len(axes):
            errors.append("sampling must provide one coordinate vector per input axis")
        for axis_index, values in enumerate(coordinates[: len(axes)]):
            if any(right <= left for left, right in zip(values, values[1:])):
                errors.append("sampling coordinates must be strictly increasing")
            lower = axes[axis_index]["bounds"]["lower"]
            upper = axes[axis_index]["bounds"]["upper"]
            if any(value < lower or value > upper for value in values):
                errors.append("sampling coordinates must stay inside declared bounds")
        requested = math.prod(len(values) for values in coordinates)
        if requested > document["work_budget"]["max_evaluated_items"]:
            errors.append("Cartesian sample count exceeds max_evaluated_items")
    else:
        if len(axes) != 2:
            errors.append("exact_cell_complex v1 requires exactly two axes")
        tolerance = document["exact_options"]["geometry_tolerance"]
        minimum_span = min(
            axis["bounds"]["upper"] - axis["bounds"]["lower"]
            for axis in axes
        )
        if domain["log_basis"] == "natural_log":
            minimum_span /= math.log(10.0)
        maximum_tolerance = min(1e-6, 1e-6 * minimum_span)
        if tolerance > maximum_tolerance:
            errors.append(
                "geometry_tolerance exceeds the shortest-side relative cap"
            )

    return errors


class ReactionOrderFieldRequestSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema_paths = [
            ROOT / "schemas" / "ro-field-request.schema.json",
            ROOT / "schemas" / "ro-field.schema.json",
            ROOT / "schemas" / "network-ir.schema.json",
        ]
        documents = [json.loads(path.read_text()) for path in schema_paths]
        cls.schema = documents[0]
        Draft202012Validator.check_schema(cls.schema)
        registry = Registry().with_resources(
            [
                (document["$id"], Resource.from_contents(document))
                for document in documents
            ]
        )
        cls.validator = Draft202012Validator(
            cls.schema,
            registry=registry,
            format_checker=FormatChecker(),
        )
        cls.fixtures = {
            path.stem: json.loads(path.read_text())
            for path in sorted(FIXTURE_ROOT.glob("*.json"))
        }

    def assert_contract_valid(self, document):
        errors = sorted(self.validator.iter_errors(document), key=lambda error: list(error.path))
        self.assertEqual([], [error.message for error in errors])
        self.assertEqual([], _semantic_errors(document))

    def test_inline_hash_and_session_model_sources_are_valid(self):
        self.assertEqual(
            {
                "sampled-inline-network",
                "exact-network-hash",
                "sampled-session-reference",
            },
            set(self.fixtures),
        )
        for name, document in self.fixtures.items():
            with self.subTest(name=name):
                self.assert_contract_valid(document)

    def test_exactly_one_model_source_is_required(self):
        document = copy.deepcopy(self.fixtures["sampled-inline-network"])
        document["network_ir_hash"] = "a" * 64
        self.assertTrue(list(self.validator.iter_errors(document)))

        del document["network"]
        del document["network_ir_hash"]
        self.assertTrue(list(self.validator.iter_errors(document)))

    def test_representation_specific_sampling_and_limits_fail_closed(self):
        sampled = copy.deepcopy(self.fixtures["sampled-inline-network"])
        del sampled["sampling"]
        self.assertTrue(list(self.validator.iter_errors(sampled)))

        sampled = copy.deepcopy(self.fixtures["sampled-inline-network"])
        sampled["exact_options"] = copy.deepcopy(
            self.fixtures["exact-network-hash"]["exact_options"]
        )
        self.assertTrue(list(self.validator.iter_errors(sampled)))

        exact = copy.deepcopy(self.fixtures["exact-network-hash"])
        exact["sampling"] = {"scheme": "cartesian_product", "axis_coordinates": [[0], [0]]}
        self.assertTrue(list(self.validator.iter_errors(exact)))

        exact = copy.deepcopy(self.fixtures["exact-network-hash"])
        exact["work_budget"]["work_unit_kind"] = "solver_samples"
        self.assertTrue(list(self.validator.iter_errors(exact)))

    def test_non_inline_storage_is_portably_expressible(self):
        self.assertEqual("chunked", self.fixtures["exact-network-hash"]["storage"]["mode"])
        self.assertEqual(
            "artifact_reference",
            self.fixtures["sampled-session-reference"]["storage"]["mode"],
        )
        self.assert_contract_valid(self.fixtures["exact-network-hash"])
        self.assert_contract_valid(self.fixtures["sampled-session-reference"])

    def test_unknown_fields_bad_hashes_and_bad_limits_are_rejected(self):
        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["future_option"] = True
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["storage"]["path"] = "/tmp/not-part-of-the-contract"
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["network_ir_hash"] = "A" * 64
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["exact_options"]["limits"]["max_cells"] = 0
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["exact_options"]["limits"]["max_pair_checks"] = 8193
        self.assertTrue(list(self.validator.iter_errors(document)))

    def test_synchronous_structural_caps_are_bounded(self):
        document = copy.deepcopy(self.fixtures["sampled-inline-network"])
        document["work_budget"]["max_evaluated_items"] = 4097
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        template = document["outputs"]["items"][0]
        document["outputs"]["items"] = []
        document["outputs"]["output_order"] = []
        for index in range(5):
            output = copy.deepcopy(template)
            output["output_id"] = f"output_{index}"
            output["symbol"] = f"x{index}"
            document["outputs"]["items"].append(output)
            document["outputs"]["output_order"].append(output["output_id"])
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["sampled-inline-network"])
        document["work_budget"]["deadline_seconds"] = 30.1
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["sampled-session-reference"])
        for suffix in ("d", "e"):
            extra = copy.deepcopy(document["domain"]["axes"][0])
            extra.update({"axis_id": f"input_{suffix}", "symbol": f"t{suffix.upper()}"})
            document["domain"]["axis_order"].append(f"input_{suffix}")
            document["domain"]["axes"].append(extra)
            document["sampling"]["axis_coordinates"].append([-0.1, 0.1])
        self.assertTrue(list(self.validator.iter_errors(document)))

    def test_coordinate_order_rank_and_bounds_are_semantic(self):
        document = copy.deepcopy(self.fixtures["sampled-inline-network"])
        document["domain"]["axis_order"].reverse()
        document["sampling"]["axis_coordinates"][0] = [-1.0, 0.5, 0.25]
        document["sampling"]["axis_coordinates"][1][-1] = 2.0
        errors = _semantic_errors(document)
        self.assertTrue(any("axis_order" in error for error in errors))
        self.assertTrue(any("strictly increasing" in error for error in errors))
        self.assertTrue(any("declared bounds" in error for error in errors))

    def test_exact_geometry_tolerance_has_absolute_and_relative_caps(self):
        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        document["exact_options"]["geometry_tolerance"] = 1.0
        self.assertTrue(list(self.validator.iter_errors(document)))

        document = copy.deepcopy(self.fixtures["exact-network-hash"])
        for axis in document["domain"]["axes"]:
            axis["bounds"] = {"lower": 0.0, "upper": 1e-4}
        # This remains below the portable absolute Schema cap, but is too
        # loose to certify coverage on the declared tiny domain.
        document["exact_options"]["geometry_tolerance"] = 1e-9
        self.assertFalse(list(self.validator.iter_errors(document)))
        self.assertTrue(any(
            "relative cap" in error for error in _semantic_errors(document)
        ))


if __name__ == "__main__":
    unittest.main()
