import copy
import hashlib
import json
import math
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "ro_field_differential_analysis"
    / "one-dimensional.json"
)


def _canonical_json(value):
    """Mirror webapp/src/canonicalization.jl for JSON-compatible values."""
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(str(key), ensure_ascii=False)
            + ":"
            + _canonical_json(value[key])
            for key in sorted(value)
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite value cannot be canonicalized")
        if value.is_integer():
            return str(int(value))
        text = repr(value).lower()
        if "e" in text:
            mantissa, exponent = text.split("e", 1)
            if "." not in mantissa:
                mantissa += ".0"
            return f"{mantissa}e{int(exponent)}"
        return text
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    raise TypeError(f"unsupported canonical JSON value: {type(value)!r}")


def _canonical_hash(document):
    payload = copy.deepcopy(document)
    payload.pop("analysis_id", None)
    payload.pop("analysis_sha256", None)
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _semantic_errors(document):
    errors = []
    digest = _canonical_hash(document)
    if document["analysis_sha256"] != digest:
        errors.append("analysis_sha256 must match canonical analysis content")
    if document["analysis_id"] != f"ro-differential-{digest[:24]}":
        errors.append("analysis_id must derive from analysis_sha256")

    axes = document["coordinate_contract"]["axis_order"]
    outputs = document["coordinate_contract"]["output_order"]
    input_count = len(axes)
    output_count = len(outputs)

    integrability = document["integrability"]
    if integrability["input_dimension"] != input_count:
        errors.append("integrability input dimension must match axis order")
    if integrability["output_count"] != output_count:
        errors.append("integrability output count must match output order")
    total = integrability["total_face_count"]
    evaluated = integrability["evaluated_face_count"]
    invalid = integrability["invalid_face_count"]
    violating = integrability["violating_face_count"]
    if evaluated + invalid != total or violating > evaluated:
        errors.append("integrability aggregate counts are inconsistent")
    expected_pair_count = input_count * (input_count - 1) // 2
    if len(integrability["pair_summaries"]) != expected_pair_count:
        errors.append("integrability axis-pair population is inconsistent")
    pair_counts = [0, 0, 0, 0]
    seen_pairs = set()
    for pair in integrability["pair_summaries"]:
        axis_pair = tuple(pair["axis_pair"])
        if not (1 <= axis_pair[0] < axis_pair[1] <= input_count):
            errors.append("integrability axis pair is invalid")
        if axis_pair in seen_pairs:
            errors.append("integrability axis pair is duplicated")
        seen_pairs.add(axis_pair)
        values = [
            pair["total_face_count"],
            pair["evaluated_face_count"],
            pair["invalid_face_count"],
            pair["violating_face_count"],
        ]
        if values[1] + values[2] != values[0] or values[3] > values[1]:
            errors.append("integrability pair counts are inconsistent")
        pair_counts = [left + right for left, right in zip(pair_counts, values)]
    if pair_counts != [total, evaluated, invalid, violating]:
        errors.append("integrability pair and aggregate counts disagree")
    expected_integrability_status = (
        "insufficient_grid"
        if total == 0
        else "discrete_inconsistent"
        if violating
        else "unknown_gap"
        if invalid
        else "consistent_on_tested_grid"
    )
    if integrability["status"] != expected_integrability_status:
        errors.append("integrability status is inconsistent with counts")
    if integrability["complete"] != (total > 0 and invalid == 0):
        errors.append("integrability complete flag is inconsistent")

    curvature = document["curvature"]
    cell_shape = curvature["cell_shape"]
    if len(cell_shape) != input_count:
        errors.append("curvature cell rank must match input rank")
    cell_count = math.prod(cell_shape)
    if curvature["total_cell_count"] != cell_count:
        errors.append("curvature total count must match cell shape")
    cell_evaluated = curvature["evaluated_cell_count"]
    cell_invalid = curvature["invalid_cell_count"]
    if cell_evaluated + cell_invalid != cell_count:
        errors.append("curvature counts are inconsistent")
    validity = curvature["validity"]
    if len(validity) != cell_count or sum(validity) != cell_evaluated:
        errors.append("curvature validity is inconsistent")
    expected_curvature_status = (
        "insufficient_grid"
        if cell_count == 0
        else "no_valid_cells"
        if cell_evaluated == 0
        else "complete"
        if cell_invalid == 0
        else "partial"
    )
    if curvature["status"] != expected_curvature_status:
        errors.append("curvature status is inconsistent")
    if curvature["complete"] != (cell_count > 0 and cell_invalid == 0):
        errors.append("curvature complete flag is inconsistent")

    tensors = {
        "gradient_jacobian": cell_shape
        + [output_count, input_count, input_count],
        "symmetric_hessian": cell_shape
        + [output_count, input_count, input_count],
        "mixed_output_curvature": cell_shape
        + [output_count, input_count, input_count],
        "antisymmetry_residual": cell_shape + [output_count],
        "hessian_eigenvalues": cell_shape + [output_count, input_count],
    }
    for name, expected_shape in tensors.items():
        shape = curvature[f"{name}_shape"]
        values = curvature[f"{name}_values"]
        if shape != expected_shape or len(values) != math.prod(shape):
            errors.append(f"{name} shape or flattened length is inconsistent")

    square_block = output_count * input_count * input_count
    vector_blocks = {
        "gradient_jacobian": square_block,
        "symmetric_hessian": square_block,
        "mixed_output_curvature": square_block,
        "antisymmetry_residual": output_count,
        "hessian_eigenvalues": output_count * input_count,
    }
    for cell_index, valid in enumerate(validity):
        for name, block_size in vector_blocks.items():
            values = curvature[f"{name}_values"]
            block = values[cell_index * block_size : (cell_index + 1) * block_size]
            if not valid and any(value is not None for value in block):
                errors.append("invalid curvature cell must remain an all-null block")
            if valid and name != "mixed_output_curvature" and any(
                value is None for value in block
            ):
                errors.append("valid curvature cell requires finite tensor values")
            if valid and name == "mixed_output_curvature":
                for output in range(output_count):
                    for left in range(input_count):
                        for right in range(input_count):
                            index = (
                                output * input_count * input_count
                                + left * input_count
                                + right
                            )
                            if (left == right) != (block[index] is None):
                                errors.append(
                                    "mixed-curvature nulls must be exactly the diagonal"
                                )

    synergy = document["synergy"]
    expected_classification_shape = cell_shape + [
        output_count,
        input_count,
        input_count,
    ]
    classifications = synergy["classification_values"]
    if (
        synergy["classification_shape"] != expected_classification_shape
        or len(classifications) != math.prod(expected_classification_shape)
    ):
        errors.append("synergy classification shape is inconsistent")
    for cell_index, valid in enumerate(validity):
        block = classifications[
            cell_index * square_block : (cell_index + 1) * square_block
        ]
        for output in range(output_count):
            for left in range(input_count):
                for right in range(input_count):
                    label = block[
                        output * input_count * input_count
                        + left * input_count
                        + right
                    ]
                    if left == right and label != "not_applicable":
                        errors.append("synergy diagonal must be not_applicable")
                    elif left != right and not valid and label != "unknown_gap":
                        errors.append("invalid synergy cell must remain unknown")
                    elif left != right and valid and label not in {
                        "synergistic_under_policy",
                        "antagonistic_under_policy",
                        "neutral_under_policy",
                    }:
                        errors.append("valid synergy cell requires a policy label")
    expected_synergy_pairs = output_count * expected_pair_count
    if len(synergy["pair_summaries"]) != expected_synergy_pairs:
        errors.append("synergy pair population is inconsistent")
    if document["evidence"]["integrability_claim"] != integrability["status"]:
        errors.append("evidence integrability claim must match the certificate")
    return errors


class ROFieldDifferentialSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        analysis_schema_path = (
            ROOT / "schemas" / "ro-field-differential-analysis.schema.json"
        )
        request_schema_path = (
            ROOT / "schemas" / "ro-field-differential-request.schema.json"
        )
        ro_field_schema_path = ROOT / "schemas" / "ro-field.schema.json"
        schemas = [
            json.loads(path.read_text())
            for path in (
                analysis_schema_path,
                request_schema_path,
                ro_field_schema_path,
            )
        ]
        for schema in schemas:
            Draft202012Validator.check_schema(schema)
        registry = Registry().with_resources(
            [
                (schema["$id"], Resource.from_contents(schema))
                for schema in schemas
            ]
        )
        cls.analysis_validator = Draft202012Validator(
            schemas[0], registry=registry, format_checker=FormatChecker()
        )
        cls.request_validator = Draft202012Validator(
            schemas[1], registry=registry, format_checker=FormatChecker()
        )
        cls.fixture = json.loads(FIXTURE.read_text())
        cls.sampled_field = json.loads(
            (ROOT / "tests" / "fixtures" / "ro_field" / "sampled-grid.json").read_text()
        )
        cls.exact_field = json.loads(
            (
                ROOT
                / "tests"
                / "fixtures"
                / "ro_field"
                / "exact-cell-complex.json"
            ).read_text()
        )

    def assert_analysis_valid(self, document):
        schema_errors = sorted(
            self.analysis_validator.iter_errors(document),
            key=lambda error: list(error.path),
        )
        self.assertEqual([], [error.message for error in schema_errors])
        self.assertEqual([], _semantic_errors(document))

    def test_tracked_analysis_fixture_is_strict_and_hash_consistent(self):
        self.assert_analysis_valid(self.fixture)

    def test_hash_counts_shapes_and_gap_semantics_fail_closed(self):
        document = copy.deepcopy(self.fixture)
        document["analysis_id"] = "ro-differential-" + "f" * 24
        self.assertTrue(_semantic_errors(document))

        document = copy.deepcopy(self.fixture)
        document["curvature"]["total_cell_count"] = 2
        self.assertTrue(_semantic_errors(document))

        document = copy.deepcopy(self.fixture)
        document["curvature"]["gradient_jacobian_values"] = []
        self.assertTrue(_semantic_errors(document))

        document = copy.deepcopy(self.fixture)
        document["synergy"]["classification_values"] = ["unknown_gap"]
        self.assertTrue(_semantic_errors(document))

    def test_unknown_fields_and_unsupported_policy_are_rejected(self):
        document = copy.deepcopy(self.fixture)
        document["future_claim"] = True
        self.assertTrue(list(self.analysis_validator.iter_errors(document)))

        document = copy.deepcopy(self.fixture)
        document["synergy"]["policy"] = "causal_synergy"
        self.assertTrue(list(self.analysis_validator.iter_errors(document)))

    def test_request_schema_accepts_only_complete_inline_sampled_fields(self):
        request = {
            "schema_version": "bne-ro-field-differential-request/v1.0.0",
            "ro_field": self.sampled_field,
            "options": {},
        }
        self.assertFalse(list(self.request_validator.iter_errors(request)))

        request["options"]["max_faces"] = 100001
        self.assertTrue(list(self.request_validator.iter_errors(request)))

        exact_request = {
            "schema_version": "bne-ro-field-differential-request/v1.0.0",
            "ro_field": self.exact_field,
            "options": {},
        }
        self.assertTrue(list(self.request_validator.iter_errors(exact_request)))

        unknown = {
            "schema_version": "bne-ro-field-differential-request/v1.0.0",
            "ro_field": self.sampled_field,
            "options": {"future": True},
        }
        self.assertTrue(list(self.request_validator.iter_errors(unknown)))

        missing_version = copy.deepcopy(request)
        missing_version.pop("schema_version")
        self.assertTrue(list(self.request_validator.iter_errors(missing_version)))


if __name__ == "__main__":
    unittest.main()
