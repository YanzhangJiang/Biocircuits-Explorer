import copy
import json
import math
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "ro_field"


def _product(values):
    return math.prod(values)


def _signed_polygon_area(vertices):
    return sum(
        vertices[index][0] * vertices[(index + 1) % len(vertices)][1]
        - vertices[index][1] * vertices[(index + 1) % len(vertices)][0]
        for index in range(len(vertices))
    ) / 2.0


def _edge_cross(start, end, point):
    return ((end[0] - start[0]) * (point[1] - start[1])
            - (end[1] - start[1]) * (point[0] - start[0]))


def _is_convex_polygon(vertices, tolerance=1e-9):
    for index, start in enumerate(vertices):
        middle = vertices[(index + 1) % len(vertices)]
        end = vertices[(index + 2) % len(vertices)]
        cross = _edge_cross(start, middle, end)
        first_length = math.hypot(middle[0] - start[0], middle[1] - start[1])
        second_length = math.hypot(end[0] - middle[0], end[1] - middle[1])
        if cross < -tolerance * max(1.0, first_length * second_length):
            return False
    return True


def _convex_intersection_area(subject, clip, tolerance=1e-9):
    output = [list(point) for point in subject]
    for edge_index, edge_start in enumerate(clip):
        if not output:
            break
        edge_end = clip[(edge_index + 1) % len(clip)]
        input_vertices = output
        output = []
        previous = input_vertices[-1]
        previous_distance = _edge_cross(edge_start, edge_end, previous)
        previous_inside = previous_distance >= -tolerance
        for current in input_vertices:
            current_distance = _edge_cross(edge_start, edge_end, current)
            current_inside = current_distance >= -tolerance
            if current_inside != previous_inside:
                denominator = previous_distance - current_distance
                if abs(denominator) > math.ulp(1.0):
                    fraction = previous_distance / denominator
                    output.append([
                        previous[coordinate]
                        + fraction * (current[coordinate] - previous[coordinate])
                        for coordinate in range(2)
                    ])
            if current_inside:
                output.append(list(current))
            previous = current
            previous_distance = current_distance
            previous_inside = current_inside
    return abs(_signed_polygon_area(output)) if len(output) >= 3 else 0.0


def _exact_domain_tolerances(domain_bounds):
    spans = [upper - lower for lower, upper in domain_bounds]
    minimum_span = min(spans)
    coordinate_scale = max(
        [abs(value) for bounds in domain_bounds for value in bounds]
        + [minimum_span]
    )
    length_tolerance = max(
        1e-10 * minimum_span,
        64.0 * math.ulp(max(coordinate_scale, minimum_span)),
    )
    domain_area = math.prod(spans)
    area_tolerance = (
        8.0 * length_tolerance * sum(spans)
        + 128.0 * math.ulp(1.0) * domain_area
    )
    return domain_area, length_tolerance, area_tolerance


def _segment_interval_on_edge(segment, edge, tolerance):
    start, end = edge
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length_squared = dx * dx + dy * dy
    edge_length = math.sqrt(length_squared)
    if edge_length <= tolerance:
        return None
    parameter_tolerance = max(128.0 * math.ulp(1.0), tolerance / edge_length)
    parameters = []
    for point in segment:
        cross = dx * (point[1] - start[1]) - dy * (point[0] - start[0])
        if abs(cross) / edge_length > tolerance:
            return None
        parameter = (
            (point[0] - start[0]) * dx + (point[1] - start[1]) * dy
        ) / length_squared
        if parameter < -parameter_tolerance or parameter > 1.0 + parameter_tolerance:
            return None
        parameters.append(min(1.0, max(0.0, parameter)))
    lower, upper = min(parameters), max(parameters)
    if (upper - lower) * edge_length <= tolerance:
        return None
    return lower, upper, parameter_tolerance


def _facet_closure_errors(cells, facets, domain_bounds, tolerance):
    errors = []
    polygons = {cell["cell_id"]: cell["vertices"] for cell in cells}
    coverage = {
        (cell_id, edge_index): []
        for cell_id, vertices in polygons.items()
        for edge_index in range(len(vertices))
    }
    for facet_index, facet in enumerate(facets):
        actual_incident = []
        memberships = []
        for cell_id, vertices in polygons.items():
            matches = []
            for edge_index, start in enumerate(vertices):
                interval = _segment_interval_on_edge(
                    facet["endpoints"],
                    (start, vertices[(edge_index + 1) % len(vertices)]),
                    tolerance,
                )
                if interval is not None:
                    matches.append((edge_index, interval))
            if len(matches) > 1:
                errors.append("a facet lies on multiple canonical edges of one cell")
            if matches:
                edge_index, interval = matches[0]
                actual_incident.append(cell_id)
                memberships.append((cell_id, edge_index, interval))
        if set(actual_incident) != set(facet["incident_cell_ids"]):
            errors.append("facet incidence must equal geometric cell-boundary incidence")

        actual_side = None
        for axis_index, (lower, upper) in enumerate(domain_bounds):
            for suffix, bound in (("lower", lower), ("upper", upper)):
                if all(
                    abs(point[axis_index] - bound) <= tolerance
                    for point in facet["endpoints"]
                ):
                    actual_side = f"axis{axis_index + 1}_{suffix}"
                    break
            if actual_side is not None:
                break
        if actual_side is None:
            if len(actual_incident) != 2 or facet["domain_side"] is not None:
                errors.append("internal facets require exactly two geometric incident cells")
        elif (
            len(actual_incident) != 1
            or facet["kind"] != "domain_boundary"
            or facet["domain_side"] != actual_side
        ):
            errors.append("domain facet kind, side, or incidence is inconsistent")

        for cell_id, edge_index, interval in memberships:
            coverage[(cell_id, edge_index)].append((*interval, facet_index))

    for intervals in coverage.values():
        if not intervals:
            errors.append("every canonical cell edge must be covered by facets")
            continue
        intervals.sort()
        cursor = 0.0
        for lower, upper, parameter_tolerance, _ in intervals:
            if lower > cursor + parameter_tolerance:
                errors.append("facet intervals leave an uncovered cell-edge gap")
            if lower < cursor - parameter_tolerance:
                errors.append("facet intervals overlap with positive length")
            cursor = max(cursor, upper)
        if cursor < 1.0 - max(item[2] for item in intervals):
            errors.append("facet intervals leave an uncovered terminal cell-edge gap")
    return errors


def _semantic_errors(document):
    """Check v1 relationships that JSON Schema cannot express generically."""
    errors = []
    domain = document["domain"]
    axis_order = domain["axis_order"]
    axis_ids = [axis["axis_id"] for axis in domain["axes"]]
    if axis_order != axis_ids:
        errors.append("domain.axis_order must equal the axes axis_id sequence")
    if any(axis["bounds"]["lower"] >= axis["bounds"]["upper"] for axis in domain["axes"]):
        errors.append("every input bound must satisfy lower < upper")
    axis_symbols = {axis["symbol"] for axis in domain["axes"]}
    background_symbols = [item["symbol"] for item in domain["fixed_background"]]
    if axis_symbols.intersection(background_symbols):
        errors.append("a swept axis cannot also be fixed background")
    if len(background_symbols) != len(set(background_symbols)):
        errors.append("fixed background symbols must be unique")

    outputs = document["outputs"]
    output_order = outputs["output_order"]
    output_ids = [item["output_id"] for item in outputs["items"]]
    if output_order != output_ids:
        errors.append("outputs.output_order must equal the item output_id sequence")
    expected_components = [
        {"output_id": output_id, "input_axis_id": axis_id}
        for output_id in output_order
        for axis_id in axis_order
    ]
    if document["component_order"] != expected_components:
        errors.append("component_order must be the output-major Cartesian product")

    data = document["data"]
    representation = document["representation"]
    input_count = len(axis_order)
    output_count = len(output_order)
    if representation == "sampled_grid":
        grid_shape = data["grid_shape"]
        if len(grid_shape) != input_count:
            errors.append("grid_shape rank must equal input count")
        if data["sampling_scheme"] == "cartesian_product":
            coordinate_shape = [len(values) for values in data["axis_coordinates"]]
            if coordinate_shape != grid_shape:
                errors.append("Cartesian axis-coordinate lengths must equal grid_shape")
        if data["output_shape"] != grid_shape + [output_count]:
            errors.append("output_shape must be grid_shape + output count")
        if data["reaction_order_shape"] != grid_shape + [output_count, input_count]:
            errors.append("reaction_order_shape must append output and input counts")
        grid_count = _product(grid_shape)
        if len(data["output_values"]) != grid_count * output_count:
            errors.append("output_values length does not match output_shape")
        if len(data["reaction_order_values"]) != grid_count * output_count * input_count:
            errors.append("reaction_order_values length does not match reaction_order_shape")
        if len(data["regime_ids"]) != grid_count or len(data["validity"]) != grid_count:
            errors.append("sample validity/regime lengths must equal grid sample count")
        for sample_index, valid in enumerate(data["validity"]):
            if valid:
                continue
            output_start = sample_index * output_count
            ro_start = sample_index * output_count * input_count
            if any(
                value is not None
                for value in data["output_values"][output_start : output_start + output_count]
            ):
                errors.append("invalid samples must use null output values")
            if any(
                value is not None
                for value in data["reaction_order_values"][
                    ro_start : ro_start + output_count * input_count
                ]
            ):
                errors.append("invalid samples must use null reaction-order values")
    elif representation == "exact_cell_complex":
        source_candidate_count = data["source_candidate_regime_count"]
        regular_candidate_count = data["regular_candidate_regime_count"]
        if regular_candidate_count > source_candidate_count:
            errors.append("regular candidate regimes cannot exceed source candidates")
        cell_ids = [cell["cell_id"] for cell in data["cells"]]
        facet_ids = [facet["facet_id"] for facet in data["facets"]]
        stratum_ids = [stratum["stratum_id"] for stratum in data["singular_strata"]]
        if data["cell_order"] != cell_ids:
            errors.append("cell_order must equal the cells cell_id sequence")
        if data["facet_order"] != facet_ids:
            errors.append("facet_order must equal the facets facet_id sequence")
        if data["singular_stratum_order"] != stratum_ids:
            errors.append(
                "singular_stratum_order must equal the singular_strata id sequence"
            )
        if input_count != 2:
            errors.append("exact_cell_complex v1 requires exactly two input axes")
        regime_cell_owners = {}
        cell_polygons = []
        cell_area_sum = 0.0
        domain_bounds = [
            (axis["bounds"]["lower"], axis["bounds"]["upper"])
            for axis in domain["axes"]
        ]
        domain_area, exact_length_tolerance, exact_area_tolerance = (
            _exact_domain_tolerances(domain_bounds)
        )
        for cell in data["cells"]:
            vertices = cell["vertices"]
            if any(len(vertex) != input_count for vertex in vertices):
                errors.append("cell vertex rank must equal input count")
            if vertices and vertices[0] != min(vertices):
                errors.append("cell vertices must start at the lexicographically smallest point")
            signed_area = _signed_polygon_area(vertices)
            if signed_area <= 0:
                errors.append("cell vertices must be counter-clockwise")
            if not _is_convex_polygon(vertices):
                errors.append("cell vertices must describe a convex polygon")
            if any(
                coordinate < domain_bounds[axis_index][0] - 1e-8
                or coordinate > domain_bounds[axis_index][1] + 1e-8
                for vertex in vertices
                for axis_index, coordinate in enumerate(vertex)
            ):
                errors.append("cell vertices must remain inside the declared domain")
            if abs(signed_area - cell["area"]) > exact_area_tolerance:
                errors.append("cell area must match its ordered vertices")
            cell_polygons.append(vertices)
            cell_area_sum += signed_area
            for source_id in cell["source_regime_ids"]:
                if source_id in regime_cell_owners:
                    errors.append("a source regime cannot own two distinct full cells")
                regime_cell_owners[source_id] = cell["cell_id"]
            for halfspace in cell.get("polyhedron", {}).get("halfspaces", []):
                if len(halfspace["coefficients"]) != input_count:
                    errors.append("cell halfspace rank must equal input count")
            label_ids = [label["label_id"] for label in cell["affine_labels"]]
            if cell["label_order"] != label_ids:
                errors.append("cell label_order must equal its affine-label sequence")
            cell_sources = set(cell["source_regime_ids"])
            for label in cell["affine_labels"]:
                matrix = label["reaction_order_matrix"]
                if len(matrix) != output_count or any(len(row) != input_count for row in matrix):
                    errors.append("cell reaction-order matrix has the wrong shape")
                if len(label["output_offset"]) != output_count:
                    errors.append("cell output offset has the wrong shape")
                if not set(label["source_regime_ids"]).issubset(cell_sources):
                    errors.append("affine-label sources must belong to the cell source union")
            label_sources = {
                source_id
                for label in cell["affine_labels"]
                for source_id in label["source_regime_ids"]
            }
            if label_sources != cell_sources:
                errors.append("affine labels must preserve the complete cell source union")
            if sum(len(label["source_regime_ids"]) for label in cell["affine_labels"]) != len(
                label_sources
            ):
                errors.append("one source regime cannot belong to two affine labels")
            affine_value_keys = {
                (
                    tuple(tuple(row) for row in label["reaction_order_matrix"]),
                    tuple(label["output_offset"]),
                )
                for label in cell["affine_labels"]
            }
            if len(affine_value_keys) != len(cell["affine_labels"]):
                errors.append("equal affine values must be merged into one label")
            if cell["status"] == "regular" and len(cell["affine_labels"]) != 1:
                errors.append("a regular cell must have exactly one affine label")
            if cell["status"] == "set_valued" and len(cell["affine_labels"]) < 2:
                errors.append("a set-valued cell must preserve every distinct affine label")
            if cell["set_valued"] != (len(cell["affine_labels"]) > 1):
                errors.append("set_valued must agree with the number of distinct labels")
            if (cell["status"] == "set_valued") != cell["set_valued"]:
                errors.append("cell status and set_valued must agree")
        if regular_candidate_count != len(regime_cell_owners):
            errors.append(
                "regular_candidate_regime_count must equal the full-cell source population"
            )
        for left_index, left in enumerate(cell_polygons):
            for right in cell_polygons[left_index + 1 :]:
                if _convex_intersection_area(left, right) > exact_area_tolerance:
                    errors.append("exact cells cannot overlap with positive area")
        if not data["gaps"] and abs(cell_area_sum - domain_area) > exact_area_tolerance:
            errors.append("gap-free exact cells must cover the complete declared domain")
        known_cells = set(cell_ids)
        known_strata = set(stratum_ids)
        for facet in data["facets"]:
            if not set(facet["incident_cell_ids"]).issubset(known_cells):
                errors.append("facet incidence references an unknown cell")
            if not set(facet["singular_stratum_ids"]).issubset(known_strata):
                errors.append("facet references an unknown singular stratum")
            endpoints = facet["endpoints"]
            if endpoints != sorted(endpoints):
                errors.append("facet endpoints must be in lexicographic order")
            if any(len(endpoint) != input_count for endpoint in endpoints):
                errors.append("facet endpoint rank must equal input count")
            if endpoints[0] == endpoints[1]:
                errors.append("facet endpoints must define a positive-length segment")
            normal = facet["normal"]
            if len(normal) != input_count:
                errors.append("facet normal rank must equal input count")
            elif not math.isclose(math.hypot(*normal), 1.0, rel_tol=1e-9, abs_tol=1e-9):
                errors.append("facet normal must be unit length")
            elif any(
                not math.isclose(
                    sum(normal[index] * endpoint[index] for index in range(input_count))
                    + facet["offset"],
                    0.0,
                    abs_tol=1e-8,
                )
                for endpoint in endpoints
            ):
                errors.append("facet normal and offset must contain both endpoints")
            expected_mixed = len(normal) == 2 and normal[0] * normal[1] < 0
            if facet["mixed_sign"] != expected_mixed:
                errors.append("facet mixed_sign must agree with its canonical normal")
            if len(normal) == 2 and (
                normal[0] < -1e-9
                or (abs(normal[0]) <= 1e-9 and normal[1] < -1e-9)
            ):
                errors.append("facet normal must use the canonical orientation")
            side = facet["domain_side"]
            if side is not None:
                axis_index = 0 if side.startswith("axis1") else 1
                bound_key = "lower" if side.endswith("lower") else "upper"
                expected = domain["axes"][axis_index]["bounds"][bound_key]
                if any(
                    not math.isclose(endpoint[axis_index], expected, abs_tol=1e-9)
                    for endpoint in endpoints
                ):
                    errors.append("domain facet endpoints must lie on the declared domain side")
            for halfspace in facet.get("polyhedron", {}).get("halfspaces", []):
                if len(halfspace["coefficients"]) != input_count:
                    errors.append("facet halfspace rank must equal input count")
        errors.extend(_facet_closure_errors(
            data["cells"], data["facets"], domain_bounds, exact_length_tolerance
        ))
        for stratum in data["singular_strata"]:
            vertices = stratum["vertices"]
            if any(len(vertex) != input_count for vertex in vertices):
                errors.append("singular-stratum vertex rank must equal input count")
            if vertices != sorted(vertices):
                errors.append("singular-stratum vertices must be lexicographically ordered")
            if stratum["dimension"] == 0 and len(vertices) != 1:
                errors.append("zero-dimensional strata must contain one vertex")
            if stratum["dimension"] == 1 and len(vertices) != 2:
                errors.append("one-dimensional strata must contain two endpoints")
        represented_source_ids = set(regime_cell_owners)
        represented_source_ids.update(
            source_id
            for stratum in data["singular_strata"]
            for source_id in stratum["source_regime_ids"]
        )
        if len(represented_source_ids) > source_candidate_count:
            errors.append("represented source regimes exceed the inspected candidate population")
        has_non_single_valued_evidence = (
            bool(data["singular_strata"])
            or bool(data["gaps"])
            or any(cell["set_valued"] for cell in data["cells"])
        )
        if has_non_single_valued_evidence:
            if not document["partial"]:
                errors.append("singular, gapped, or set-valued exact artifacts must be partial")
            if document["evidence"]["completeness_claim"] != "no_positive_claim":
                errors.append("singular, gapped, or set-valued exact artifacts cannot make a positive claim")
            if document["coverage"]["invalid_count"] < 1:
                errors.append("singular, gapped, or set-valued exact artifacts need invalid coverage")

        expected_valid = sum(not cell["set_valued"] for cell in data["cells"])
        expected_invalid = (
            sum(cell["set_valued"] for cell in data["cells"])
            + len(data["singular_strata"])
            + len(data["gaps"])
        )
        coverage = document["coverage"]
        if coverage["population_kind"] != "cell_complex_items":
            errors.append("exact coverage must name the cell_complex_items population")
        if coverage["valid_count"] != expected_valid:
            errors.append("exact valid_count must count regular serialized cells")
        if coverage["invalid_count"] != expected_invalid:
            errors.append("exact invalid_count must count set-valued cells, strata, and gaps")
        if coverage["evaluated_count"] != expected_valid + expected_invalid:
            errors.append("exact evaluated_count must count serialized cell-complex items")
    elif representation == "directional_path":
        sample_count = len(data["path_parameter_values"])
        if data["path_shape"] != [sample_count, input_count]:
            errors.append("path_shape must be [sample count, input count]")
        if data["output_shape"] != [sample_count, output_count]:
            errors.append("directional output_shape is inconsistent")
        if data["reaction_order_shape"] != [sample_count, output_count, input_count]:
            errors.append("directional reaction_order_shape is inconsistent")
        if data["directional_order_shape"] != [sample_count, output_count]:
            errors.append("directional_order_shape is inconsistent")
        expected_lengths = {
            "input_log_coordinates": sample_count * input_count,
            "output_values": sample_count * output_count,
            "reaction_order_values": sample_count * output_count * input_count,
            "directional_reaction_order_values": sample_count * output_count,
            "regime_ids": sample_count,
            "validity": sample_count,
        }
        for key, expected_length in expected_lengths.items():
            if len(data[key]) != expected_length:
                errors.append(f"{key} length does not match its declared shape")
        if any(len(vertex) != input_count for vertex in data["path"]["vertices"]):
            errors.append("path vertices must use the input-domain rank")

    coverage = document["coverage"]
    if coverage["evaluated_count"] != coverage["valid_count"] + coverage["invalid_count"]:
        errors.append("evaluated_count must equal valid_count + invalid_count")
    if coverage["eligible_count"] != coverage["evaluated_count"] + coverage["omitted_count"]:
        errors.append("eligible_count must equal evaluated_count + omitted_count")
    if coverage["storage"]["stored_count"] > coverage["evaluated_count"]:
        errors.append("stored_count cannot exceed evaluated_count")
    should_be_partial = (
        coverage["invalid_count"] > 0
        or coverage["omitted_count"] > 0
        or coverage["truncated"]
        or not coverage["enumeration_complete"]
        or not coverage["storage"]["complete"]
    )
    if document["partial"] != should_be_partial:
        errors.append("partial must reflect invalid, omitted, truncated, or incomplete evidence")
    return errors


class ReactionOrderFieldSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads((ROOT / "schemas" / "ro-field.schema.json").read_text())
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema, format_checker=FormatChecker())
        cls.fixtures = {
            path.stem: json.loads(path.read_text())
            for path in sorted(FIXTURE_ROOT.glob("*.json"))
        }

    def assert_contract_valid(self, document):
        errors = sorted(self.validator.iter_errors(document), key=lambda error: list(error.path))
        self.assertEqual([], [error.message for error in errors])
        self.assertEqual([], _semantic_errors(document))

    def test_all_three_representation_examples_are_valid(self):
        self.assertEqual(
            {"sampled-grid", "exact-cell-complex", "directional-path"},
            set(self.fixtures),
        )
        for name, document in self.fixtures.items():
            with self.subTest(name=name):
                self.assert_contract_valid(document)

    def test_representation_discriminator_rejects_mismatched_payload(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["representation"] = "exact_cell_complex"
        document["coverage"]["population_kind"] = "cell_complex_items"
        document["evidence"]["evidence_class"] = "exact_polyhedral"
        self.assertTrue(list(self.validator.iter_errors(document)))

    def test_unknown_fields_and_future_versions_fail_closed(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["unversioned_extension"] = True
        self.assertTrue(list(self.validator.iter_errors(document)))
        del document["unversioned_extension"]
        document["schema_version"] = "bne-ro-field/v2.0.0"
        self.assertTrue(list(self.validator.iter_errors(document)))

    def test_complete_artifact_cannot_hide_invalid_samples(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["coverage"]["valid_count"] = 3
        document["coverage"]["invalid_count"] = 1
        self.assertTrue(list(self.validator.iter_errors(document)))
        self.assertTrue(_semantic_errors(document))

    def test_truncation_is_explicit_and_cannot_claim_complete(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["partial"] = True
        document["coverage"].update(
            {
                "eligible_count": 5,
                "omitted_count": 1,
                "enumeration_complete": False,
                "truncated": True,
                "truncation": {
                    "reason": "work_budget",
                    "detail": "The fifth requested sample exceeded the fixture budget."
                },
            }
        )
        document["evidence"].update(
            {
                "status": "partial",
                "completeness_claim": "best_over_evaluated_prefix",
            }
        )
        self.assert_contract_valid(document)

        invalid = copy.deepcopy(document)
        invalid["coverage"]["truncation"] = None
        self.assertTrue(list(self.validator.iter_errors(invalid)))

    def test_semantic_links_reject_shape_and_axis_drift(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["domain"]["axis_order"].reverse()
        document["data"]["reaction_order_shape"] = [2, 2, 2, 1]
        errors = _semantic_errors(document)
        self.assertTrue(any("axis_order" in error for error in errors))
        self.assertTrue(any("reaction_order_shape" in error for error in errors))

    def test_invalid_sample_is_a_null_gap_not_a_numeric_zero(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["data"]["validity"][0] = False
        document["coverage"].update({"valid_count": 3, "invalid_count": 1})
        document["partial"] = True
        document["evidence"].update(
            {"status": "partial", "completeness_claim": "no_positive_claim"}
        )
        errors = _semantic_errors(document)
        self.assertTrue(any("null output" in error for error in errors))
        self.assertTrue(any("null reaction-order" in error for error in errors))

        document["data"]["output_values"][0] = None
        document["data"]["reaction_order_values"][0:2] = [None, None]
        self.assert_contract_valid(document)

    def test_unknown_source_revision_is_explicit_and_cannot_be_faked(self):
        document = copy.deepcopy(self.fixtures["sampled-grid"])
        document["provenance"].update(
            {
                "source_revision_status": "unknown",
                "source_commit": None,
                "source_dirty": None,
            }
        )
        self.assert_contract_valid(document)

        invalid = copy.deepcopy(document)
        invalid["provenance"]["source_commit"] = "0" * 40
        self.assertTrue(list(self.validator.iter_errors(invalid)))

        invalid = copy.deepcopy(self.fixtures["sampled-grid"])
        invalid["provenance"]["source_revision_status"] = "known"
        invalid["provenance"]["source_dirty"] = None
        self.assertTrue(list(self.validator.iter_errors(invalid)))

    def test_coincident_cell_preserves_multiple_affine_labels(self):
        document = copy.deepcopy(self.fixtures["exact-cell-complex"])
        cell = document["data"]["cells"][0]
        cell["status"] = "set_valued"
        cell["set_valued"] = True
        cell["source_regime_ids"].append("regime-5")
        cell["label_order"].append("cell_all-label-2")
        second = copy.deepcopy(cell["affine_labels"][0])
        second.update(
            {
                "label_id": "cell_all-label-2",
                "source_regime_ids": ["regime-5"],
                "reaction_order_matrix": [[0.0, 1.0]],
            }
        )
        cell["affine_labels"].append(second)
        document["data"].update(
            {
                "source_candidate_regime_count": 5,
                "regular_candidate_regime_count": 4,
            }
        )
        document["coverage"].update({"valid_count": 2, "invalid_count": 2})
        self.assert_contract_valid(document)

        invalid = copy.deepcopy(document)
        invalid["data"]["cells"][0]["affine_labels"] = [second]
        self.assertTrue(list(self.validator.iter_errors(invalid)))

        fake_complete = copy.deepcopy(document)
        fake_complete["partial"] = False
        fake_complete["coverage"].update({"valid_count": 4, "invalid_count": 0})
        fake_complete["evidence"].update(
            {
                "status": "complete",
                "completeness_claim": "complete_over_declared_population",
            }
        )
        self.assertTrue(list(self.validator.iter_errors(fake_complete)))
        self.assertTrue(_semantic_errors(fake_complete))

    def test_exact_geometry_order_dimension_and_incidence_are_semantic(self):
        document = copy.deepcopy(self.fixtures["exact-cell-complex"])
        document["data"]["cells"][0]["vertices"].reverse()
        document["data"]["facets"][0]["incident_cell_ids"] = ["unknown-cell"]
        document["data"]["singular_stratum_order"] = ["unknown-stratum"]
        errors = _semantic_errors(document)
        self.assertTrue(any("counter-clockwise" in error for error in errors))
        self.assertTrue(any("unknown cell" in error for error in errors))
        self.assertTrue(any("singular_stratum_order" in error for error in errors))

    def test_exact_geometry_cannot_fabricate_domain_coverage(self):
        document = copy.deepcopy(self.fixtures["exact-cell-complex"])
        document["data"]["cells"] = document["data"]["cells"][:-1]
        errors = _semantic_errors(document)
        self.assertTrue(any("complete declared domain" in error for error in errors))

        outside = copy.deepcopy(self.fixtures["exact-cell-complex"])
        outside["data"]["cells"][0]["vertices"][0][0] = 99.0
        self.assertTrue(any(
            "inside the declared domain" in error
            for error in _semantic_errors(outside)
        ))

        overlap = copy.deepcopy(self.fixtures["exact-cell-complex"])
        duplicate = copy.deepcopy(overlap["data"]["cells"][0])
        duplicate["cell_id"] = "overlapping-cell"
        duplicate["source_regime_ids"] = ["regime-overlap"]
        duplicate["affine_labels"][0]["label_id"] = "overlapping-label"
        duplicate["affine_labels"][0]["source_regime_ids"] = ["regime-overlap"]
        duplicate["label_order"] = ["overlapping-label"]
        overlap["data"]["cells"].append(duplicate)
        overlap["data"]["cell_order"].append("overlapping-cell")
        overlap["data"]["source_candidate_regime_count"] += 1
        overlap["data"]["regular_candidate_regime_count"] += 1
        self.assertTrue(any(
            "overlap with positive area" in error
            for error in _semantic_errors(overlap)
        ))

        no_facets = copy.deepcopy(self.fixtures["exact-cell-complex"])
        no_facets["data"]["facets"] = []
        no_facets["data"]["facet_order"] = []
        self.assertTrue(any(
            "covered by facets" in error
            for error in _semantic_errors(no_facets)
        ))

        missing_facet = copy.deepcopy(self.fixtures["exact-cell-complex"])
        missing_facet["data"]["facets"].pop()
        missing_facet["data"]["facet_order"].pop()
        self.assertTrue(any(
            "facet" in error
            for error in _semantic_errors(missing_facet)
        ))


if __name__ == "__main__":
    unittest.main()
