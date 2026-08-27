import copy
import json
import math
import sys
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "ro_field"
FLOAT64_EPSILON = math.ulp(1.0)


def _product(values):
    return math.prod(values)


def _is_finite_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _signed_polygon_area(vertices):
    if len(vertices) < 3:
        return 0.0
    origin_x, origin_y = vertices[0]
    return sum(
        (vertices[index][0] - origin_x)
        * (vertices[(index + 1) % len(vertices)][1] - origin_y)
        - (vertices[(index + 1) % len(vertices)][0] - origin_x)
        * (vertices[index][1] - origin_y)
        for index in range(len(vertices))
    ) / 2.0


def _edge_cross(start, end, point):
    return ((end[0] - start[0]) * (point[1] - start[1])
            - (end[1] - start[1]) * (point[0] - start[0]))


def _is_convex_polygon(vertices, length_tolerance=1e-9):
    for index, start in enumerate(vertices):
        middle = vertices[(index + 1) % len(vertices)]
        end = vertices[(index + 2) % len(vertices)]
        cross = _edge_cross(start, middle, end)
        first_length = math.hypot(middle[0] - start[0], middle[1] - start[1])
        second_length = math.hypot(end[0] - middle[0], end[1] - middle[1])
        cross_tolerance = (
            max(first_length, second_length) * length_tolerance
            + 64.0 * FLOAT64_EPSILON * first_length * second_length
        )
        if cross < -cross_tolerance:
            return False
    return True


def _edge_cross_tolerance(start, end, point, length_tolerance):
    edge_length = math.hypot(end[0] - start[0], end[1] - start[1])
    point_distance = math.hypot(point[0] - start[0], point[1] - start[1])
    return min(
        edge_length * length_tolerance,
        64.0 * FLOAT64_EPSILON * edge_length * point_distance,
    )


def _convex_intersection_area(subject, clip, length_tolerance=1e-9):
    output = [list(point) for point in subject]
    for edge_index, edge_start in enumerate(clip):
        if not output:
            break
        edge_end = clip[(edge_index + 1) % len(clip)]
        input_vertices = output
        output = []
        previous = input_vertices[-1]
        previous_distance = _edge_cross(edge_start, edge_end, previous)
        previous_inside = previous_distance >= -_edge_cross_tolerance(
            edge_start, edge_end, previous, length_tolerance
        )
        for current in input_vertices:
            current_distance = _edge_cross(edge_start, edge_end, current)
            current_inside = current_distance >= -_edge_cross_tolerance(
                edge_start, edge_end, current, length_tolerance
            )
            if current_inside != previous_inside:
                denominator = previous_distance - current_distance
                denominator_tolerance = 64.0 * FLOAT64_EPSILON * max(
                    abs(previous_distance),
                    abs(current_distance),
                    sys.float_info.min,
                )
                if abs(denominator) > denominator_tolerance:
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


def _axis_length_tolerance(bounds):
    span = bounds["upper"] - bounds["lower"]
    coordinate_scale = max(abs(bounds["lower"]), abs(bounds["upper"]), span)
    return max(
        1e-10 * span,
        64.0 * FLOAT64_EPSILON * coordinate_scale,
    )


def _scaled_values_close(actual, expected, base_tolerance=0.0, *scale_values):
    if not _is_finite_number(actual) or not _is_finite_number(expected):
        return False
    scale = max(
        [abs(actual), abs(expected), sys.float_info.min]
        + [abs(value) for value in scale_values if _is_finite_number(value)]
    )
    tolerance = max(
        base_tolerance,
        64.0 * FLOAT64_EPSILON * scale,
    )
    return abs(actual - expected) <= tolerance


def _exact_domain_tolerances(domain_bounds):
    spans = [upper - lower for lower, upper in domain_bounds]
    minimum_span = min(spans)
    coordinate_scale = max(
        [abs(value) for bounds in domain_bounds for value in bounds]
        + [minimum_span]
    )
    length_tolerance = max(
        1e-10 * minimum_span,
        64.0 * FLOAT64_EPSILON * max(coordinate_scale, minimum_span),
    )
    domain_area = math.prod(spans)
    area_tolerance = (
        8.0 * length_tolerance * sum(spans)
        + 128.0 * FLOAT64_EPSILON * domain_area
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


def _segments_have_positive_overlap(left, right, tolerance):
    if (
        len(left) != 2
        or len(right) != 2
        or any(len(point) != 2 for point in left + right)
        or any(
            not _is_finite_number(coordinate)
            for point in left + right
            for coordinate in point
        )
    ):
        return False
    start, end = left
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length_squared = dx * dx + dy * dy
    segment_length = math.sqrt(length_squared)
    if segment_length <= tolerance:
        return False
    for point in right:
        cross = dx * (point[1] - start[1]) - dy * (point[0] - start[0])
        if abs(cross) / segment_length > tolerance:
            return False
    parameters = [
        (
            (point[0] - start[0]) * dx
            + (point[1] - start[1]) * dy
        )
        / length_squared
        for point in right
    ]
    lower = max(0.0, min(parameters))
    upper = min(1.0, max(parameters))
    return (upper - lower) * segment_length > tolerance


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
            expected_kind = (
                "singular_boundary"
                if facet["singular_stratum_ids"]
                else "regime_transition"
            )
            if facet["kind"] != expected_kind:
                errors.append(
                    "internal facet kind must agree with geometric "
                    "singular-stratum incidence"
                )
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
    actual_evaluated = None
    actual_valid = None
    actual_invalid = None
    if representation == "sampled_grid":
        grid_shape = data["grid_shape"]
        if len(grid_shape) != input_count:
            errors.append("grid_shape rank must equal input count")
        if len(data["axis_coordinates"]) != input_count:
            errors.append("axis-coordinate rank must equal input count")
        for axis_index, coordinates in enumerate(data["axis_coordinates"]):
            if axis_index >= input_count:
                break
            bounds = domain["axes"][axis_index]["bounds"]
            if any(
                not _is_finite_number(value)
                or value < bounds["lower"]
                or value > bounds["upper"]
                for value in coordinates
            ):
                errors.append("sample coordinates must be finite and inside the domain")
        if data["sampling_scheme"] == "cartesian_product":
            coordinate_shape = [len(values) for values in data["axis_coordinates"]]
            if coordinate_shape != grid_shape:
                errors.append("Cartesian axis-coordinate lengths must equal grid_shape")
            if any(
                not all(
                    _is_finite_number(value) for value in coordinates
                )
                or any(
                    left >= right
                    for left, right in zip(coordinates, coordinates[1:])
                )
                for coordinates in data["axis_coordinates"]
            ):
                errors.append("Cartesian axis coordinates must be strictly increasing")
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
            output_start = sample_index * output_count
            ro_start = sample_index * output_count * input_count
            output_block = data["output_values"][
                output_start : output_start + output_count
            ]
            ro_block = data["reaction_order_values"][
                ro_start : ro_start + output_count * input_count
            ]
            regime_id = (
                data["regime_ids"][sample_index]
                if sample_index < len(data["regime_ids"])
                else None
            )
            if valid:
                if not all(_is_finite_number(value) for value in output_block):
                    errors.append("valid samples require finite output values")
                if not all(_is_finite_number(value) for value in ro_block):
                    errors.append("valid samples require finite reaction-order values")
                if not isinstance(regime_id, str) or not regime_id.strip():
                    errors.append("valid samples require a non-blank regime identity")
            else:
                if any(value is not None for value in output_block):
                    errors.append("invalid samples must use null output values")
                if any(value is not None for value in ro_block):
                    errors.append("invalid samples must use null reaction-order values")
                if regime_id is not None:
                    errors.append("invalid samples must use a null regime identity")
        actual_evaluated = grid_count
        actual_valid = sum(value is True for value in data["validity"])
        actual_invalid = grid_count - actual_valid
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
            if not _is_convex_polygon(
                vertices, length_tolerance=exact_length_tolerance
            ):
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
                if _convex_intersection_area(
                    left, right, length_tolerance=exact_length_tolerance
                ) > exact_area_tolerance:
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
            expected_singular_ids = sorted(
                stratum["stratum_id"]
                for stratum in data["singular_strata"]
                if stratum["dimension"] == 1
                and _segments_have_positive_overlap(
                    endpoints,
                    stratum["vertices"],
                    exact_length_tolerance,
                )
            )
            if sorted(facet["singular_stratum_ids"]) != expected_singular_ids:
                errors.append(
                    "facet singular_stratum_ids must equal the positively "
                    "overlapping one-dimensional strata"
                )
            if (
                facet["kind"] == "singular_boundary"
                and not facet["singular_stratum_ids"]
            ):
                errors.append(
                    "singular-boundary facets must reference a singular stratum"
                )
            if (
                facet["kind"] == "regime_transition"
                and facet["singular_stratum_ids"]
            ):
                errors.append(
                    "regime-transition facets cannot reference singular strata"
                )
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
        actual_evaluated = expected_valid + expected_invalid
        actual_valid = expected_valid
        actual_invalid = expected_invalid
    elif representation == "directional_path":
        sample_count = len(data["path_parameter_values"])
        axis_length_tolerances = [
            _axis_length_tolerance(axis["bounds"])
            for axis in domain["axes"]
        ]
        if data["path_shape"] != [sample_count, input_count]:
            errors.append("path_shape must be [sample count, input count]")
        if data["output_shape"] != [sample_count, output_count]:
            errors.append("directional output_shape is inconsistent")
        if data["reaction_order_shape"] != [sample_count, output_count, input_count]:
            errors.append("directional reaction_order_shape is inconsistent")
        if data["directional_order_shape"] != [sample_count, output_count]:
            errors.append("directional_order_shape is inconsistent")
        if data["path_tangent_shape"] != [sample_count, input_count]:
            errors.append("path_tangent_shape must be [sample count, input count]")
        expected_lengths = {
            "input_log_coordinates": sample_count * input_count,
            "path_tangent_values": sample_count * input_count,
            "output_values": sample_count * output_count,
            "reaction_order_values": sample_count * output_count * input_count,
            "directional_reaction_order_values": sample_count * output_count,
            "regime_ids": sample_count,
            "validity": sample_count,
        }
        for key, expected_length in expected_lengths.items():
            if len(data[key]) != expected_length:
                errors.append(f"{key} length does not match its declared shape")
        for sample_index in range(sample_count):
            coordinate_start = sample_index * input_count
            coordinate_block = data["input_log_coordinates"][
                coordinate_start : coordinate_start + input_count
            ]
            if len(coordinate_block) != input_count:
                continue
            if any(
                not _is_finite_number(coordinate)
                or coordinate
                < domain["axes"][axis_index]["bounds"]["lower"]
                - axis_length_tolerances[axis_index]
                or coordinate
                > domain["axes"][axis_index]["bounds"]["upper"]
                + axis_length_tolerances[axis_index]
                for axis_index, coordinate in enumerate(coordinate_block)
            ):
                errors.append(
                    "path input coordinates must be finite and inside the domain"
                )
        path = data["path"]
        vertices = path["vertices"]
        vertex_parameters = path["vertex_parameter_values"]
        if any(len(vertex) != input_count for vertex in vertices):
            errors.append("path vertices must use the input-domain rank")
        if len(vertex_parameters) != len(vertices):
            errors.append("path vertex parameters must match the vertex population")
        elif any(
            not _is_finite_number(value) for value in vertex_parameters
        ) or any(
            left >= right
            for left, right in zip(vertex_parameters, vertex_parameters[1:])
        ):
            errors.append("path vertex parameters must be finite and strictly increasing")

        sample_parameters = data["path_parameter_values"]
        if any(not _is_finite_number(value) for value in sample_parameters) or any(
            left >= right
            for left, right in zip(sample_parameters, sample_parameters[1:])
        ):
            errors.append("path sample parameters must be finite and strictly increasing")

        path_geometry_usable = (
            len(vertices) >= 2
            and len(vertex_parameters) == len(vertices)
            and all(len(vertex) == input_count for vertex in vertices)
            and all(_is_finite_number(value) for value in vertex_parameters)
            and all(
                left < right
                for left, right in zip(vertex_parameters, vertex_parameters[1:])
            )
        )
        if path_geometry_usable:
            for vertex in vertices:
                if any(
                    not _is_finite_number(value)
                    or value < domain["axes"][axis_index]["bounds"]["lower"]
                    or value > domain["axes"][axis_index]["bounds"]["upper"]
                    for axis_index, value in enumerate(vertex)
                ):
                    errors.append("path vertices must be finite and inside the domain")
            for sample_index, parameter in enumerate(sample_parameters):
                if not _is_finite_number(parameter):
                    continue
                if parameter < vertex_parameters[0] or parameter > vertex_parameters[-1]:
                    errors.append("path samples must lie inside the declared parameter interval")
                    continue
                for vertex_index in range(1, len(vertex_parameters) - 1):
                    if parameter != vertex_parameters[vertex_index]:
                        continue
                    left_duration = (
                        vertex_parameters[vertex_index]
                        - vertex_parameters[vertex_index - 1]
                    )
                    right_duration = (
                        vertex_parameters[vertex_index + 1]
                        - vertex_parameters[vertex_index]
                    )
                    left_tangent = [
                        (
                            vertices[vertex_index][axis_index]
                            - vertices[vertex_index - 1][axis_index]
                        )
                        / left_duration
                        for axis_index in range(input_count)
                    ]
                    right_tangent = [
                        (
                            vertices[vertex_index + 1][axis_index]
                            - vertices[vertex_index][axis_index]
                        )
                        / right_duration
                        for axis_index in range(input_count)
                    ]
                    if left_tangent != right_tangent:
                        errors.append(
                            "directional samples cannot lie on a non-differentiable path kink"
                        )
                    break
                segment_index = len(vertex_parameters) - 2
                for candidate in range(len(vertex_parameters) - 1):
                    if parameter < vertex_parameters[candidate + 1] or (
                        candidate == len(vertex_parameters) - 2
                        and parameter <= vertex_parameters[candidate + 1]
                    ):
                        segment_index = candidate
                        break
                start_parameter = vertex_parameters[segment_index]
                end_parameter = vertex_parameters[segment_index + 1]
                fraction = (parameter - start_parameter) / (
                    end_parameter - start_parameter
                )
                expected_coordinates = [
                    vertices[segment_index][axis_index]
                    + fraction
                    * (
                        vertices[segment_index + 1][axis_index]
                        - vertices[segment_index][axis_index]
                    )
                    for axis_index in range(input_count)
                ]
                expected_tangent = [
                    (
                        vertices[segment_index + 1][axis_index]
                        - vertices[segment_index][axis_index]
                    )
                    / (end_parameter - start_parameter)
                    for axis_index in range(input_count)
                ]
                coordinate_start = sample_index * input_count
                coordinate_block = data["input_log_coordinates"][
                    coordinate_start : coordinate_start + input_count
                ]
                tangent_block = data["path_tangent_values"][
                    coordinate_start : coordinate_start + input_count
                ]
                if len(coordinate_block) == input_count and not all(
                    _scaled_values_close(
                        actual,
                        expected,
                        axis_length_tolerances[axis_index],
                    )
                    for axis_index, (actual, expected) in enumerate(
                        zip(coordinate_block, expected_coordinates)
                    )
                ):
                    errors.append(
                        "input coordinates must equal the machine-readable path at each sample"
                    )
                if len(tangent_block) == input_count and not all(
                    _scaled_values_close(
                        actual,
                        expected,
                        axis_length_tolerances[axis_index]
                        / abs(end_parameter - start_parameter),
                    )
                    for axis_index, (actual, expected) in enumerate(
                        zip(tangent_block, expected_tangent)
                    )
                ):
                    errors.append(
                        "path tangents must equal the derivative of the machine-readable path"
                    )

        for sample_index, valid in enumerate(data["validity"]):
            output_start = sample_index * output_count
            ro_start = sample_index * output_count * input_count
            tangent_start = sample_index * input_count
            output_block = data["output_values"][
                output_start : output_start + output_count
            ]
            ro_block = data["reaction_order_values"][
                ro_start : ro_start + output_count * input_count
            ]
            directional_block = data["directional_reaction_order_values"][
                output_start : output_start + output_count
            ]
            tangent_block = data["path_tangent_values"][
                tangent_start : tangent_start + input_count
            ]
            regime_id = (
                data["regime_ids"][sample_index]
                if sample_index < len(data["regime_ids"])
                else None
            )
            if valid:
                if not all(_is_finite_number(value) for value in output_block):
                    errors.append("valid path samples require finite output values")
                if not all(_is_finite_number(value) for value in ro_block):
                    errors.append(
                        "valid path samples require finite reaction-order values"
                    )
                if not all(_is_finite_number(value) for value in directional_block):
                    errors.append(
                        "valid path samples require finite directional-order values"
                    )
                if not isinstance(regime_id, str) or not regime_id.strip():
                    errors.append(
                        "valid path samples require a non-blank regime identity"
                    )
                if (
                    len(ro_block) == output_count * input_count
                    and len(tangent_block) == input_count
                    and len(directional_block) == output_count
                    and all(_is_finite_number(value) for value in ro_block)
                    and all(_is_finite_number(value) for value in tangent_block)
                    and all(_is_finite_number(value) for value in directional_block)
                ):
                    for output_index in range(output_count):
                        expected_directional = sum(
                            ro_block[output_index * input_count + axis_index]
                            * tangent_block[axis_index]
                            for axis_index in range(input_count)
                        )
                        product_terms = [
                            ro_block[output_index * input_count + axis_index]
                            * tangent_block[axis_index]
                            for axis_index in range(input_count)
                        ]
                        if not _scaled_values_close(
                            directional_block[output_index],
                            expected_directional,
                            0.0,
                            *product_terms,
                        ):
                            errors.append(
                                "directional reaction order must equal R times the path tangent"
                            )
            else:
                if any(value is not None for value in output_block):
                    errors.append("invalid path samples must use null output values")
                if any(value is not None for value in ro_block):
                    errors.append(
                        "invalid path samples must use null reaction-order values"
                    )
                if any(value is not None for value in directional_block):
                    errors.append(
                        "invalid path samples must use null directional-order values"
                    )
                if regime_id is not None:
                    errors.append("invalid path samples must use a null regime identity")
        actual_evaluated = sample_count
        actual_valid = sum(value is True for value in data["validity"])
        actual_invalid = sample_count - actual_valid

    coverage = document["coverage"]
    if coverage["evaluated_count"] != coverage["valid_count"] + coverage["invalid_count"]:
        errors.append("evaluated_count must equal valid_count + invalid_count")
    if coverage["eligible_count"] != coverage["evaluated_count"] + coverage["omitted_count"]:
        errors.append("eligible_count must equal evaluated_count + omitted_count")
    if actual_evaluated is not None and (
        coverage["evaluated_count"],
        coverage["valid_count"],
        coverage["invalid_count"],
    ) != (actual_evaluated, actual_valid, actual_invalid):
        errors.append(
            "coverage evaluated/valid/invalid counts must match serialized evidence"
        )
    if coverage["storage"]["stored_count"] > coverage["evaluated_count"]:
        errors.append("stored_count cannot exceed evaluated_count")
    budget = coverage["budget"]
    expected_work_unit_kind = {
        "sampled_grid": "solver_samples",
        "exact_cell_complex": "source_regime_candidates",
        "directional_path": "path_samples",
    }[representation]
    if budget["work_unit_kind"] != expected_work_unit_kind:
        errors.append("coverage budget work-unit kind is inconsistent")
    deadline = budget["deadline_seconds"]
    if deadline is not None and (
        not _is_finite_number(deadline) or deadline <= 0
    ):
        errors.append("deadline_seconds must be finite and positive or null")
    budget_work_count = (
        data["source_candidate_regime_count"]
        if representation == "exact_cell_complex"
        else coverage["evaluated_count"]
    )
    if budget_work_count > budget["max_evaluated_items"]:
        errors.append("evaluated evidence exceeds its work budget")

    storage = coverage["storage"]
    if storage["stored_count"] > budget["max_stored_items"]:
        errors.append("stored evidence exceeds its storage-item budget")
    if storage["payload_bytes"] > budget["max_payload_bytes"]:
        errors.append("stored evidence exceeds its payload-byte budget")
    if storage["complete"] and storage["stored_count"] != coverage["evaluated_count"]:
        errors.append("complete storage must contain every evaluated item")
    if storage["mode"] == "inline":
        if storage["stored_count"] != coverage["evaluated_count"]:
            errors.append("inline data must serialize every evaluated item")
    else:
        artifact_count = sum(item["item_count"] for item in storage["artifacts"])
        artifact_bytes = sum(item["byte_length"] for item in storage["artifacts"])
        if artifact_count != storage["stored_count"]:
            errors.append("artifact item counts must equal stored_count")
        if artifact_bytes != storage["payload_bytes"]:
            errors.append("artifact byte lengths must equal payload_bytes")

    # Canonical payload/domain bytes and hashes are owned by Julia's
    # canonicalization.jl and the production RO-field validator.  Reimplementing
    # Julia Float64 formatting here would create a second, divergent authority;
    # webapp/test/ro_field_api_contract.jl exercises the tracked fixtures,
    # tampering, and adversarial exponent-form Float64 values at that boundary.

    should_be_partial = (
        coverage["invalid_count"] > 0
        or coverage["omitted_count"] > 0
        or coverage["truncated"]
        or not coverage["enumeration_complete"]
        or not coverage["storage"]["complete"]
    )
    if document["partial"] != should_be_partial:
        errors.append("partial must reflect invalid, omitted, truncated, or incomplete evidence")
    evidence = document["evidence"]
    if document["partial"]:
        if evidence["status"] == "complete":
            errors.append("partial evidence cannot have complete status")
        if evidence["completeness_claim"] == "complete_over_declared_population":
            errors.append("partial evidence cannot make a complete-population claim")
        if (
            coverage["invalid_count"] > 0
            and evidence["completeness_claim"] != "no_positive_claim"
        ):
            errors.append("invalid evidence requires no_positive_claim")
    elif (
        evidence["status"] != "complete"
        or evidence["completeness_claim"] != "complete_over_declared_population"
    ):
        errors.append("complete evidence requires complete status and claim")
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
        document["data"]["regime_ids"][0] = None
        self.assert_contract_valid(document)

    def test_valid_sample_requires_complete_finite_values_and_regime_identity(self):
        fields = (
            ("output_values", 0, "valid samples require finite output values"),
            (
                "reaction_order_values",
                0,
                "valid samples require finite reaction-order values",
            ),
            ("regime_ids", 0, "valid samples require a non-blank regime identity"),
        )
        for field, index, expected_error in fields:
            document = copy.deepcopy(self.fixtures["sampled-grid"])
            document["data"][field][index] = None
            with self.subTest(field=field):
                self.assertTrue(
                    any(expected_error in error for error in _semantic_errors(document))
                )

        non_finite = copy.deepcopy(self.fixtures["sampled-grid"])
        non_finite["data"]["output_values"][0] = math.nan
        self.assertTrue(
            any(
                "valid samples require finite output values" in error
                for error in _semantic_errors(non_finite)
            )
        )

        blank_regime = copy.deepcopy(self.fixtures["sampled-grid"])
        blank_regime["data"]["regime_ids"][0] = " \t"
        self.assertTrue(
            any(
                "valid samples require a non-blank regime identity" in error
                for error in _semantic_errors(blank_regime)
            )
        )

    def test_sample_coordinates_and_deadline_are_strictly_finite(self):
        unordered = copy.deepcopy(self.fixtures["sampled-grid"])
        unordered["data"]["axis_coordinates"][0] = [1.0, -1.0]
        self.assertTrue(
            any(
                "Cartesian axis coordinates must be strictly increasing" in error
                for error in _semantic_errors(unordered)
            )
        )

        non_finite_deadline = copy.deepcopy(self.fixtures["sampled-grid"])
        non_finite_deadline["coverage"]["budget"]["deadline_seconds"] = math.nan
        self.assertTrue(
            any(
                "deadline_seconds must be finite and positive" in error
                for error in _semantic_errors(non_finite_deadline)
            )
        )

    def test_directional_values_are_the_declared_matrix_tangent_product(self):
        document = copy.deepcopy(self.fixtures["directional-path"])
        document["data"]["directional_reaction_order_values"][1] = 99.0
        self.assertTrue(
            any(
                "directional reaction order must equal R times the path tangent" in error
                for error in _semantic_errors(document)
            )
        )

        wrong_coordinates = copy.deepcopy(self.fixtures["directional-path"])
        wrong_coordinates["data"]["input_log_coordinates"][2] = 0.25
        self.assertTrue(
            any(
                "input coordinates must equal the machine-readable path" in error
                for error in _semantic_errors(wrong_coordinates)
            )
        )

        wrong_tangent = copy.deepcopy(self.fixtures["directional-path"])
        wrong_tangent["data"]["path_tangent_values"][2] = -1.0
        errors = _semantic_errors(wrong_tangent)
        self.assertTrue(any("path tangents must equal" in error for error in errors))
        self.assertTrue(any("R times the path tangent" in error for error in errors))

        no_tangent = copy.deepcopy(self.fixtures["directional-path"])
        del no_tangent["data"]["path_tangent_values"]
        # v1 and directional_path first entered this still-unmerged research
        # branch together at 4b4a81d. Requiring the machine tangent is therefore
        # pre-publication v1 hardening, not a hidden minor-version migration.
        self.assertTrue(list(self.validator.iter_errors(no_tangent)))

        kink = copy.deepcopy(self.fixtures["directional-path"])
        kink["data"]["path"]["vertex_parameter_values"] = [-1.0, 0.0, 1.0]
        kink["data"]["path"]["vertices"] = [
            [-1.0, -1.0],
            [0.0, 0.0],
            [1.0, 0.0],
        ]
        kink["data"]["input_log_coordinates"] = [
            -1.0,
            -1.0,
            0.0,
            0.0,
            1.0,
            0.0,
        ]
        kink["data"]["path_tangent_values"] = [1.0, 1.0, 1.0, 0.0, 1.0, 0.0]
        kink["data"]["directional_reaction_order_values"] = [2.0, 1.0, 1.0]
        self.assertTrue(
            any(
                "directional samples cannot lie on a non-differentiable path kink"
                in error
                for error in _semantic_errors(kink)
            )
        )

        near_kink = copy.deepcopy(kink)
        near_kink["data"]["path_parameter_values"][1] = 5e-13
        near_kink["data"]["input_log_coordinates"][2:4] = [5e-13, 0.0]
        self.assertFalse(
            any(
                "directional samples cannot lie on a non-differentiable path kink"
                in error
                for error in _semantic_errors(near_kink)
            )
        )

        tiny_real_kink = copy.deepcopy(kink)
        tiny_real_kink["data"]["path"]["vertices"][2] = [1.0, 5e-13]
        tiny_real_kink["data"]["input_log_coordinates"][-2:] = [1.0, 5e-13]
        tiny_real_kink["data"]["path_tangent_values"][2:] = [
            1.0,
            5e-13,
            1.0,
            5e-13,
        ]
        tiny_real_kink["data"]["directional_reaction_order_values"][1:] = [
            1.0000000000005,
            1.0000000000005,
        ]
        self.assertTrue(
            any(
                "directional samples cannot lie on a non-differentiable path kink"
                in error
                for error in _semantic_errors(tiny_real_kink)
            )
        )

    def test_directional_binding_scales_with_a_tiny_domain(self):
        scale = 1e-12
        document = copy.deepcopy(self.fixtures["directional-path"])
        for axis in document["domain"]["axes"]:
            axis["bounds"]["lower"] *= scale
            axis["bounds"]["upper"] *= scale
        document["data"]["path"]["vertices"] = [
            [coordinate * scale for coordinate in vertex]
            for vertex in document["data"]["path"]["vertices"]
        ]
        for key in (
            "input_log_coordinates",
            "path_tangent_values",
            "directional_reaction_order_values",
        ):
            document["data"][key] = [
                value * scale for value in document["data"][key]
            ]
        self.assert_contract_valid(document)

        outside = copy.deepcopy(document)
        outside["data"]["input_log_coordinates"][2] = 5e-10
        errors = _semantic_errors(outside)
        self.assertTrue(any("inside the domain" in error for error in errors))
        self.assertTrue(any(
            "input coordinates must equal the machine-readable path" in error
            for error in errors
        ))

        wrong_tangent = copy.deepcopy(document)
        wrong_tangent["data"]["path_tangent_values"][2] = 5e-10
        errors = _semantic_errors(wrong_tangent)
        self.assertTrue(any("path tangents must equal" in error for error in errors))
        self.assertTrue(any("R times the path tangent" in error for error in errors))

        wrong_directional = copy.deepcopy(document)
        wrong_directional["data"]["directional_reaction_order_values"][1] = 5e-10
        self.assertTrue(any(
            "R times the path tangent" in error
            for error in _semantic_errors(wrong_directional)
        ))

    def test_coverage_storage_and_budget_bind_the_serialized_evidence(self):
        cases = (
            (
                lambda document: document["coverage"].update(
                    {"valid_count": 3, "invalid_count": 1}
                ),
                "coverage evaluated/valid/invalid counts must match serialized evidence",
            ),
            (
                lambda document: document["coverage"]["storage"].update(
                    {"stored_count": 3}
                ),
                "complete storage must contain every evaluated item",
            ),
            (
                lambda document: document["coverage"]["budget"].update(
                    {
                        "max_payload_bytes": document["coverage"]["storage"][
                            "payload_bytes"
                        ]
                        - 1
                    }
                ),
                "stored evidence exceeds its payload-byte budget",
            ),
            (
                lambda document: document["coverage"]["budget"].update(
                    {"max_evaluated_items": 3}
                ),
                "evaluated evidence exceeds its work budget",
            ),
        )
        for mutate, expected_error in cases:
            document = copy.deepcopy(self.fixtures["sampled-grid"])
            mutate(document)
            with self.subTest(expected_error=expected_error):
                self.assertTrue(
                    any(expected_error in error for error in _semantic_errors(document))
                )

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

    def test_singular_strata_are_bound_to_positive_facet_overlap(self):
        partial_overlap = copy.deepcopy(self.fixtures["exact-cell-complex"])
        partial_overlap["data"]["singular_strata"][0]["vertices"] = [
            [1.0, 1.0],
            [2.0, 2.0],
        ]
        self.assert_contract_valid(partial_overlap)

        remote_collinear = copy.deepcopy(self.fixtures["exact-cell-complex"])
        remote_collinear["data"]["singular_strata"][0]["vertices"] = [
            [-2.0, -2.0],
            [-1.0, -1.0],
        ]
        self.assertFalse(list(self.validator.iter_errors(remote_collinear)))
        self.assertTrue(any(
            "positively overlapping one-dimensional strata" in error
            for error in _semantic_errors(remote_collinear)
        ))

        omitted_reference = copy.deepcopy(self.fixtures["exact-cell-complex"])
        singular_facet = next(
            facet
            for facet in omitted_reference["data"]["facets"]
            if facet["facet_id"] == "facet-a-b-singular"
        )
        singular_facet["singular_stratum_ids"] = []
        self.assertTrue(list(self.validator.iter_errors(omitted_reference)))
        errors = _semantic_errors(omitted_reference)
        self.assertTrue(any(
            "positively overlapping one-dimensional strata" in error
            for error in errors
        ))
        self.assertTrue(any(
            "singular-boundary facets must reference" in error
            for error in errors
        ))

        wrong_kind = copy.deepcopy(self.fixtures["exact-cell-complex"])
        singular_facet = next(
            facet
            for facet in wrong_kind["data"]["facets"]
            if facet["facet_id"] == "facet-a-b-singular"
        )
        singular_facet["kind"] = "regime_transition"
        self.assertTrue(list(self.validator.iter_errors(wrong_kind)))
        errors = _semantic_errors(wrong_kind)
        self.assertTrue(any(
            "regime-transition facets cannot reference" in error
            for error in errors
        ))
        self.assertTrue(any(
            "internal facet kind must agree" in error
            for error in errors
        ))

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

    def test_exact_geometry_helpers_are_scale_and_translation_stable(self):
        translated_square = [
            [1e9, 1e9],
            [1e9 + 2.0, 1e9],
            [1e9 + 2.0, 1e9 + 2.0],
            [1e9, 1e9 + 2.0],
        ]
        self.assertEqual(4.0, _signed_polygon_area(translated_square))

        domain_bounds = [(0.0, 1e-6), (0.0, 1e-6)]
        _, length_tolerance, _ = _exact_domain_tolerances(domain_bounds)
        concave = [
            [0.0, 0.0],
            [1e-6, 0.0],
            [4e-7, 1e-7],
            [0.0, 1e-6],
        ]
        self.assertFalse(
            _is_convex_polygon(concave, length_tolerance=length_tolerance)
        )

        left = [[0.0, 0.0], [1e-6, 0.0], [1e-6, 1e-6], [0.0, 1e-6]]
        right = [
            [5e-7, 0.0],
            [1.5e-6, 0.0],
            [1.5e-6, 1e-6],
            [5e-7, 1e-6],
        ]
        self.assertAlmostEqual(
            5e-13,
            _convex_intersection_area(
                left, right, length_tolerance=length_tolerance
            ),
            delta=64.0 * FLOAT64_EPSILON * 5e-13,
        )


if __name__ == "__main__":
    unittest.main()
