# Strict two-dimensional views over verified Cartesian RO-field chunk datasets.
#
# A slice is an exact reuse artifact: it selects source samples at declared
# Cartesian coordinate indices, retains invalid samples as gaps, and projects
# only the reaction-order matrix columns belonging to the two ordered free
# axes.  It never interpolates or evaluates a new point.

const RO_FIELD_SLICE_SPEC_SCHEMA_VERSION =
    "bne-ro-field-slice-spec/v1.0.0"
const RO_FIELD_SLICE_SCHEMA_VERSION =
    "bne-ro-field-slice/v1.0.0"
const RO_FIELD_SLICE_ALGORITHM_VERSION =
    "bne-ro-field-cartesian-exact-slice/v1.0.0"

const _ROFS_HARD_MAX_SOURCE_POINTS = 4096
const _ROFS_HARD_MAX_SOURCE_CHUNKS = 4096
const _ROFS_HARD_MAX_SOURCE_PAYLOAD_BYTES = 64 * 1024 * 1024
const _ROFS_HARD_MAX_SLICE_POINTS = 4096
const _ROFS_HARD_MAX_SLICE_SCALARS = 131_072
const _ROFS_HARD_MAX_SLICE_BYTES = 8 * 1024 * 1024
const _ROFS_HARD_MAX_RAW_DEPTH = 40

struct ROFieldSliceLimits
    max_source_points::Int
    max_source_chunks::Int
    max_source_payload_bytes::Int
    max_slice_points::Int
    max_slice_scalars::Int
    max_slice_bytes::Int
end

function _rofs_bounded_positive(raw, name::AbstractString, hard_max::Int)
    (raw isa Integer && !(raw isa Bool)) ||
        throw(ArgumentError("$(name) must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$(name) is outside the supported integer range"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$(name) must be in 1:$(hard_max)"))
    return value
end

function ROFieldSliceLimits(;
    max_source_points::Integer=_ROFS_HARD_MAX_SOURCE_POINTS,
    max_source_chunks::Integer=_ROFS_HARD_MAX_SOURCE_CHUNKS,
    max_source_payload_bytes::Integer=_ROFS_HARD_MAX_SOURCE_PAYLOAD_BYTES,
    max_slice_points::Integer=_ROFS_HARD_MAX_SLICE_POINTS,
    max_slice_scalars::Integer=_ROFS_HARD_MAX_SLICE_SCALARS,
    max_slice_bytes::Integer=_ROFS_HARD_MAX_SLICE_BYTES,
)
    return ROFieldSliceLimits(
        _rofs_bounded_positive(max_source_points, "max_source_points",
            _ROFS_HARD_MAX_SOURCE_POINTS),
        _rofs_bounded_positive(max_source_chunks, "max_source_chunks",
            _ROFS_HARD_MAX_SOURCE_CHUNKS),
        _rofs_bounded_positive(max_source_payload_bytes,
            "max_source_payload_bytes",
            _ROFS_HARD_MAX_SOURCE_PAYLOAD_BYTES),
        _rofs_bounded_positive(max_slice_points, "max_slice_points",
            _ROFS_HARD_MAX_SLICE_POINTS),
        _rofs_bounded_positive(max_slice_scalars, "max_slice_scalars",
            _ROFS_HARD_MAX_SLICE_SCALARS),
        _rofs_bounded_positive(max_slice_bytes, "max_slice_bytes",
            _ROFS_HARD_MAX_SLICE_BYTES),
    )
end

struct ROFieldSliceLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldSliceLimitExceeded)
    print(io, "RO-field slice ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

struct ROFieldSliceContractError <: Exception
    code::Symbol
    message::String
end

function Base.showerror(io::IO, err::ROFieldSliceContractError)
    print(io, "RO-field slice contract error [", err.code, "]: ",
        err.message)
end

@inline function _rofs_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= BigInt(limit) || throw(ROFieldSliceLimitExceeded(
        phase, requested, limit))
    return nothing
end

_rofs_error(code::Symbol, message::AbstractString) =
    throw(ROFieldSliceContractError(code, String(message)))

# Reject adversarially deep or wide raw JSON-like trees before any recursive
# materialization duplicates them.  The caller supplies a bound derived only
# from configured hard limits, never from attacker-declared counts.
function _rofs_preflight_raw_tree(
    raw,
    phase::Symbol,
    max_nodes::Int;
    max_depth::Int=_ROFS_HARD_MAX_RAW_DEPTH,
)
    count = Ref(BigInt(0))
    function visit(value, depth::Int, path::String)
        depth <= max_depth || _rofs_error(
            :raw_nesting_too_deep,
            "$(path) exceeds the supported nesting depth $(max_depth)")
        count[] += 1
        _rofs_limit(phase, count[], max_nodes)
        if value isa AbstractDict
            seen = Set{String}()
            for (raw_key, child) in pairs(value)
                key = _rofc_normalized_key(raw_key, path)
                key in seen && _rofs_error(
                    :duplicate_key,
                    "$(path) contains duplicate string/symbol forms of $(key)")
                push!(seen, key)
                visit(child, depth + 1, "$(path).$(key)")
            end
        elseif value isa AbstractVector || value isa Tuple
            for child in value
                visit(child, depth + 1, "$(path)[]")
            end
        end
        return nothing
    end
    visit(raw, 0, String(phase))
    return count[]
end

function _rofs_plan_raw_node_limit(limits::ROFieldChunkLimits)
    return 128 + 2 * limits.max_spec_nodes +
        limits.max_points * (limits.max_dimensions + 2)
end

function _rofs_manifest_raw_node_limit(limits::ROFieldSliceLimits)
    return 64 + 16 * limits.max_source_chunks
end

function _rofs_chunk_raw_node_limit(limits::ROFieldChunkLimits)
    return 64 + limits.max_points_per_work_unit *
        (32 + limits.max_outputs * (limits.max_dimensions + 6))
end

function _rofs_slice_raw_node_limit(
    slice_limits::ROFieldSliceLimits,
    chunk_limits::ROFieldChunkLimits,
)
    return 256 + slice_limits.max_slice_points *
        (32 + 8 * chunk_limits.max_outputs)
end

function _rofs_preflight_raw_chunk_shape(raw, limits::ROFieldChunkLimits)
    _rofs_preflight_raw_tree(raw, :source_chunk_nodes,
        _rofs_chunk_raw_node_limit(limits); max_depth=10)
    chunk = _rofc_exact_keys(_rofc_object(raw, "chunk"), (
        "schema_version", "plan_sha256", "work_unit_sha256",
        "work_unit_id", "ordinal", "axis_ids", "output_ids",
        "point_start", "point_count", "sample_order", "samples",
        "valid_count", "invalid_count",
    ), "chunk")
    axis_count = length(_rofc_array(
        _rofc_get(chunk, "axis_ids", "chunk"), "chunk.axis_ids"))
    output_count = length(_rofc_array(
        _rofc_get(chunk, "output_ids", "chunk"), "chunk.output_ids"))
    1 <= axis_count <= limits.max_dimensions || _rofs_error(
        :invalid_chunk_shape, "chunk axis count is outside the configured limit")
    1 <= output_count <= limits.max_outputs || _rofs_error(
        :invalid_chunk_shape, "chunk output count is outside the configured limit")
    point_count = _rofc_int(
        _rofc_get(chunk, "point_count", "chunk"),
        "chunk.point_count"; minimum=1)
    _rofc_limit(:points_per_work_unit, BigInt(point_count),
        limits.max_points_per_work_unit)
    orders = _rofc_array(
        _rofc_get(chunk, "sample_order", "chunk"), "chunk.sample_order")
    samples = _rofc_array(
        _rofc_get(chunk, "samples", "chunk"), "chunk.samples")
    length(orders) == point_count == length(samples) || _rofs_error(
        :invalid_chunk_shape,
        "raw chunk point_count, sample_order, and samples disagree")
    for (sample_index, raw_sample) in enumerate(samples)
        path = "chunk.samples[$(sample_index)]"
        sample = _rofc_exact_keys(raw_sample, (
            "point_index", "point", "status", "output_values",
            "reaction_order_matrix", "regime_id", "gap",
        ), path)
        length(_rofc_array(_rofc_get(sample, "point", path),
            "$(path).point")) == axis_count || _rofs_error(
                :invalid_chunk_shape,
                "raw chunk sample point has the wrong dimension")
        status = _rofc_string(
            _rofc_get(sample, "status", path), "$(path).status")
        if status == "valid"
            outputs = _rofc_array(
                _rofc_get(sample, "output_values", path),
                "$(path).output_values")
            matrix = _rofc_array(
                _rofc_get(sample, "reaction_order_matrix", path),
                "$(path).reaction_order_matrix")
            length(outputs) == output_count == length(matrix) || _rofs_error(
                :invalid_chunk_shape,
                "raw valid chunk sample has the wrong output shape")
            all(length(_rofc_array(row,
                "$(path).reaction_order_matrix[]")) == axis_count
                for row in matrix) || _rofs_error(
                    :invalid_chunk_shape,
                    "raw chunk reaction-order rows have the wrong dimension")
        elseif status != "invalid"
            _rofs_error(:invalid_sample_status,
                "raw chunk sample status must be valid or invalid")
        end
    end
    return nothing
end

function _rofs_plan_for_slice(raw_plan, chunk_limits::ROFieldChunkLimits)
    _rofs_preflight_raw_tree(raw_plan, :source_plan_nodes,
        _rofs_plan_raw_node_limit(chunk_limits); max_depth=40)
    plan = validate_ro_field_chunk_plan!(raw_plan; limits=chunk_limits)
    identity = plan["identity"]
    identity["plan_kind"] == "cartesian_grid" || _rofs_error(
        :non_cartesian_source,
        "strict slices require a Cartesian chunk plan")
    dimensions = length(identity["axis_ids"])
    dimensions in (3, 4) || _rofs_error(
        :unsupported_source_dimension,
        "strict slices accept exactly three- or four-dimensional sources")
    return plan
end

function _rofs_normalize_slice_spec(raw_spec, plan)
    _rofs_preflight_raw_tree(
        raw_spec, :slice_spec_nodes, 128; max_depth=6)
    spec = _rofc_exact_keys(_rofc_materialize(raw_spec), (
        "schema_version", "free_axis_ids", "fixed_axes",
    ), "slice_spec")
    _rofc_get(spec, "schema_version", "slice_spec") ==
        RO_FIELD_SLICE_SPEC_SCHEMA_VERSION || _rofs_error(
            :unsupported_version, "unsupported slice-spec version")

    identity = plan["identity"]
    axis_ids = identity["axis_ids"]
    coordinates = identity["axis_coordinates"]
    free_ids = _rofc_ids(
        _rofc_get(spec, "free_axis_ids", "slice_spec"),
        "slice_spec.free_axis_ids", 2, 2)
    free_positions = Int[]
    for axis_id in free_ids
        position = findfirst(==(axis_id), axis_ids)
        position === nothing && _rofs_error(
            :unknown_free_axis,
            "free axis $(axis_id) is not present in the source plan")
        push!(free_positions, position)
    end
    allunique(free_positions) || _rofs_error(
        :duplicate_free_axis, "the two free axes must be distinct")

    raw_fixed = _rofc_array(
        _rofc_get(spec, "fixed_axes", "slice_spec"),
        "slice_spec.fixed_axes")
    length(raw_fixed) == length(axis_ids) - 2 || _rofs_error(
        :incomplete_fixed_axes,
        "every non-free source axis must be fixed exactly once")
    fixed_by_position = Dict{Int,Dict{String,Any}}()
    for (entry_index, raw_entry) in enumerate(raw_fixed)
        path = "slice_spec.fixed_axes[$(entry_index)]"
        entry = _rofc_exact_keys(raw_entry, (
            "axis_id", "coordinate_index", "coordinate",
        ), path)
        axis_id = _rofc_identifier(
            _rofc_get(entry, "axis_id", path), "$(path).axis_id")
        position = findfirst(==(axis_id), axis_ids)
        position === nothing && _rofs_error(
            :unknown_fixed_axis,
            "fixed axis $(axis_id) is not present in the source plan")
        position in free_positions && _rofs_error(
            :free_axis_fixed,
            "free axis $(axis_id) cannot also be fixed")
        haskey(fixed_by_position, position) && _rofs_error(
            :duplicate_fixed_axis,
            "fixed axis $(axis_id) is declared more than once")
        coordinate_index = _rofc_int(
            _rofc_get(entry, "coordinate_index", path),
            "$(path).coordinate_index"; minimum=1)
        coordinate_index <= length(coordinates[position]) || _rofs_error(
            :fixed_coordinate_out_of_domain,
            "fixed coordinate index is outside axis $(axis_id)")
        coordinate = _rofc_finite(
            _rofc_get(entry, "coordinate", path), "$(path).coordinate")
        canonical = coordinates[position][coordinate_index]
        coordinate == canonical || _rofs_error(
            :fixed_coordinate_mismatch,
            "fixed coordinate must equal the source coordinate at its declared index")
        fixed_by_position[position] = Dict{String,Any}(
            "source_axis_index" => position,
            "axis_id" => axis_id,
            "coordinate_index" => coordinate_index,
            "coordinate" => canonical,
        )
    end
    expected_fixed_positions = sort!(setdiff(
        collect(eachindex(axis_ids)), free_positions))
    sort!(collect(keys(fixed_by_position))) == expected_fixed_positions ||
        _rofs_error(:incomplete_fixed_axes,
            "fixed axes do not equal the complement of the free axes")

    free_axes = Dict{String,Any}[
        Dict{String,Any}(
            "source_axis_index" => position,
            "axis_id" => axis_ids[position],
            "coordinates" => copy(coordinates[position]),
        )
        for position in free_positions
    ]
    fixed_axes = Dict{String,Any}[
        fixed_by_position[position] for position in expected_fixed_positions
    ]
    return free_positions, free_axes, fixed_axes
end

function _rofs_preflight_manifest(
    raw_manifest,
    plan,
    slice_limits::ROFieldSliceLimits,
    chunk_limits::ROFieldChunkLimits,
)
    _rofs_preflight_raw_tree(raw_manifest, :source_manifest_nodes,
        _rofs_manifest_raw_node_limit(slice_limits); max_depth=6)
    object = _rofc_exact_keys(_rofc_object(raw_manifest, "manifest"), (
        "schema_version", "plan_sha256", "point_count",
        "work_unit_count", "chunk_count", "valid_count", "invalid_count",
        "chunk_payload_bytes", "chunks", "manifest_sha256",
    ), "manifest")
    _rofc_get(object, "schema_version", "manifest") ==
        RO_FIELD_DATASET_MANIFEST_SCHEMA_VERSION || _rofs_error(
            :unsupported_version, "unsupported dataset-manifest version")
    raw_entries = _rofc_get(object, "chunks", "manifest")
    (raw_entries isa AbstractVector || raw_entries isa Tuple) ||
        _rofs_error(:invalid_manifest, "manifest.chunks must be an array")
    entry_count = length(raw_entries)
    _rofs_limit(:source_chunks, BigInt(entry_count),
        slice_limits.max_source_chunks)

    point_count = _rofc_int(
        _rofc_get(object, "point_count", "manifest"),
        "manifest.point_count"; minimum=1)
    chunk_count = _rofc_int(
        _rofc_get(object, "chunk_count", "manifest"),
        "manifest.chunk_count"; minimum=1)
    work_unit_count = _rofc_int(
        _rofc_get(object, "work_unit_count", "manifest"),
        "manifest.work_unit_count"; minimum=1)
    valid_count = _rofc_int(
        _rofc_get(object, "valid_count", "manifest"),
        "manifest.valid_count")
    invalid_count = _rofc_int(
        _rofc_get(object, "invalid_count", "manifest"),
        "manifest.invalid_count")
    payload_bytes = _rofc_int(
        _rofc_get(object, "chunk_payload_bytes", "manifest"),
        "manifest.chunk_payload_bytes")
    _rofs_limit(:source_points, BigInt(point_count),
        slice_limits.max_source_points)
    _rofs_limit(:source_payload_bytes, BigInt(payload_bytes),
        slice_limits.max_source_payload_bytes)
    entry_count == chunk_count == work_unit_count || _rofs_error(
        :incomplete_manifest,
        "manifest chunk and work-unit counts must cover every listed chunk")
    point_count == plan["identity"]["point_count"] || _rofs_error(
        :foreign_manifest,
        "manifest point_count differs from the source plan")
    expected_unit_count = cld(BigInt(point_count),
        BigInt(plan["identity"]["work_unit_size"]))
    BigInt(work_unit_count) == expected_unit_count || _rofs_error(
        :incomplete_manifest,
        "manifest does not declare every deterministic source work unit")
    BigInt(valid_count) + BigInt(invalid_count) == BigInt(point_count) ||
        _rofs_error(:manifest_count_mismatch,
            "manifest valid/invalid counts do not cover its points")
    _rofc_hash(_rofc_get(object, "plan_sha256", "manifest"),
        "manifest.plan_sha256") == plan["plan_sha256"] || _rofs_error(
            :foreign_manifest, "manifest references a different source plan")

    # A self-hashed manifest can be regenerated by an attacker after lowering
    # every declared payload byte count.  Before any loader call, reserve the
    # maximum canonical bytes each admitted chunk is permitted to contain.
    # This may conservatively reject a highly fragmented small dataset, but it
    # proves that no loader result can exceed the source-payload budget.
    conservative_payload = BigInt(entry_count) *
        BigInt(chunk_limits.max_chunk_bytes)
    _rofs_limit(:source_payload_reservation_bytes, conservative_payload,
        slice_limits.max_source_payload_bytes)

    declared_payload = BigInt(0)
    chunk_hashes = Set{String}()
    expected_units = Dict{String,Any}[]
    sizehint!(expected_units, entry_count)
    for (entry_index, raw_entry) in enumerate(raw_entries)
        path = "manifest.chunks[$(entry_index)]"
        entry = _rofc_exact_keys(raw_entry, (
            "ordinal", "work_unit_id", "work_unit_sha256", "chunk_sha256",
            "point_start", "point_count", "valid_count", "invalid_count",
            "chunk_payload_bytes",
        ), path)
        unit = _rofc_plan_work_unit(plan, entry_index)
        ordinal = _rofc_int(_rofc_get(entry, "ordinal", path),
            "$(path).ordinal"; minimum=1)
        work_unit_id = _rofc_identifier(
            _rofc_get(entry, "work_unit_id", path), "$(path).work_unit_id")
        work_unit_hash = _rofc_hash(
            _rofc_get(entry, "work_unit_sha256", path),
            "$(path).work_unit_sha256")
        point_start = _rofc_int(
            _rofc_get(entry, "point_start", path),
            "$(path).point_start"; minimum=1)
        entry_point_count = _rofc_int(
            _rofc_get(entry, "point_count", path),
            "$(path).point_count"; minimum=1)
        ordinal == unit["ordinal"] &&
            work_unit_id == unit["work_unit_id"] &&
            work_unit_hash == _rofc_sha256(unit) &&
            point_start == unit["point_start"] &&
            entry_point_count == unit["point_count"] || _rofs_error(
                :foreign_manifest_entry,
                "manifest entry $(entry_index) is not the deterministic source-plan work unit")
        entry_valid = _rofc_int(
            _rofc_get(entry, "valid_count", path), "$(path).valid_count")
        entry_invalid = _rofc_int(
            _rofc_get(entry, "invalid_count", path), "$(path).invalid_count")
        entry_valid + entry_invalid == entry_point_count || _rofs_error(
            :manifest_count_mismatch,
            "manifest entry valid/invalid counts do not cover its work unit")
        chunk_hash = _rofc_hash(
            _rofc_get(entry, "chunk_sha256", path),
            "$(path).chunk_sha256")
        chunk_hash in chunk_hashes && _rofs_error(
            :duplicate_chunk, "manifest repeats one chunk identity")
        push!(chunk_hashes, chunk_hash)
        declared_payload += BigInt(_rofc_int(
            _rofc_get(entry, "chunk_payload_bytes", path),
            "$(path).chunk_payload_bytes"; minimum=1))
        _rofs_limit(:source_payload_bytes, declared_payload,
            slice_limits.max_source_payload_bytes)
        push!(expected_units, unit)
    end
    declared_payload == BigInt(payload_bytes) || _rofs_error(
        :manifest_payload_mismatch,
        "manifest chunk payload bytes do not sum to the declared total")

    # Only after all count and payload budgets pass is the full manifest copied
    # and hashed.  No source chunk loader has been called at this point.
    manifest = _rofc_exact_keys(_rofc_materialize(raw_manifest), (
        "schema_version", "plan_sha256", "point_count",
        "work_unit_count", "chunk_count", "valid_count", "invalid_count",
        "chunk_payload_bytes", "chunks", "manifest_sha256",
    ), "manifest")
    supplied_hash = _rofc_hash(
        _rofc_get(manifest, "manifest_sha256", "manifest"),
        "manifest.manifest_sha256")
    identity = copy(manifest)
    pop!(identity, "manifest_sha256")
    supplied_hash == _rofc_sha256(identity) || _rofs_error(
        :manifest_hash_mismatch,
        "manifest content does not match manifest_sha256")
    return manifest, expected_units
end

function _rofs_preflight_slice_allocation(
    plan,
    free_axes,
    slice_limits::ROFieldSliceLimits,
)
    point_count = BigInt(1)
    for axis in free_axes
        point_count *= BigInt(length(axis["coordinates"]))
    end
    _rofs_limit(:slice_points, point_count, slice_limits.max_slice_points)
    output_count = BigInt(length(plan["identity"]["output_ids"]))
    source_dimensions = BigInt(length(plan["identity"]["axis_ids"]))
    # Per selected sample: free point, full source point, output values, and
    # the output-by-two projected matrix.  Counts are computed in BigInt before
    # allocating the selected-point table or result samples.
    scalar_count = point_count *
        (BigInt(2) + source_dimensions + output_count * BigInt(3))
    _rofs_limit(:slice_scalars, scalar_count,
        slice_limits.max_slice_scalars)
    return Int(point_count)
end

function _rofs_load_chunks(
    chunk_source,
    manifest,
    plan,
    expected_units,
    slice_limits::ROFieldSliceLimits,
    chunk_limits::ROFieldChunkLimits,
    cancel_check,
)
    entries = manifest["chunks"]
    length(entries) == length(expected_units) || _rofs_error(
        :incomplete_manifest,
        "manifest entries do not cover deterministic source work units")
    chunks = Dict{String,Any}[]
    sizehint!(chunks, length(entries))

    function accept(raw_chunk, entry, unit; raw_shape_checked::Bool=false)
        raw_shape_checked ||
            _rofs_preflight_raw_chunk_shape(raw_chunk, chunk_limits)
        normalized = validate_ro_field_chunk!(raw_chunk;
            plan=plan, work_unit=unit, limits=chunk_limits,
            cancel_check=cancel_check)
        bytes = _rofc_bytes(normalized)
        observed_hash = _rofc_sha256_bytes(bytes)
        observed_hash == entry["chunk_sha256"] || _rofs_error(
            :chunk_hash_mismatch,
            "loaded chunk does not match its manifest content address")
        length(bytes) == entry["chunk_payload_bytes"] || _rofs_error(
            :manifest_payload_mismatch,
            "manifest entry understates or overstates canonical chunk bytes")
        return normalized, length(bytes)
    end

    exact_payload = BigInt(0)
    if chunk_source isa Function
        for index in eachindex(entries)
            cancel_check()
            entry = entries[index]
            expected_hash = entry["chunk_sha256"]
            chunk = chunk_source(expected_hash)
            chunk === nothing && _rofs_error(
                :missing_chunk,
                "chunk loader returned no chunk for $(expected_hash)")
            cancel_check()
            normalized, payload = accept(
                chunk, entry, expected_units[index])
            exact_payload += BigInt(payload)
            _rofs_limit(:source_payload_bytes, exact_payload,
                slice_limits.max_source_payload_bytes)
            push!(chunks, normalized)
        end
    elseif chunk_source isa AbstractVector || chunk_source isa Tuple
        length(chunk_source) == length(entries) || _rofs_error(
            :incomplete_chunks,
            "the supplied chunks do not cover every manifest entry")
        entry_by_work = Dict(
            entry["work_unit_sha256"] => (entry, expected_units[index])
            for (index, entry) in enumerate(entries)
        )
        seen_work = Set{String}()
        for raw_chunk in chunk_source
            cancel_check()
            _rofs_preflight_raw_chunk_shape(raw_chunk, chunk_limits)
            object = _rofc_object(raw_chunk, "chunk")
            work_hash = _rofc_hash(
                _rofc_get(object, "work_unit_sha256", "chunk"),
                "chunk.work_unit_sha256")
            haskey(entry_by_work, work_hash) || _rofs_error(
                :foreign_chunk,
                "supplied chunk references a work unit outside the manifest")
            work_hash in seen_work && _rofs_error(
                :duplicate_chunk,
                "supplied chunks repeat one manifest work unit")
            push!(seen_work, work_hash)
            entry, unit = entry_by_work[work_hash]
            normalized, payload = accept(raw_chunk, entry, unit;
                raw_shape_checked=true)
            exact_payload += BigInt(payload)
            _rofs_limit(:source_payload_bytes, exact_payload,
                slice_limits.max_source_payload_bytes)
            push!(chunks, normalized)
        end
        length(seen_work) == length(entries) || _rofs_error(
            :incomplete_chunks,
            "the supplied chunks do not cover every manifest work unit")
    else
        _rofs_error(:invalid_chunk_source,
            "chunk source must be an array, tuple, or content-hash loader")
    end
    exact_payload == BigInt(manifest["chunk_payload_bytes"]) || _rofs_error(
        :manifest_payload_mismatch,
        "manifest payload total differs from exact canonical chunk bytes")
    validate_ro_field_dataset_manifest!(manifest, plan, chunks;
        limits=chunk_limits, cancel_check=cancel_check)
    return chunks
end

function _rofs_selected_samples(
    plan,
    chunks,
    free_positions,
    free_axes,
    fixed_axes,
    expected_count::Int,
    chunk_limits::ROFieldChunkLimits,
    cancel_check,
)
    units, _, chunks_by_unit = _rofc_units_and_chunks(
        plan, chunks; limits=chunk_limits, cancel_check=cancel_check)
    coordinate_positions = [
        Dict(value => index for (index, value) in enumerate(axis["coordinates"]))
        for axis in free_axes
    ]
    selected = Dict{Tuple{Int,Int},Dict{String,Any}}()
    for unit in units
        cancel_check()
        chunk = chunks_by_unit[_rofc_sha256(unit)]
        for sample in chunk["samples"]
            cancel_check()
            source_point = sample["point"]
            matches = all(source_point[fixed["source_axis_index"]] ==
                fixed["coordinate"] for fixed in fixed_axes)
            matches || continue
            key = (
                coordinate_positions[1][source_point[free_positions[1]]],
                coordinate_positions[2][source_point[free_positions[2]]],
            )
            haskey(selected, key) && _rofs_error(
                :duplicate_slice_point,
                "more than one source sample maps to one slice coordinate")
            selected[key] = sample
        end
    end
    isempty(selected) && _rofs_error(
        :no_matching_slice_points,
        "no source sample matches the declared fixed coordinates")
    length(selected) == expected_count || _rofs_error(
        :incomplete_slice_coverage,
        "source samples do not completely cover the declared slice")
    return selected
end

function _rofs_slice_sample(
    source_sample,
    slice_point_index::Int,
    free_positions,
    free_point,
)
    common = Dict{String,Any}(
        "slice_point_index" => slice_point_index,
        "source_point_index" => source_sample["point_index"],
        "point" => copy(free_point),
        "source_point" => copy(source_sample["point"]),
        "status" => source_sample["status"],
    )
    if source_sample["status"] == "valid"
        common["output_values"] = copy(source_sample["output_values"])
        common["reaction_order_matrix"] = Vector{Float64}[
            Float64[row[free_positions[1]], row[free_positions[2]]]
            for row in source_sample["reaction_order_matrix"]
        ]
        common["regime_id"] = source_sample["regime_id"]
        common["gap"] = nothing
    else
        common["output_values"] = nothing
        common["reaction_order_matrix"] = nothing
        common["regime_id"] = nothing
        common["gap"] = copy(source_sample["gap"])
    end
    return common
end

"""
Build an exact two-dimensional slice from a verified Cartesian chunk dataset.

`chunk_source` is either the complete chunk array or a function mapping each
manifest `chunk_sha256` to its chunk document.  Count, scalar, and declared
source-payload budgets are checked before the function is called.
"""
function build_ro_field_slice(
    raw_plan,
    raw_manifest,
    chunk_source,
    raw_slice_spec;
    runtime_context=Dict{String,Any}(),
    slice_limits::ROFieldSliceLimits=ROFieldSliceLimits(),
    chunk_limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = _rofs_plan_for_slice(raw_plan, chunk_limits)
    free_positions, free_axes, fixed_axes =
        _rofs_normalize_slice_spec(raw_slice_spec, plan)
    expected_count = _rofs_preflight_slice_allocation(
        plan, free_axes, slice_limits)
    manifest, expected_units = _rofs_preflight_manifest(
        raw_manifest, plan, slice_limits, chunk_limits)

    # No callback or chunk load occurs before every deterministic preflight
    # above has accepted the request.
    cancel_check()
    chunks = _rofs_load_chunks(
        chunk_source, manifest, plan, expected_units, slice_limits,
        chunk_limits, cancel_check)
    selected = _rofs_selected_samples(
        plan, chunks, free_positions, free_axes, fixed_axes,
        expected_count, chunk_limits, cancel_check)

    samples = Dict{String,Any}[]
    sample_order = Int[]
    sizehint!(samples, expected_count)
    sizehint!(sample_order, expected_count)
    slice_index = 0
    for first_index in eachindex(free_axes[1]["coordinates"])
        for second_index in eachindex(free_axes[2]["coordinates"])
            cancel_check()
            slice_index += 1
            key = (first_index, second_index)
            haskey(selected, key) || _rofs_error(
                :incomplete_slice_coverage,
                "one declared Cartesian slice point is missing")
            source_sample = selected[key]
            free_point = Float64[
                free_axes[1]["coordinates"][first_index],
                free_axes[2]["coordinates"][second_index],
            ]
            push!(samples, _rofs_slice_sample(
                source_sample, slice_index, free_positions, free_point))
            push!(sample_order, source_sample["point_index"])
        end
    end
    valid_count = count(sample -> sample["status"] == "valid", samples)
    invalid_count = length(samples) - valid_count
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_SLICE_SCHEMA_VERSION,
        "algorithm_version" => RO_FIELD_SLICE_ALGORITHM_VERSION,
        "source" => Dict{String,Any}(
            "plan_sha256" => plan["plan_sha256"],
            "manifest_sha256" => manifest["manifest_sha256"],
        ),
        "source_axis_ids" => copy(plan["identity"]["axis_ids"]),
        "free_axes" => free_axes,
        "fixed_axes" => fixed_axes,
        "output_ids" => copy(plan["identity"]["output_ids"]),
        "shape" => Int[
            length(free_axes[1]["coordinates"]),
            length(free_axes[2]["coordinates"]),
        ],
        "point_count" => expected_count,
        "coverage" => Dict{String,Any}(
            "source_point_count" => plan["identity"]["point_count"],
            "expected_slice_point_count" => expected_count,
            "selected_point_count" => length(samples),
            "valid_count" => valid_count,
            "invalid_count" => invalid_count,
            "complete" => true,
        ),
        "sample_order" => sample_order,
        "value_origin" => "reused_exact",
        "interpolation" => "none",
        "samples" => samples,
    )
    artifact = Dict{String,Any}(
        "schema_version" => RO_FIELD_SLICE_SCHEMA_VERSION,
        "slice_sha256" => _rofc_sha256(identity),
        "identity" => identity,
        "runtime_context" => _rofc_runtime_context(runtime_context),
    )
    _rofs_limit(:slice_bytes, BigInt(length(_rofc_bytes(artifact))),
        slice_limits.max_slice_bytes)
    return validate_ro_field_slice!(artifact;
        source_plan=plan, source_manifest=manifest, chunk_source=chunks,
        require_source_provenance=true,
        slice_limits=slice_limits, chunk_limits=chunk_limits,
        cancel_check=cancel_check)
end

function _rofs_normalize_stored_sample(
    raw_sample,
    slice_index::Int,
    expected_free_point,
    axis_count::Int,
    output_count::Int,
    free_positions,
    fixed_by_position,
    source_point_count::Int,
)
    path = "slice.identity.samples[$(slice_index)]"
    sample = _rofc_exact_keys(raw_sample, (
        "slice_point_index", "source_point_index", "point",
        "source_point", "status", "output_values",
        "reaction_order_matrix", "regime_id", "gap",
    ), path)
    _rofc_int(_rofc_get(sample, "slice_point_index", path),
        "$(path).slice_point_index"; minimum=1) == slice_index ||
        _rofs_error(:sample_order_mismatch,
            "slice sample index differs from Cartesian order")
    source_index = _rofc_int(
        _rofc_get(sample, "source_point_index", path),
        "$(path).source_point_index"; minimum=1)
    source_index <= source_point_count || _rofs_error(
        :source_point_out_of_range,
        "slice sample references a source point outside the plan")
    free_point = _rofc_normalize_point(
        _rofc_get(sample, "point", path), 2, "$(path).point")
    free_point == expected_free_point || _rofs_error(
        :sample_order_mismatch,
        "slice point differs from the ordered free-axis grid")
    source_point = _rofc_normalize_point(
        _rofc_get(sample, "source_point", path), axis_count,
        "$(path).source_point")
    source_point[free_positions[1]] == free_point[1] &&
        source_point[free_positions[2]] == free_point[2] || _rofs_error(
            :source_point_mismatch,
            "source point does not project to the slice point")
    for (position, coordinate) in fixed_by_position
        source_point[position] == coordinate || _rofs_error(
            :source_point_mismatch,
            "source point does not satisfy every fixed coordinate")
    end

    status = _rofc_string(_rofc_get(sample, "status", path),
        "$(path).status")
    normalized = Dict{String,Any}(
        "slice_point_index" => slice_index,
        "source_point_index" => source_index,
        "point" => free_point,
        "source_point" => source_point,
        "status" => status,
    )
    if status == "valid"
        _rofc_get(sample, "gap", path) === nothing || _rofs_error(
            :fabricated_numeric_gap, "valid slice samples cannot carry gaps")
        outputs = _rofc_array(
            _rofc_get(sample, "output_values", path),
            "$(path).output_values")
        length(outputs) == output_count || _rofs_error(
            :invalid_sample_shape,
            "slice output values do not preserve output order and count")
        matrix = _rofc_array(
            _rofc_get(sample, "reaction_order_matrix", path),
            "$(path).reaction_order_matrix")
        length(matrix) == output_count || _rofs_error(
            :invalid_sample_shape,
            "slice reaction-order rows do not preserve output count")
        normalized["output_values"] = Float64[
            _rofc_finite(value, "$(path).output_values[]")
            for value in outputs
        ]
        normalized_matrix = Vector{Float64}[]
        for row in matrix
            values = _rofc_array(row,
                "$(path).reaction_order_matrix[]")
            length(values) == 2 || _rofs_error(
                :invalid_sample_shape,
                "slice reaction-order rows must contain two free-axis components")
            push!(normalized_matrix, Float64[
                _rofc_finite(value,
                    "$(path).reaction_order_matrix[][]")
                for value in values
            ])
        end
        normalized["reaction_order_matrix"] = normalized_matrix
        raw_regime = _rofc_get(sample, "regime_id", path)
        normalized["regime_id"] = raw_regime === nothing ? nothing :
            _rofc_identifier(raw_regime, "$(path).regime_id")
        normalized["gap"] = nothing
    elseif status == "invalid"
        _rofc_get(sample, "output_values", path) === nothing &&
            _rofc_get(sample, "reaction_order_matrix", path) === nothing &&
            _rofc_get(sample, "regime_id", path) === nothing || _rofs_error(
                :fabricated_numeric_gap,
                "invalid slice samples must retain null numeric fields")
        normalized["output_values"] = nothing
        normalized["reaction_order_matrix"] = nothing
        normalized["regime_id"] = nothing
        normalized["gap"] = _rofc_normalize_gap(
            _rofc_get(sample, "gap", path), "$(path).gap")
    else
        _rofs_error(:invalid_sample_status,
            "slice sample status must be valid or invalid")
    end
    return normalized
end

function _rofs_preflight_raw_slice_shape(
    raw_artifact,
    slice_limits::ROFieldSliceLimits,
    chunk_limits::ROFieldChunkLimits,
)
    _rofs_preflight_raw_tree(raw_artifact, :slice_nodes,
        _rofs_slice_raw_node_limit(slice_limits, chunk_limits);
        max_depth=12)
    top = _rofc_exact_keys(_rofc_object(raw_artifact, "slice"), (
        "schema_version", "slice_sha256", "identity", "runtime_context",
    ), "slice")
    identity = _rofc_exact_keys(
        _rofc_get(top, "identity", "slice"), (
            "schema_version", "algorithm_version", "source",
            "source_axis_ids", "free_axes", "fixed_axes", "output_ids",
            "shape", "point_count", "coverage", "sample_order",
            "value_origin", "interpolation", "samples",
        ), "slice.identity")
    axis_count = length(_rofc_array(
        _rofc_get(identity, "source_axis_ids", "slice.identity"),
        "slice.identity.source_axis_ids"))
    output_count = length(_rofc_array(
        _rofc_get(identity, "output_ids", "slice.identity"),
        "slice.identity.output_ids"))
    3 <= axis_count <= 4 || _rofs_error(
        :invalid_slice_shape, "raw slice source must have three or four axes")
    1 <= output_count <= chunk_limits.max_outputs || _rofs_error(
        :invalid_slice_shape, "raw slice output count exceeds its limit")
    length(_rofc_array(_rofc_get(identity, "free_axes", "slice.identity"),
        "slice.identity.free_axes")) == 2 || _rofs_error(
            :invalid_slice_shape, "raw slice must have two free axes")
    length(_rofc_array(_rofc_get(identity, "fixed_axes", "slice.identity"),
        "slice.identity.fixed_axes")) == axis_count - 2 || _rofs_error(
            :invalid_slice_shape,
            "raw slice must fix every source axis not selected as free")
    shape = _rofc_array(_rofc_get(identity, "shape", "slice.identity"),
        "slice.identity.shape")
    length(shape) == 2 || _rofs_error(
        :invalid_slice_shape, "raw slice shape must contain two lengths")
    point_count = _rofc_int(
        _rofc_get(identity, "point_count", "slice.identity"),
        "slice.identity.point_count"; minimum=1)
    _rofs_limit(:slice_points, BigInt(point_count),
        slice_limits.max_slice_points)
    sample_order = _rofc_array(
        _rofc_get(identity, "sample_order", "slice.identity"),
        "slice.identity.sample_order")
    samples = _rofc_array(
        _rofc_get(identity, "samples", "slice.identity"),
        "slice.identity.samples")
    length(sample_order) == point_count == length(samples) || _rofs_error(
        :invalid_slice_shape,
        "raw slice point_count, sample_order, and samples disagree")
    for (sample_index, raw_sample) in enumerate(samples)
        path = "slice.identity.samples[$(sample_index)]"
        sample = _rofc_exact_keys(raw_sample, (
            "slice_point_index", "source_point_index", "point",
            "source_point", "status", "output_values",
            "reaction_order_matrix", "regime_id", "gap",
        ), path)
        length(_rofc_array(_rofc_get(sample, "point", path),
            "$(path).point")) == 2 || _rofs_error(
                :invalid_slice_shape,
                "raw slice point has the wrong dimension")
        length(_rofc_array(_rofc_get(sample, "source_point", path),
            "$(path).source_point")) == axis_count || _rofs_error(
                :invalid_slice_shape,
                "raw slice source point has the wrong dimension")
        status = _rofc_string(
            _rofc_get(sample, "status", path), "$(path).status")
        if status == "valid"
            outputs = _rofc_array(
                _rofc_get(sample, "output_values", path),
                "$(path).output_values")
            matrix = _rofc_array(
                _rofc_get(sample, "reaction_order_matrix", path),
                "$(path).reaction_order_matrix")
            length(outputs) == output_count == length(matrix) || _rofs_error(
                :invalid_slice_shape,
                "raw valid slice sample has the wrong output shape")
            all(length(_rofc_array(row,
                "$(path).reaction_order_matrix[]")) == 2
                for row in matrix) || _rofs_error(
                    :invalid_slice_shape,
                    "raw slice reaction-order rows must have two components")
        elseif status != "invalid"
            _rofs_error(:invalid_sample_status,
                "raw slice sample status must be valid or invalid")
        end
    end
    return nothing
end

function _rofs_verify_source_semantics!(
    slice,
    plan,
    chunks,
    chunk_limits::ROFieldChunkLimits,
    cancel_check,
)
    identity = slice["identity"]
    free_axes = identity["free_axes"]
    free_positions = getindex.(free_axes, "source_axis_index")
    fixed_axes = identity["fixed_axes"]
    expected_count = identity["point_count"]
    selected = _rofs_selected_samples(
        plan, chunks, free_positions, free_axes, fixed_axes,
        expected_count, chunk_limits, cancel_check)
    sample_index = 0
    for first_index in eachindex(free_axes[1]["coordinates"])
        for second_index in eachindex(free_axes[2]["coordinates"])
            cancel_check()
            sample_index += 1
            key = (first_index, second_index)
            haskey(selected, key) || _rofs_error(
                :incomplete_slice_coverage,
                "verified source is missing a declared slice point")
            free_point = Float64[
                free_axes[1]["coordinates"][first_index],
                free_axes[2]["coordinates"][second_index],
            ]
            expected = _rofs_slice_sample(
                selected[key], sample_index, free_positions, free_point)
            _rofc_canonical_json(identity["samples"][sample_index]) ==
                _rofc_canonical_json(expected) || _rofs_error(
                    :source_semantics_mismatch,
                    "slice values or gaps differ from the authenticated source chunk")
        end
    end
    return slice
end

"""
Validate a slice's bounded structure and self-hash.

With `allow_unverified_source=true`, no source context deliberately performs
only structural validation: a caller who changes values and recomputes
`slice_sha256` has not established `reused_exact` provenance.  A supplied
`source_plan` in that explicitly downgraded mode additionally authenticates
Cartesian point/index geometry.  Full value/gap provenance is checked only when
`source_plan`, `source_manifest`, and `chunk_source` are all supplied (or when
`require_source_provenance=true`, which fails if that complete context is
missing).  The default fails closed unless the complete source chain is present.
This proves consistency to the supplied content-addressed manifest root; a
caller that needs origin authenticity must obtain or pin that root hash through
its trusted job/store boundary.
"""
function validate_ro_field_slice!(
    raw_artifact;
    source_plan=nothing,
    source_manifest=nothing,
    chunk_source=nothing,
    require_source_provenance::Bool=false,
    allow_unverified_source::Bool=false,
    slice_limits::ROFieldSliceLimits=ROFieldSliceLimits(),
    chunk_limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    has_manifest = source_manifest !== nothing
    has_chunks = chunk_source !== nothing
    full_source_context = source_plan !== nothing && has_manifest && has_chunks
    (has_manifest == has_chunks) || _rofs_error(
        :incomplete_source_context,
        "source_manifest and chunk_source must be supplied together")
    (has_manifest || has_chunks) && source_plan === nothing && _rofs_error(
        :incomplete_source_context,
        "source_plan is required with a manifest and chunks")
    require_source_provenance && !full_source_context && _rofs_error(
        :incomplete_source_context,
        "full provenance validation requires source plan, manifest, and chunks")
    !full_source_context && !allow_unverified_source && _rofs_error(
        :unverified_source_forbidden,
        "validation without the full source chain requires allow_unverified_source=true")

    _rofs_preflight_raw_slice_shape(
        raw_artifact, slice_limits, chunk_limits)
    raw_top = _rofc_object(raw_artifact, "slice")
    raw_identity = _rofc_object(
        _rofc_get(raw_top, "identity", "slice"), "slice.identity")
    raw_point_count = _rofc_int(
        _rofc_get(raw_identity, "point_count", "slice.identity"),
        "slice.identity.point_count"; minimum=1)
    _rofs_limit(:slice_points, BigInt(raw_point_count),
        slice_limits.max_slice_points)
    raw_samples = _rofc_get(raw_identity, "samples", "slice.identity")
    (raw_samples isa AbstractVector || raw_samples isa Tuple) ||
        _rofs_error(:invalid_slice, "slice samples must be an array")
    length(raw_samples) == raw_point_count || _rofs_error(
        :slice_count_mismatch,
        "slice point_count and samples disagree")

    artifact = _rofc_exact_keys(_rofc_materialize(raw_artifact), (
        "schema_version", "slice_sha256", "identity", "runtime_context",
    ), "slice")
    _rofc_get(artifact, "schema_version", "slice") ==
        RO_FIELD_SLICE_SCHEMA_VERSION || _rofs_error(
            :unsupported_version, "unsupported slice version")
    identity = _rofc_exact_keys(
        _rofc_get(artifact, "identity", "slice"), (
            "schema_version", "algorithm_version", "source",
            "source_axis_ids", "free_axes", "fixed_axes", "output_ids",
            "shape", "point_count", "coverage", "sample_order",
            "value_origin", "interpolation", "samples",
        ), "slice.identity")
    _rofc_get(identity, "schema_version", "slice.identity") ==
        RO_FIELD_SLICE_SCHEMA_VERSION || _rofs_error(
            :unsupported_version, "slice identity version is unsupported")
    _rofc_get(identity, "algorithm_version", "slice.identity") ==
        RO_FIELD_SLICE_ALGORITHM_VERSION || _rofs_error(
            :unsupported_algorithm, "slice algorithm version is unsupported")
    _rofc_get(identity, "value_origin", "slice.identity") ==
        "reused_exact" || _rofs_error(
            :fabricated_value_origin,
            "strict slice values must be labelled reused_exact")
    _rofc_get(identity, "interpolation", "slice.identity") ==
        "none" || _rofs_error(
            :interpolation_forbidden,
            "strict slice artifacts cannot contain interpolation")

    source = _rofc_exact_keys(
        _rofc_get(identity, "source", "slice.identity"),
        ("plan_sha256", "manifest_sha256"), "slice.identity.source")
    source_plan_hash = _rofc_hash(
        _rofc_get(source, "plan_sha256", "slice.identity.source"),
        "slice.identity.source.plan_sha256")
    source_manifest_hash = _rofc_hash(
        _rofc_get(source, "manifest_sha256", "slice.identity.source"),
        "slice.identity.source.manifest_sha256")
    axis_ids = _rofc_ids(
        _rofc_get(identity, "source_axis_ids", "slice.identity"),
        "slice.identity.source_axis_ids", 3, 4)
    output_ids = _rofc_ids(
        _rofc_get(identity, "output_ids", "slice.identity"),
        "slice.identity.output_ids", 1, 4)

    free_raw = _rofc_array(
        _rofc_get(identity, "free_axes", "slice.identity"),
        "slice.identity.free_axes")
    length(free_raw) == 2 || _rofs_error(
        :invalid_free_axes, "slice identity must contain two free axes")
    free_axes = Dict{String,Any}[]
    free_positions = Int[]
    for (axis_index, raw_axis) in enumerate(free_raw)
        path = "slice.identity.free_axes[$(axis_index)]"
        axis = _rofc_exact_keys(raw_axis,
            ("source_axis_index", "axis_id", "coordinates"), path)
        position = _rofc_int(
            _rofc_get(axis, "source_axis_index", path),
            "$(path).source_axis_index"; minimum=1)
        position <= length(axis_ids) || _rofs_error(
            :axis_index_mismatch, "free source-axis index is out of range")
        axis_id = _rofc_identifier(
            _rofc_get(axis, "axis_id", path), "$(path).axis_id")
        axis_ids[position] == axis_id || _rofs_error(
            :axis_index_mismatch,
            "free axis ID does not match its source-axis index")
        coordinates = Float64[
            _rofc_finite(value, "$(path).coordinates[]")
            for value in _rofc_array(
                _rofc_get(axis, "coordinates", path),
                "$(path).coordinates")
        ]
        isempty(coordinates) && _rofs_error(
            :invalid_grid_axis, "free-axis coordinates cannot be empty")
        all(coordinates[index] < coordinates[index + 1]
            for index in 1:(length(coordinates) - 1)) || _rofs_error(
                :invalid_grid_axis,
                "free-axis coordinates must be strictly increasing")
        push!(free_positions, position)
        push!(free_axes, Dict{String,Any}(
            "source_axis_index" => position,
            "axis_id" => axis_id,
            "coordinates" => coordinates,
        ))
    end
    allunique(free_positions) || _rofs_error(
        :duplicate_free_axis, "slice free axes must be distinct")

    fixed_raw = _rofc_array(
        _rofc_get(identity, "fixed_axes", "slice.identity"),
        "slice.identity.fixed_axes")
    length(fixed_raw) == length(axis_ids) - 2 || _rofs_error(
        :incomplete_fixed_axes,
        "slice identity must fix every non-free source axis")
    fixed_axes = Dict{String,Any}[]
    fixed_by_position = Dict{Int,Float64}()
    for (fixed_index, raw_fixed) in enumerate(fixed_raw)
        path = "slice.identity.fixed_axes[$(fixed_index)]"
        fixed = _rofc_exact_keys(raw_fixed, (
            "source_axis_index", "axis_id", "coordinate_index",
            "coordinate",
        ), path)
        position = _rofc_int(
            _rofc_get(fixed, "source_axis_index", path),
            "$(path).source_axis_index"; minimum=1)
        position <= length(axis_ids) || _rofs_error(
            :axis_index_mismatch, "fixed source-axis index is out of range")
        position in free_positions && _rofs_error(
            :free_axis_fixed, "one source axis is both free and fixed")
        haskey(fixed_by_position, position) && _rofs_error(
            :duplicate_fixed_axis, "one source axis is fixed more than once")
        axis_id = _rofc_identifier(
            _rofc_get(fixed, "axis_id", path), "$(path).axis_id")
        axis_ids[position] == axis_id || _rofs_error(
            :axis_index_mismatch,
            "fixed axis ID does not match its source-axis index")
        coordinate_index = _rofc_int(
            _rofc_get(fixed, "coordinate_index", path),
            "$(path).coordinate_index"; minimum=1)
        coordinate = _rofc_finite(
            _rofc_get(fixed, "coordinate", path), "$(path).coordinate")
        fixed_by_position[position] = coordinate
        push!(fixed_axes, Dict{String,Any}(
            "source_axis_index" => position,
            "axis_id" => axis_id,
            "coordinate_index" => coordinate_index,
            "coordinate" => coordinate,
        ))
    end
    fixed_positions = getindex.(fixed_axes, "source_axis_index")
    fixed_positions == sort!(setdiff(
        collect(eachindex(axis_ids)), free_positions)) || _rofs_error(
            :noncanonical_fixed_axes,
            "fixed axes must be ordered by source-axis index")

    shape = Int[
        _rofc_int(value, "slice.identity.shape[]"; minimum=1)
        for value in _rofc_array(
            _rofc_get(identity, "shape", "slice.identity"),
            "slice.identity.shape")
    ]
    shape == length.(getindex.(free_axes, "coordinates")) || _rofs_error(
        :slice_shape_mismatch,
        "slice shape must equal the ordered free-axis lengths")
    expected_count_big = BigInt(shape[1]) * BigInt(shape[2])
    _rofs_limit(:slice_points, expected_count_big,
        slice_limits.max_slice_points)
    expected_count_big == BigInt(raw_point_count) || _rofs_error(
        :slice_count_mismatch,
        "slice point_count is not the Cartesian shape product")
    scalar_count = expected_count_big *
        (BigInt(2 + length(axis_ids)) + BigInt(length(output_ids)) * BigInt(3))
    _rofs_limit(:slice_scalars, scalar_count,
        slice_limits.max_slice_scalars)

    coverage = _rofc_exact_keys(
        _rofc_get(identity, "coverage", "slice.identity"), (
            "source_point_count", "expected_slice_point_count",
            "selected_point_count", "valid_count", "invalid_count",
            "complete",
        ), "slice.identity.coverage")
    source_point_count = _rofc_int(
        _rofc_get(coverage, "source_point_count", "slice.identity.coverage"),
        "slice.identity.coverage.source_point_count"; minimum=1)
    _rofs_limit(:source_points, BigInt(source_point_count),
        slice_limits.max_source_points)
    expected_count = Int(expected_count_big)
    _rofc_int(_rofc_get(coverage, "expected_slice_point_count",
        "slice.identity.coverage"),
        "slice.identity.coverage.expected_slice_point_count"; minimum=1) ==
        expected_count || _rofs_error(:slice_count_mismatch,
            "coverage expected count differs from slice shape")
    _rofc_int(_rofc_get(coverage, "selected_point_count",
        "slice.identity.coverage"),
        "slice.identity.coverage.selected_point_count"; minimum=1) ==
        expected_count || _rofs_error(:incomplete_slice_coverage,
            "coverage selected count is incomplete")
    _rofc_get(coverage, "complete", "slice.identity.coverage") === true ||
        _rofs_error(:incomplete_slice_coverage,
            "strict slice coverage must be complete")

    sample_order = Int[
        _rofc_int(value, "slice.identity.sample_order[]"; minimum=1)
        for value in _rofc_array(
            _rofc_get(identity, "sample_order", "slice.identity"),
            "slice.identity.sample_order")
    ]
    length(sample_order) == expected_count && allunique(sample_order) ||
        _rofs_error(:sample_order_mismatch,
            "slice source sample order must be complete and unique")
    all(index <= source_point_count for index in sample_order) ||
        _rofs_error(:source_point_out_of_range,
            "slice sample order references a point outside the source")

    samples_raw = _rofc_array(
        _rofc_get(identity, "samples", "slice.identity"),
        "slice.identity.samples")
    length(samples_raw) == expected_count || _rofs_error(
        :slice_count_mismatch, "slice samples do not cover its shape")
    samples = Dict{String,Any}[]
    sizehint!(samples, expected_count)
    sample_index = 0
    for first_index in eachindex(free_axes[1]["coordinates"])
        for second_index in eachindex(free_axes[2]["coordinates"])
            sample_index += 1
            expected_point = Float64[
                free_axes[1]["coordinates"][first_index],
                free_axes[2]["coordinates"][second_index],
            ]
            normalized = _rofs_normalize_stored_sample(
                samples_raw[sample_index], sample_index, expected_point,
                length(axis_ids), length(output_ids), free_positions,
                fixed_by_position, source_point_count)
            normalized["source_point_index"] == sample_order[sample_index] ||
                _rofs_error(:sample_order_mismatch,
                    "sample_order differs from stored source point indices")
            push!(samples, normalized)
        end
    end
    valid_count = count(sample -> sample["status"] == "valid", samples)
    invalid_count = expected_count - valid_count
    _rofc_int(_rofc_get(coverage, "valid_count", "slice.identity.coverage"),
        "slice.identity.coverage.valid_count") == valid_count &&
        _rofc_int(_rofc_get(coverage, "invalid_count",
            "slice.identity.coverage"),
            "slice.identity.coverage.invalid_count") == invalid_count ||
        _rofs_error(:slice_count_mismatch,
            "coverage valid/invalid counts differ from slice samples")

    normalized_identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_SLICE_SCHEMA_VERSION,
        "algorithm_version" => RO_FIELD_SLICE_ALGORITHM_VERSION,
        "source" => Dict{String,Any}(
            "plan_sha256" => source_plan_hash,
            "manifest_sha256" => source_manifest_hash,
        ),
        "source_axis_ids" => axis_ids,
        "free_axes" => free_axes,
        "fixed_axes" => fixed_axes,
        "output_ids" => output_ids,
        "shape" => shape,
        "point_count" => expected_count,
        "coverage" => Dict{String,Any}(
            "source_point_count" => source_point_count,
            "expected_slice_point_count" => expected_count,
            "selected_point_count" => expected_count,
            "valid_count" => valid_count,
            "invalid_count" => invalid_count,
            "complete" => true,
        ),
        "sample_order" => sample_order,
        "value_origin" => "reused_exact",
        "interpolation" => "none",
        "samples" => samples,
    )
    supplied_hash = _rofc_hash(
        _rofc_get(artifact, "slice_sha256", "slice"),
        "slice.slice_sha256")
    supplied_hash == _rofc_sha256(normalized_identity) || _rofs_error(
        :slice_hash_mismatch,
        "slice identity does not match slice_sha256")
    _rofc_canonical_json(identity) == _rofc_canonical_json(normalized_identity) ||
        _rofs_error(:noncanonical_slice,
            "slice identity is not in canonical normalized form")
    normalized = Dict{String,Any}(
        "schema_version" => RO_FIELD_SLICE_SCHEMA_VERSION,
        "slice_sha256" => supplied_hash,
        "identity" => normalized_identity,
        "runtime_context" => _rofc_runtime_context(
            _rofc_get(artifact, "runtime_context", "slice")),
    )
    _rofs_limit(:slice_bytes, BigInt(length(_rofc_bytes(normalized))),
        slice_limits.max_slice_bytes)

    validated_source_plan = nothing
    if source_plan !== nothing
        plan = _rofs_plan_for_slice(source_plan, chunk_limits)
        validated_source_plan = plan
        plan["plan_sha256"] == source_plan_hash || _rofs_error(
            :foreign_source_plan,
            "supplied source plan does not match the slice identity")
        plan["identity"]["axis_ids"] == axis_ids &&
            plan["identity"]["output_ids"] == output_ids || _rofs_error(
                :foreign_source_plan,
                "source plan axis/output order differs from the slice")
        plan["identity"]["point_count"] == source_point_count || _rofs_error(
            :foreign_source_plan,
            "source plan point count differs from slice coverage")
        coordinates = plan["identity"]["axis_coordinates"]
        for axis in free_axes
            coordinates[axis["source_axis_index"]] == axis["coordinates"] ||
                _rofs_error(:foreign_source_plan,
                    "free-axis coordinates differ from the source plan")
        end
        for fixed in fixed_axes
            position = fixed["source_axis_index"]
            index = fixed["coordinate_index"]
            index <= length(coordinates[position]) &&
                coordinates[position][index] == fixed["coordinate"] ||
                _rofs_error(:foreign_source_plan,
                    "fixed coordinate/index differs from the source plan")
        end
        for sample in samples
            expected_source_point = _rofc_plan_point(
                plan, sample["source_point_index"])
            expected_source_point == sample["source_point"] || _rofs_error(
                :source_point_index_mismatch,
                "source_point_index does not map to the stored Cartesian source point")
        end
    end

    if full_source_context
        manifest, expected_units = _rofs_preflight_manifest(
            source_manifest, validated_source_plan, slice_limits,
            chunk_limits)
        manifest["manifest_sha256"] == source_manifest_hash || _rofs_error(
            :foreign_source_manifest,
            "supplied source manifest does not match the slice identity")
        chunks = _rofs_load_chunks(
            chunk_source, manifest, validated_source_plan, expected_units,
            slice_limits, chunk_limits, cancel_check)
        _rofs_verify_source_semantics!(
            normalized, validated_source_plan, chunks, chunk_limits,
            cancel_check)
    end
    return normalized
end
