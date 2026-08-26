const RO_COMPLETE_REGULAR_ROOT_CENSUS_VERSION =
    "bne-ro-complete-regular-root-census/v1.0.0"
const RO_REGULAR_ROOT_CENSUS_CELL_VERSION =
    "bne-ro-regular-root-census-cell/v1.0.0"
const RO_COMPLETE_REGULAR_ROOT_CENSUS_SCOPE =
    :complete_regular_root_population_inside_declared_affine_moving_domain

struct RORegularRootCensusLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::RORegularRootCensusLimitExceeded)
    print(
        io,
        "regular-root census limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct RORegularRootCensusRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::RORegularRootCensusRejected)
    print(io, "regular-root census rejected (", err.reason, "): ", err.detail)
end

@inline function _rorc_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount <= limit || throw(
        RORegularRootCensusLimitExceeded(phase, amount, limit))
    return nothing
end

"""Hard bounds for one exact regular-root census."""
struct RORegularRootCensusLimits
    max_patches::Int
    max_axis_breakpoints_per_state::Int
    max_cells::Int
    max_source_replay_interval_operations::Int
    max_analysis_interval_operations::Int

    function RORegularRootCensusLimits(
        max_patches::Int,
        max_axis_breakpoints_per_state::Int,
        max_cells::Int,
        max_source_replay_interval_operations::Int,
        max_analysis_interval_operations::Int,
    )
        max_patches > 0 || throw(ArgumentError(
            "max_patches must be positive"))
        max_axis_breakpoints_per_state >= 2 || throw(ArgumentError(
            "max_axis_breakpoints_per_state must be at least two"))
        max_cells > 0 || throw(ArgumentError(
            "max_cells must be positive"))
        max_source_replay_interval_operations > 0 || throw(ArgumentError(
            "max_source_replay_interval_operations must be positive"))
        max_analysis_interval_operations > 0 || throw(ArgumentError(
            "max_analysis_interval_operations must be positive"))
        return new(
            max_patches,
            max_axis_breakpoints_per_state,
            max_cells,
            max_source_replay_interval_operations,
            max_analysis_interval_operations,
        )
    end
end

function RORegularRootCensusLimits(;
    max_patches::Int=512,
    max_axis_breakpoints_per_state::Int=64,
    max_cells::Int=4096,
    max_source_replay_interval_operations::Int=50_000_000,
    max_analysis_interval_operations::Int=50_000_000,
)
    return RORegularRootCensusLimits(
        max_patches,
        max_axis_breakpoints_per_state,
        max_cells,
        max_source_replay_interval_operations,
        max_analysis_interval_operations,
    )
end

function _rorc_write_limits(io::IO, limits::RORegularRootCensusLimits)
    for value in (
        limits.max_patches,
        limits.max_axis_breakpoints_per_state,
        limits.max_cells,
        limits.max_source_replay_interval_operations,
        limits.max_analysis_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

struct _RORCValidatedToken end
const _RORC_VALIDATED_TOKEN = _RORCValidatedToken()

function _rorc_cell_sha256(
    linear_index::Int,
    grid_index::Tuple,
    remainder_box::Tuple,
    classification::Symbol,
    excluding_equation_index::Int,
    residual_enclosure::Tuple,
    patch_certificate_sha256::String,
)
    io = IOBuffer()
    _rors_write_token(io, RO_REGULAR_ROOT_CENSUS_CELL_VERSION)
    _rors_write_token(io, linear_index)
    _rors_write_token(io, length(grid_index))
    for index in grid_index
        _rors_write_token(io, index)
    end
    _rors_write_interval_vector(io, remainder_box)
    _rors_write_token(io, classification)
    _rors_write_token(io, excluding_equation_index)
    _rors_write_interval_vector(io, residual_enclosure)
    _rors_write_token(io, patch_certificate_sha256)
    return bytes2hex(SHA.sha256(take!(io)))
end

"""One exhaustive affine-remainder partition-cell decision."""
struct RORegularRootCensusCell
    version::String
    linear_index::Int
    grid_index::Tuple
    remainder_box::Tuple
    classification::Symbol
    excluding_equation_index::Int
    residual_enclosure::Tuple
    patch_certificate_sha256::String
    evidence_sha256::String

    function RORegularRootCensusCell(
        ::_RORCValidatedToken,
        version::String,
        linear_index::Int,
        grid_index::Tuple,
        remainder_box::Tuple,
        classification::Symbol,
        excluding_equation_index::Int,
        residual_enclosure::Tuple,
        patch_certificate_sha256::String,
        evidence_sha256::String,
    )
        version == RO_REGULAR_ROOT_CENSUS_CELL_VERSION ||
            throw(ArgumentError("regular-root census cell version mismatch"))
        linear_index > 0 || throw(ArgumentError(
            "regular-root census cell index must be positive"))
        !isempty(grid_index) && all(index -> index isa Int && index > 0,
            grid_index) || throw(ArgumentError(
            "regular-root census grid indices must be positive integers"))
        length(remainder_box) == length(grid_index) ||
            throw(DimensionMismatch(
                "regular-root census cell box does not match its grid index"))
        all(interval -> interval isa ROExactInterval, remainder_box) ||
            throw(ArgumentError(
                "regular-root census cell bounds must be exact intervals"))
        !isempty(residual_enclosure) &&
            all(interval -> interval isa ROExactInterval,
                residual_enclosure) || throw(ArgumentError(
            "regular-root census residuals must be exact intervals"))
        classification in (
            :unique_regular_root,
            :root_free_by_residual_exclusion,
        ) || throw(ArgumentError(
            "unknown regular-root census cell classification"))
        if classification == :unique_regular_root
            excluding_equation_index == 0 || throw(ArgumentError(
                "a unique-root cell cannot name an excluding equation"))
            _rors_validate_sha256(
                patch_certificate_sha256,
                "patch_certificate_sha256",
            )
            all(_rors_contains_zero, residual_enclosure) ||
                throw(ArgumentError(
                    "a unique-root cell residual enclosure must contain zero"))
        else
            1 <= excluding_equation_index <= length(residual_enclosure) ||
                throw(ArgumentError(
                    "a root-free cell must name one excluding equation"))
            isempty(patch_certificate_sha256) || throw(ArgumentError(
                "a root-free cell cannot bind a patch"))
            !_rors_contains_zero(
                residual_enclosure[excluding_equation_index]) ||
                throw(ArgumentError(
                    "the named root-free equation does not exclude zero"))
        end
        _rors_validate_sha256(evidence_sha256, "cell evidence_sha256")
        expected = _rorc_cell_sha256(
            linear_index,
            grid_index,
            remainder_box,
            classification,
            excluding_equation_index,
            residual_enclosure,
            patch_certificate_sha256,
        )
        evidence_sha256 == expected || throw(ArgumentError(
            "regular-root census cell hash mismatch"))
        return new(
            version,
            linear_index,
            grid_index,
            remainder_box,
            classification,
            excluding_equation_index,
            residual_enclosure,
            patch_certificate_sha256,
            evidence_sha256,
        )
    end
end

function _rorc_make_cell(
    linear_index::Int,
    grid_index::Tuple,
    remainder_box::Tuple,
    classification::Symbol,
    excluding_equation_index::Int,
    residual_enclosure::Tuple,
    patch_certificate_sha256::String,
)
    evidence_sha256 = _rorc_cell_sha256(
        linear_index,
        grid_index,
        remainder_box,
        classification,
        excluding_equation_index,
        residual_enclosure,
        patch_certificate_sha256,
    )
    return RORegularRootCensusCell(
        _RORC_VALIDATED_TOKEN,
        RO_REGULAR_ROOT_CENSUS_CELL_VERSION,
        linear_index,
        grid_index,
        remainder_box,
        classification,
        excluding_equation_index,
        residual_enclosure,
        patch_certificate_sha256,
        evidence_sha256,
    )
end

function _rorc_certificate_sha256(
    system_declaration_sha256::String,
    limits::RORegularRootCensusLimits,
    control_box::Tuple,
    control_reference::Tuple,
    state_reference::Tuple,
    predictor_slope::ROExactMatrix,
    remainder_axis_breaks::Tuple,
    declared_remainder_box::Tuple,
    declared_state_enclosure::Tuple,
    patch_certificate_sha256s::Tuple,
    cells::Tuple,
    root_count_per_control::Int,
    regular_sheet_count::Int,
    partition_cell_count::Int,
    unique_root_cell_count::Int,
    root_free_cell_count::Int,
    source_replay_interval_operation_count::Int,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    _rors_write_token(io, RO_COMPLETE_REGULAR_ROOT_CENSUS_VERSION)
    _rors_write_token(io, system_declaration_sha256)
    _rorc_write_limits(io, limits)
    _rors_write_interval_vector(io, control_box)
    _rors_write_exact_vector(io, control_reference)
    _rors_write_exact_vector(io, state_reference)
    _rors_write_exact_matrix(io, predictor_slope)
    _rors_write_token(io, length(remainder_axis_breaks))
    for axis in remainder_axis_breaks
        _rors_write_exact_vector(io, axis)
    end
    _rors_write_interval_vector(io, declared_remainder_box)
    _rors_write_interval_vector(io, declared_state_enclosure)
    _rors_write_token(io, length(patch_certificate_sha256s))
    for hash in patch_certificate_sha256s
        _rors_write_token(io, hash)
    end
    _rors_write_token(io, length(cells))
    for cell in cells
        _rors_write_token(io, cell.evidence_sha256)
    end
    for value in (
        root_count_per_control,
        regular_sheet_count,
        partition_cell_count,
        unique_root_cell_count,
        root_free_cell_count,
        source_replay_interval_operation_count,
        analysis_interval_operation_count,
    )
        _rors_write_token(io, value)
    end
    for value in (
        true,  # root_population_complete_inside_declared_domain
        true,  # regular_sheet_population_complete_inside_declared_domain
        true,  # continuation_complete_inside_declared_control_box
        true,  # fold_event_set_complete_inside_declared_domain
        0,     # fold_event_count
        true,  # all_roots_regular_inside_declared_domain
        false, # roots_outside_declared_domain_excluded
        false, # stable_root_population_complete
        false, # hopf_event_set_complete
        false, # global_continuation_certified
        false, # true_hysteresis_certified
        RO_COMPLETE_REGULAR_ROOT_CENSUS_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Complete P8s1a regular-root census inside one declared affine moving domain.

The result is complete only inside `p(u) + declared_remainder_box`. It does not
exclude roots outside that domain and does not classify stability or Hopf events.
"""
struct ROCompleteRegularRootCensus
    version::String
    system_declaration_sha256::String
    limits::RORegularRootCensusLimits
    control_box::Tuple
    control_reference::Tuple
    state_reference::Tuple
    predictor_slope::ROExactMatrix
    remainder_axis_breaks::Tuple
    declared_remainder_box::Tuple
    declared_state_enclosure::Tuple
    patch_certificate_sha256s::Tuple
    cells::Tuple
    root_count_per_control::Int
    regular_sheet_count::Int
    partition_cell_count::Int
    unique_root_cell_count::Int
    root_free_cell_count::Int
    source_replay_interval_operation_count::Int
    analysis_interval_operation_count::Int
    root_population_complete_inside_declared_domain::Bool
    regular_sheet_population_complete_inside_declared_domain::Bool
    continuation_complete_inside_declared_control_box::Bool
    fold_event_set_complete_inside_declared_domain::Bool
    fold_event_count::Int
    all_roots_regular_inside_declared_domain::Bool
    roots_outside_declared_domain_excluded::Bool
    stable_root_population_complete::Bool
    hopf_event_set_complete::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROCompleteRegularRootCensus(
        ::_RORCValidatedToken,
        version::String,
        system_declaration_sha256::String,
        limits::RORegularRootCensusLimits,
        control_box::Tuple,
        control_reference::Tuple,
        state_reference::Tuple,
        predictor_slope::ROExactMatrix,
        remainder_axis_breaks::Tuple,
        declared_remainder_box::Tuple,
        declared_state_enclosure::Tuple,
        patch_certificate_sha256s::Tuple,
        cells::Tuple,
        root_count_per_control::Int,
        regular_sheet_count::Int,
        partition_cell_count::Int,
        unique_root_cell_count::Int,
        root_free_cell_count::Int,
        source_replay_interval_operation_count::Int,
        analysis_interval_operation_count::Int,
        root_population_complete_inside_declared_domain::Bool,
        regular_sheet_population_complete_inside_declared_domain::Bool,
        continuation_complete_inside_declared_control_box::Bool,
        fold_event_set_complete_inside_declared_domain::Bool,
        fold_event_count::Int,
        all_roots_regular_inside_declared_domain::Bool,
        roots_outside_declared_domain_excluded::Bool,
        stable_root_population_complete::Bool,
        hopf_event_set_complete::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_COMPLETE_REGULAR_ROOT_CENSUS_VERSION ||
            throw(ArgumentError("complete regular-root census version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        _rors_validate_sha256(certificate_sha256, "certificate_sha256")
        evidence_scope == RO_COMPLETE_REGULAR_ROOT_CENSUS_SCOPE ||
            throw(ArgumentError("regular-root census evidence scope mismatch"))
        root_population_complete_inside_declared_domain ||
            throw(ArgumentError("regular-root population completeness was lost"))
        regular_sheet_population_complete_inside_declared_domain ||
            throw(ArgumentError("regular-sheet population completeness was lost"))
        continuation_complete_inside_declared_control_box ||
            throw(ArgumentError("declared-box continuation completeness was lost"))
        fold_event_set_complete_inside_declared_domain ||
            throw(ArgumentError("declared-domain fold completeness was lost"))
        fold_event_count == 0 || throw(ArgumentError(
            "P8s1a admits only a complete empty fold-event set"))
        all_roots_regular_inside_declared_domain || throw(ArgumentError(
            "P8s1a requires every in-domain root to be regular"))
        roots_outside_declared_domain_excluded && throw(ArgumentError(
            "P8s1a cannot exclude roots outside its declared domain"))
        stable_root_population_complete && throw(ArgumentError(
            "P8s1a does not classify the complete stable-root population"))
        hopf_event_set_complete && throw(ArgumentError(
            "P8s1a does not certify a complete Hopf-event set"))
        global_continuation_certified && throw(ArgumentError(
            "P8s1a is not a global continuation certificate"))
        true_hysteresis_certified && throw(ArgumentError(
            "a static P8s1a census cannot certify true hysteresis"))
        state_count = length(state_reference)
        control_count = length(control_reference)
        length(control_box) == control_count || throw(DimensionMismatch(
            "census control box does not match its reference"))
        size(predictor_slope) == (state_count, control_count) ||
            throw(DimensionMismatch(
                "census predictor slope has the wrong shape"))
        length(remainder_axis_breaks) == state_count ||
            throw(DimensionMismatch(
                "census remainder axes do not match the state count"))
        length(declared_remainder_box) == state_count &&
            length(declared_state_enclosure) == state_count ||
            throw(DimensionMismatch(
                "census declared state domain has the wrong dimension"))
        cell_count_big = BigInt(1)
        for state in 1:state_count
            axis = remainder_axis_breaks[state]
            length(axis) >= 2 || throw(ArgumentError(
                "each census remainder axis needs two breakpoints"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "census remainder breakpoints must be strictly increasing"))
            declared_remainder_box[state].lower == first(axis) &&
                declared_remainder_box[state].upper == last(axis) ||
                throw(ArgumentError(
                    "declared remainder bounds do not match their axis"))
            declared_state_enclosure[state].lower > 0 ||
                throw(ArgumentError(
                    "the declared census state domain must stay positive"))
            cell_count_big *= length(axis) - 1
        end
        _rorc_limit(:cells, cell_count_big, limits.max_cells)
        partition_cell_count == cell_count_big || throw(ArgumentError(
            "regular-root census partition count mismatch"))
        length(cells) == partition_cell_count || throw(ArgumentError(
            "regular-root census cell population is incomplete"))
        all(cell -> cell isa RORegularRootCensusCell, cells) ||
            throw(ArgumentError(
                "regular-root census contains an invalid cell record"))
        cell_dimensions = Tuple(length(axis) - 1
            for axis in remainder_axis_breaks)
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(cell_dimensions))
            cell = cells[linear_index]
            cell.linear_index == linear_index || throw(ArgumentError(
                "regular-root census cell order is not canonical"))
            cell.grid_index == Tuple(cartesian_index) || throw(ArgumentError(
                "regular-root census grid index is not canonical"))
            length(cell.residual_enclosure) == state_count ||
                throw(DimensionMismatch(
                    "regular-root census cell residual dimension mismatch"))
            for state in 1:state_count
                axis = remainder_axis_breaks[state]
                index = cell.grid_index[state]
                interval = cell.remainder_box[state]
                interval.lower == axis[index] &&
                    interval.upper == axis[index + 1] ||
                    throw(ArgumentError(
                        "regular-root census cell bounds do not match their axis"))
            end
            expected_cell_hash = _rorc_cell_sha256(
                cell.linear_index,
                cell.grid_index,
                cell.remainder_box,
                cell.classification,
                cell.excluding_equation_index,
                cell.residual_enclosure,
                cell.patch_certificate_sha256,
            )
            cell.evidence_sha256 == expected_cell_hash ||
                throw(ArgumentError(
                    "regular-root census contains a forged cell"))
        end
        unique_cells = Tuple(cell for cell in cells if
            cell.classification == :unique_regular_root)
        free_cells = Tuple(cell for cell in cells if
            cell.classification == :root_free_by_residual_exclusion)
        unique_root_cell_count == length(unique_cells) ||
            throw(ArgumentError("unique-root cell count mismatch"))
        root_free_cell_count == length(free_cells) ||
            throw(ArgumentError("root-free cell count mismatch"))
        unique_root_cell_count + root_free_cell_count ==
            partition_cell_count || throw(ArgumentError(
            "regular-root census classifications are not exhaustive"))
        issorted(patch_certificate_sha256s) &&
            length(unique(patch_certificate_sha256s)) ==
                length(patch_certificate_sha256s) || throw(ArgumentError(
            "census patch hashes must be unique and sorted"))
        all(hash -> hash isa String, patch_certificate_sha256s) ||
            throw(ArgumentError("census patch hashes must be strings"))
        for hash in patch_certificate_sha256s
            _rors_validate_sha256(hash, "patch_certificate_sha256")
        end
        cell_patch_hashes = Tuple(sort!([
            cell.patch_certificate_sha256 for cell in unique_cells]))
        cell_patch_hashes == patch_certificate_sha256s ||
            throw(ArgumentError(
                "census patch population does not match unique-root cells"))
        root_count_per_control == unique_root_cell_count &&
            regular_sheet_count == unique_root_cell_count ||
            throw(ArgumentError(
                "census root/sheet counts do not match patch cells"))
        source_replay_interval_operation_count >= 0 &&
            analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "regular-root census operation counts must be nonnegative"))
        _rorc_limit(
            :source_replay_interval_operations,
            source_replay_interval_operation_count,
            limits.max_source_replay_interval_operations,
        )
        _rorc_limit(
            :analysis_interval_operations,
            analysis_interval_operation_count,
            limits.max_analysis_interval_operations,
        )
        expected = _rorc_certificate_sha256(
            system_declaration_sha256,
            limits,
            control_box,
            control_reference,
            state_reference,
            predictor_slope,
            remainder_axis_breaks,
            declared_remainder_box,
            declared_state_enclosure,
            patch_certificate_sha256s,
            cells,
            root_count_per_control,
            regular_sheet_count,
            partition_cell_count,
            unique_root_cell_count,
            root_free_cell_count,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "complete regular-root census hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            limits,
            control_box,
            control_reference,
            state_reference,
            predictor_slope,
            remainder_axis_breaks,
            declared_remainder_box,
            declared_state_enclosure,
            patch_certificate_sha256s,
            cells,
            root_count_per_control,
            regular_sheet_count,
            partition_cell_count,
            unique_root_cell_count,
            root_free_cell_count,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
            true,
            true,
            true,
            true,
            0,
            true,
            false,
            false,
            false,
            false,
            false,
            evidence_scope,
            certificate_sha256,
        )
    end
end

function _rorc_regular_limits_with_operation_cap(
    limits::RORegularSheetLimits,
    operation_cap::Int,
)
    return RORegularSheetLimits(
        limits.max_states,
        limits.max_controls,
        limits.max_terms_per_equation,
        limits.max_total_terms,
        limits.max_expanded_terms,
        limits.max_total_degree,
        limits.max_metadata_bytes,
        limits.max_exact_operand_bits,
        operation_cap,
    )
end

function _rorc_admit_patches(patches, limits::RORegularRootCensusLimits)
    patches isa AbstractVector || patches isa Tuple || throw(ArgumentError(
        "patches must be an ordered vector or tuple"))
    _rorc_limit(:patches, length(patches), limits.max_patches)
    admitted = RORegularSheetPatchCertificate[]
    sizehint!(admitted, length(patches))
    for patch in patches
        patch isa RORegularSheetPatchCertificate || throw(ArgumentError(
            "patches must contain RORegularSheetPatchCertificate values"))
        push!(admitted, patch)
    end
    sort!(admitted; by=patch -> patch.certificate_sha256)
    hashes = [patch.certificate_sha256 for patch in admitted]
    length(unique(hashes)) == length(hashes) || throw(ArgumentError(
        "regular-root census patches must be unique"))
    return admitted
end

function _rorc_parse_axis_breaks(
    raw,
    state_count::Int,
    limits::RORegularRootCensusLimits,
    context::_RORSContext,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "remainder_axis_breaks must be an ordered vector or tuple"))
    length(raw) == state_count || throw(DimensionMismatch(
        "remainder_axis_breaks must match the state count"))
    axes = Vector{Tuple}(undef, state_count)
    cell_count = BigInt(1)
    for state in 1:state_count
        context.cancel_check()
        axis = raw[state]
        axis isa AbstractVector || axis isa Tuple || throw(ArgumentError(
            "each remainder-axis declaration must be a vector or tuple"))
        length(axis) >= 2 || throw(ArgumentError(
            "each remainder axis requires at least two breakpoints"))
        _rorc_limit(
            :axis_breakpoints_per_state,
            length(axis),
            limits.max_axis_breakpoints_per_state,
        )
        admitted = Vector{_RORSExact}(undef, length(axis))
        for index in eachindex(axis)
            _rors_tick!(context)
            admitted[index] = _rors_exact_float(
                axis[index], context.limits, "remainder_axis_breaks")
        end
        all(index -> admitted[index] < admitted[index + 1],
            1:(length(admitted) - 1)) || throw(ArgumentError(
            "remainder-axis breakpoints must be strictly increasing"))
        axes[state] = Tuple(admitted)
        cell_count *= length(admitted) - 1
        _rorc_limit(:cells, cell_count, limits.max_cells)
    end
    return Tuple(axes)
end

function _rorc_replay_patches(
    system::ROPolynomialEquilibriumSystem,
    patches::Vector{RORegularSheetPatchCertificate},
    limits::RORegularRootCensusLimits,
    cancel_check,
)
    expected_operations = sum(
        (BigInt(patch.exact_operation_count) for patch in patches);
        init=BigInt(0),
    )
    _rorc_limit(
        :source_replay_interval_operations,
        expected_operations,
        limits.max_source_replay_interval_operations,
    )
    context_limits = _rorc_regular_limits_with_operation_cap(
        system.limits,
        limits.max_source_replay_interval_operations,
    )
    context = _RORSContext(context_limits, cancel_check)
    rebuilt = RORegularSheetPatchCertificate[]
    sizehint!(rebuilt, length(patches))
    try
        for patch in patches
            context.cancel_check()
            patch.system_declaration_sha256 == system.declaration_sha256 ||
                throw(ArgumentError(
                    "regular-root census patch belongs to another system"))
            patch.limits == system.limits || throw(ArgumentError(
                "regular-root census patch limits do not match the system"))
            replayed = _rors_certify_patch_exact(
                system,
                collect(patch.control_box),
                collect(patch.control_reference),
                collect(patch.state_reference),
                _rors_exact_matrix_values(patch.predictor_slope),
                collect(patch.remainder_box),
                _rors_exact_matrix_values(patch.preconditioner),
                context,
            )
            replayed.certificate_sha256 == patch.certificate_sha256 ||
                throw(ArgumentError(
                    "regular-root census patch replay changed its hash"))
            push!(rebuilt, replayed)
        end
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(RORegularRootCensusLimitExceeded(
                :source_replay_interval_operations,
                err.requested,
                limits.max_source_replay_interval_operations,
            ))
        end
        rethrow()
    end
    context.operations == expected_operations || throw(ArgumentError(
        "regular-root census source replay operation count changed"))
    context.operations <= typemax(Int) || throw(
        RORegularRootCensusLimitExceeded(
            :source_replay_interval_operations,
            context.operations,
            limits.max_source_replay_interval_operations,
        ))
    return rebuilt, Int(context.operations)
end

function _rorc_predictor_intercept(
    state_reference,
    control_reference,
    predictor_slope::Matrix{_RORSExact},
    context::_RORSContext,
)
    state_count, control_count = size(predictor_slope)
    intercept = Vector{_RORSExact}(undef, state_count)
    for state in 1:state_count
        value = state_reference[state]
        for control in 1:control_count
            value = _rors_exact_subtract(
                context,
                value,
                _rors_exact_multiply(
                    context,
                    predictor_slope[state, control],
                    control_reference[control],
                ),
            )
        end
        intercept[state] = value
    end
    return intercept
end

function _rorc_patch_grid_index(
    patch::RORegularSheetPatchCertificate,
    control_box::Vector{ROExactInterval},
    global_intercept::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    remainder_axis_breaks::Tuple,
    context::_RORSContext,
)
    Tuple(control_box) == patch.control_box || throw(
        RORegularRootCensusRejected(
            :patch_control_box_mismatch,
            "every P8s1a patch must span the complete census control box",
        ))
    patch_slope = _rors_exact_matrix_values(patch.predictor_slope)
    patch_slope == predictor_slope || throw(
        RORegularRootCensusRejected(
            :patch_predictor_slope_mismatch,
            "every P8s1a patch must use the census affine slope coordinate",
        ))
    patch_intercept = _rorc_predictor_intercept(
        patch.state_reference,
        patch.control_reference,
        patch_slope,
        context,
    )
    grid_index = Vector{Int}(undef, length(global_intercept))
    for state in eachindex(global_intercept)
        offset = _rors_exact_subtract(
            context, patch_intercept[state], global_intercept[state])
        lower = _rors_exact_add(
            context, patch.remainder_box[state].lower, offset)
        upper = _rors_exact_add(
            context, patch.remainder_box[state].upper, offset)
        axis = remainder_axis_breaks[state]
        cell_index = findfirst(index ->
            axis[index] == lower && axis[index + 1] == upper,
            1:(length(axis) - 1),
        )
        cell_index === nothing && throw(
            RORegularRootCensusRejected(
                :patch_tube_not_a_partition_cell,
                "patch $(patch.certificate_sha256) does not map exactly onto one remainder cell",
            ))
        grid_index[state] = cell_index
    end
    return Tuple(grid_index)
end

function _rorc_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    patches::Vector{RORegularSheetPatchCertificate},
    control_box::Vector{ROExactInterval},
    control_reference::Vector{_RORSExact},
    state_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    remainder_axis_breaks::Tuple,
    limits::RORegularRootCensusLimits,
    cancel_check,
)
    validate_ro_polynomial_equilibrium_system(system)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    length(control_box) == control_count || throw(DimensionMismatch(
        "census control box does not match the system"))
    length(control_reference) == control_count || throw(DimensionMismatch(
        "census control reference does not match the system"))
    length(state_reference) == state_count || throw(DimensionMismatch(
        "census state reference does not match the system"))
    size(predictor_slope) == (state_count, control_count) ||
        throw(DimensionMismatch("census predictor slope has the wrong shape"))
    length(remainder_axis_breaks) == state_count ||
        throw(DimensionMismatch("census remainder axes have the wrong shape"))
    rebuilt_patches, source_operations = _rorc_replay_patches(
        system, patches, limits, cancel_check)
    context_limits = _rorc_regular_limits_with_operation_cap(
        system.limits,
        limits.max_analysis_interval_operations,
    )
    context = _RORSContext(context_limits, cancel_check)
    cells = RORegularRootCensusCell[]
    try
        for control in 1:control_count
            interval = control_box[control]
            interval.lower > 0 || throw(RORegularRootCensusRejected(
                :nonpositive_control_box,
                "control coordinate $control is not strictly positive",
            ))
            interval.lower < interval.upper || throw(
                RORegularRootCensusRejected(
                    :degenerate_control_box,
                    "control coordinate $control must have positive width",
                ))
            interval.lower <= control_reference[control] <= interval.upper ||
                throw(RORegularRootCensusRejected(
                    :control_reference_outside_box,
                    "control reference $control lies outside its box",
                ))
        end
        declared_remainder_box = Vector{ROExactInterval}(undef, state_count)
        cell_dimensions = Vector{Int}(undef, state_count)
        cell_count_big = BigInt(1)
        for state in 1:state_count
            axis = remainder_axis_breaks[state]
            length(axis) >= 2 || throw(ArgumentError(
                "each census remainder axis needs two breakpoints"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "census remainder breakpoints must be strictly increasing"))
            declared_remainder_box[state] = _rors_interval(
                context, first(axis), last(axis))
            cell_dimensions[state] = length(axis) - 1
            cell_count_big *= cell_dimensions[state]
            _rorc_limit(:cells, cell_count_big, limits.max_cells)
        end
        predictor_state = _rors_affine_enclosure(
            state_reference,
            control_reference,
            predictor_slope,
            control_box,
            context,
        )
        declared_state_enclosure = Vector{ROExactInterval}(
            undef, state_count)
        for state in 1:state_count
            declared_state_enclosure[state] = _rors_add(
                context,
                predictor_state[state],
                declared_remainder_box[state],
            )
            declared_state_enclosure[state].lower > 0 || throw(
                RORegularRootCensusRejected(
                    :nonpositive_declared_state_domain,
                    "declared state domain $state leaves positive coordinates",
                ))
        end

        global_intercept = _rorc_predictor_intercept(
            state_reference,
            control_reference,
            predictor_slope,
            context,
        )
        patch_by_grid_index = Dict{Tuple,RORegularSheetPatchCertificate}()
        for patch in rebuilt_patches
            context.cancel_check()
            grid_index = _rorc_patch_grid_index(
                patch,
                control_box,
                global_intercept,
                predictor_slope,
                remainder_axis_breaks,
                context,
            )
            haskey(patch_by_grid_index, grid_index) && throw(
                RORegularRootCensusRejected(
                    :multiple_patches_in_partition_cell,
                    "more than one regular patch maps to cell $grid_index",
                ))
            patch_by_grid_index[grid_index] = patch
        end

        state_polynomials = _rors_affine_tube_polynomials(
            state_reference,
            control_reference,
            predictor_slope,
            context,
        )
        variable_count = control_count + state_count
        composed_equations = Vector{_RORSPolynomial}(undef, state_count)
        for equation in 1:state_count
            context.cancel_check()
            composed_equations[equation] = _rors_compose_equation(
                system.equations[equation],
                state_polynomials,
                control_count,
                variable_count,
                context,
            )
        end

        sizehint!(cells, Int(cell_count_big))
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(Tuple(cell_dimensions)))
            context.cancel_check()
            grid_index = Tuple(cartesian_index)
            remainder_box = Vector{ROExactInterval}(undef, state_count)
            for state in 1:state_count
                axis = remainder_axis_breaks[state]
                index = grid_index[state]
                remainder_box[state] = _rors_interval(
                    context, axis[index], axis[index + 1])
            end
            variable_box = vcat(control_box, remainder_box)
            residual = Vector{ROExactInterval}(undef, state_count)
            for equation in 1:state_count
                residual[equation] = _rors_evaluate_polynomial(
                    composed_equations[equation],
                    variable_box,
                    context,
                )
            end
            patch = get(patch_by_grid_index, grid_index, nothing)
            if patch === nothing
                excluding_equation = findfirst(
                    interval -> !_rors_contains_zero(interval), residual)
                excluding_equation === nothing && throw(
                    RORegularRootCensusRejected(
                        :unresolved_partition_cell,
                        "partition cell $grid_index neither excludes a root nor binds one regular patch",
                    ))
                push!(cells, _rorc_make_cell(
                    linear_index,
                    grid_index,
                    Tuple(remainder_box),
                    :root_free_by_residual_exclusion,
                    excluding_equation,
                    Tuple(residual),
                    "",
                ))
            else
                all(_rors_contains_zero, residual) || throw(ArgumentError(
                    "a replayed patch root is outside its census residual enclosure"))
                push!(cells, _rorc_make_cell(
                    linear_index,
                    grid_index,
                    Tuple(remainder_box),
                    :unique_regular_root,
                    0,
                    Tuple(residual),
                    patch.certificate_sha256,
                ))
            end
        end
        length(patch_by_grid_index) == length(rebuilt_patches) ||
            throw(ArgumentError(
                "not every replayed patch entered the census partition"))
        context.cancel_check()
        context.operations <= typemax(Int) || throw(
            RORegularRootCensusLimitExceeded(
                :analysis_interval_operations,
                context.operations,
                limits.max_analysis_interval_operations,
            ))
        analysis_operations = Int(context.operations)
        patch_hashes = Tuple(
            patch.certificate_sha256 for patch in rebuilt_patches)
        cell_tuple = Tuple(cells)
        unique_count = count(cell ->
            cell.classification == :unique_regular_root, cells)
        free_count = length(cells) - unique_count
        predictor_wrapper = _rors_exact_matrix_wrapper(predictor_slope)
        control_box_tuple = Tuple(control_box)
        control_reference_tuple = Tuple(control_reference)
        state_reference_tuple = Tuple(state_reference)
        declared_remainder_tuple = Tuple(declared_remainder_box)
        declared_state_tuple = Tuple(declared_state_enclosure)
        certificate_sha256 = _rorc_certificate_sha256(
            system.declaration_sha256,
            limits,
            control_box_tuple,
            control_reference_tuple,
            state_reference_tuple,
            predictor_wrapper,
            remainder_axis_breaks,
            declared_remainder_tuple,
            declared_state_tuple,
            patch_hashes,
            cell_tuple,
            unique_count,
            unique_count,
            length(cells),
            unique_count,
            free_count,
            source_operations,
            analysis_operations,
        )
        return ROCompleteRegularRootCensus(
            _RORC_VALIDATED_TOKEN,
            RO_COMPLETE_REGULAR_ROOT_CENSUS_VERSION,
            system.declaration_sha256,
            limits,
            control_box_tuple,
            control_reference_tuple,
            state_reference_tuple,
            predictor_wrapper,
            remainder_axis_breaks,
            declared_remainder_tuple,
            declared_state_tuple,
            patch_hashes,
            cell_tuple,
            unique_count,
            unique_count,
            length(cells),
            unique_count,
            free_count,
            source_operations,
            analysis_operations,
            true,
            true,
            true,
            true,
            0,
            true,
            false,
            false,
            false,
            false,
            false,
            RO_COMPLETE_REGULAR_ROOT_CENSUS_SCOPE,
            certificate_sha256,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(RORegularRootCensusLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
end

"""
    certify_ro_complete_regular_root_census(system, patches; ...)

Exhaustively classify a dyadic tensor partition of one affine moving state
domain. Every cell must either exclude zero in one residual component or equal
one fully replayed P5r0.1 root tube.
"""
function certify_ro_complete_regular_root_census(
    system::ROPolynomialEquilibriumSystem,
    patches;
    control_lower,
    control_upper,
    control_reference,
    state_reference,
    predictor_slope,
    remainder_axis_breaks,
    limits::RORegularRootCensusLimits=RORegularRootCensusLimits(),
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    admitted_patches = _rorc_admit_patches(patches, limits)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    parse_limits = _rorc_regular_limits_with_operation_cap(
        system.limits,
        limits.max_analysis_interval_operations,
    )
    context = _RORSContext(parse_limits, cancel_check)
    try
        control_box = _rors_exact_box(
            control_lower,
            control_upper,
            control_count,
            "control",
            context,
        )
        admitted_control_reference = _rors_exact_vector(
            control_reference,
            control_count,
            "control_reference",
            context,
        )
        admitted_state_reference = _rors_exact_vector(
            state_reference,
            state_count,
            "state_reference",
            context,
        )
        admitted_slope = _rors_exact_matrix(
            predictor_slope,
            state_count,
            control_count,
            "predictor_slope",
            context,
        )
        admitted_axes = _rorc_parse_axis_breaks(
            remainder_axis_breaks,
            state_count,
            limits,
            context,
        )
        return _rorc_certify_exact(
            system,
            admitted_patches,
            control_box,
            admitted_control_reference,
            admitted_state_reference,
            admitted_slope,
            admitted_axes,
            limits,
            cancel_check,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(RORegularRootCensusLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
end

function replay_ro_complete_regular_root_census(
    system::ROPolynomialEquilibriumSystem,
    patches,
    certificate::ROCompleteRegularRootCensus;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "regular-root census belongs to a different polynomial system"))
    admitted_patches = _rorc_admit_patches(patches, certificate.limits)
    supplied_hashes = Tuple(
        patch.certificate_sha256 for patch in admitted_patches)
    supplied_hashes == certificate.patch_certificate_sha256s ||
        throw(ArgumentError(
            "regular-root census replay patch population mismatch"))
    rebuilt = _rorc_certify_exact(
        system,
        admitted_patches,
        collect(certificate.control_box),
        collect(certificate.control_reference),
        collect(certificate.state_reference),
        _rors_exact_matrix_values(certificate.predictor_slope),
        certificate.remainder_axis_breaks,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "complete regular-root census replay changed its hash"))
    return rebuilt
end

function validate_ro_complete_regular_root_census(
    system::ROPolynomialEquilibriumSystem,
    patches,
    certificate::ROCompleteRegularRootCensus;
    cancel_check=() -> nothing,
)
    replay_ro_complete_regular_root_census(
        system,
        patches,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
