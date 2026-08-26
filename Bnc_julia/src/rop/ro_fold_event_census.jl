const RO_SIMPLE_FOLD_EVENT_CENSUS_VERSION =
    "bne-ro-simple-fold-event-census/v1.0.0"
const RO_SIMPLE_FOLD_EVENT_VERSION =
    "bne-ro-simple-fold-event/v1.0.0"
const RO_FOLD_EVENT_CENSUS_CELL_VERSION =
    "bne-ro-fold-event-census-cell/v1.0.0"
const RO_SIMPLE_FOLD_EVENT_CENSUS_SCOPE =
    :complete_simple_fold_event_population_inside_declared_augmented_domain

struct ROFoldEventCensusLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFoldEventCensusLimitExceeded)
    print(
        io,
        "fold-event census limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct ROFoldEventCensusRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROFoldEventCensusRejected)
    print(io, "fold-event census rejected (", err.reason, "): ", err.detail)
end

@inline function _rofe_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "fold-event census requested work must be nonnegative"))
    amount <= limit || throw(
        ROFoldEventCensusLimitExceeded(phase, amount, limit))
    return nothing
end

"""Hard bounds for one exact augmented fold-event census."""
struct ROFoldEventCensusLimits
    max_events::Int
    max_axis_breakpoints_per_variable::Int
    max_cells::Int
    max_interval_operations::Int

    function ROFoldEventCensusLimits(
        max_events::Int,
        max_axis_breakpoints_per_variable::Int,
        max_cells::Int,
        max_interval_operations::Int,
    )
        max_events >= 0 || throw(ArgumentError(
            "max_events must be nonnegative"))
        max_axis_breakpoints_per_variable >= 2 || throw(ArgumentError(
            "max_axis_breakpoints_per_variable must be at least two"))
        max_cells > 0 || throw(ArgumentError(
            "max_cells must be positive"))
        max_interval_operations > 0 || throw(ArgumentError(
            "max_interval_operations must be positive"))
        return new(
            max_events,
            max_axis_breakpoints_per_variable,
            max_cells,
            max_interval_operations,
        )
    end
end

function ROFoldEventCensusLimits(;
    max_events::Integer=256,
    max_axis_breakpoints_per_variable::Integer=64,
    max_cells::Integer=4096,
    max_interval_operations::Integer=50_000_000,
)
    values = (
        max_events,
        max_axis_breakpoints_per_variable,
        max_cells,
        max_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("fold-event census limits must fit Int"))
    return ROFoldEventCensusLimits(Int.(values)...)
end

function _rofe_write_limits(io::IO, limits::ROFoldEventCensusLimits)
    for value in (
        limits.max_events,
        limits.max_axis_breakpoints_per_variable,
        limits.max_cells,
        limits.max_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

struct _ROFEValidatedToken end
const _ROFE_VALIDATED_TOKEN = _ROFEValidatedToken()

function _rofe_event_sha256(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    center::Tuple,
    preconditioner::ROExactMatrix,
    krawczyk_offset_image::Tuple,
    augmented_residual_enclosure::Tuple,
    augmented_jacobian_enclosure::ROExactIntervalMatrix,
    contraction_beta::_RORSExact,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SIMPLE_FOLD_EVENT_VERSION)
    _rors_write_token(io, linear_index)
    _rors_write_token(io, length(grid_index))
    for index in grid_index
        _rors_write_token(io, index)
    end
    _rors_write_interval_vector(io, event_box)
    _rors_write_exact_vector(io, center)
    _rors_write_exact_matrix(io, preconditioner)
    _rors_write_interval_vector(io, krawczyk_offset_image)
    _rors_write_interval_vector(io, augmented_residual_enclosure)
    _rors_write_interval_matrix(io, augmented_jacobian_enclosure)
    _rors_write_exact(io, contraction_beta)
    for value in (
        true, # unique_augmented_root_inside_event_box
        true, # state_jacobian_corank_one_certified
        true, # control_transversality_certified
        true, # quadratic_nondegeneracy_certified
        true, # simple_fold_certified
        true, # fold_point_strictly_inside_event_box
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
One isolated zero of `(F, det(F_x))` whose nonsingular augmented Jacobian
implies the standard one-control simple-fold conditions.
"""
struct ROSimpleFoldEvent
    version::String
    linear_index::Int
    grid_index::Tuple
    event_box::Tuple
    center::Tuple
    preconditioner::ROExactMatrix
    krawczyk_offset_image::Tuple
    augmented_residual_enclosure::Tuple
    augmented_jacobian_enclosure::ROExactIntervalMatrix
    contraction_beta::_RORSExact
    unique_augmented_root_inside_event_box::Bool
    state_jacobian_corank_one_certified::Bool
    control_transversality_certified::Bool
    quadratic_nondegeneracy_certified::Bool
    simple_fold_certified::Bool
    fold_point_strictly_inside_event_box::Bool
    certificate_sha256::String

    function ROSimpleFoldEvent(
        ::_ROFEValidatedToken,
        version::String,
        linear_index::Int,
        grid_index::Tuple,
        event_box::Tuple,
        center::Tuple,
        preconditioner::ROExactMatrix,
        krawczyk_offset_image::Tuple,
        augmented_residual_enclosure::Tuple,
        augmented_jacobian_enclosure::ROExactIntervalMatrix,
        contraction_beta::_RORSExact,
        unique_augmented_root_inside_event_box::Bool,
        state_jacobian_corank_one_certified::Bool,
        control_transversality_certified::Bool,
        quadratic_nondegeneracy_certified::Bool,
        simple_fold_certified::Bool,
        fold_point_strictly_inside_event_box::Bool,
        certificate_sha256::String,
    )
        version == RO_SIMPLE_FOLD_EVENT_VERSION || throw(ArgumentError(
            "simple-fold event version mismatch"))
        linear_index > 0 || throw(ArgumentError(
            "simple-fold event linear index must be positive"))
        variable_count = length(grid_index)
        variable_count > 1 || throw(ArgumentError(
            "a fold event requires at least one state and one control"))
        all(index -> index isa Int && index > 0, grid_index) ||
            throw(ArgumentError(
                "fold-event grid indices must be positive Int values"))
        length(event_box) == variable_count &&
            length(center) == variable_count &&
            length(krawczyk_offset_image) == variable_count &&
            length(augmented_residual_enclosure) == variable_count ||
            throw(DimensionMismatch(
                "simple-fold event vectors have inconsistent dimensions"))
        all(interval -> interval isa ROExactInterval, event_box) &&
            all(interval -> interval isa ROExactInterval,
                krawczyk_offset_image) &&
            all(interval -> interval isa ROExactInterval,
                augmented_residual_enclosure) || throw(ArgumentError(
            "simple-fold event enclosures must be exact intervals"))
        all(value -> value isa _RORSExact, center) || throw(ArgumentError(
            "simple-fold event center must be exact"))
        size(preconditioner) == (variable_count, variable_count) ||
            throw(DimensionMismatch(
                "simple-fold event preconditioner has the wrong shape"))
        size(augmented_jacobian_enclosure) ==
            (variable_count, variable_count) || throw(DimensionMismatch(
            "simple-fold event augmented Jacobian has the wrong shape"))
        for variable in 1:variable_count
            box = event_box[variable]
            box.lower > 0 || throw(ArgumentError(
                "simple-fold event boxes must stay strictly positive"))
            box.lower < box.upper || throw(ArgumentError(
                "simple-fold event boxes must have positive width"))
            center[variable] == (box.lower + box.upper) / 2 ||
                throw(ArgumentError(
                    "simple-fold event center is not the exact box midpoint"))
            centered_box = ROExactInterval(
                box.lower - center[variable],
                box.upper - center[variable],
                Val(:validated),
            )
            _rors_strict_subset(
                krawczyk_offset_image[variable], centered_box) ||
                throw(ArgumentError(
                    "simple-fold event Krawczyk image is not strictly interior"))
        end
        all(_rors_contains_zero, augmented_residual_enclosure) ||
            throw(ArgumentError(
                "simple-fold event residual enclosure must contain zero"))
        0 <= contraction_beta < 1 || throw(ArgumentError(
            "simple-fold event contraction beta must lie in [0,1)"))
        unique_augmented_root_inside_event_box || throw(ArgumentError(
            "simple-fold event lost augmented-root uniqueness"))
        state_jacobian_corank_one_certified || throw(ArgumentError(
            "simple-fold event lost the corank-one conclusion"))
        control_transversality_certified || throw(ArgumentError(
            "simple-fold event lost control transversality"))
        quadratic_nondegeneracy_certified || throw(ArgumentError(
            "simple-fold event lost quadratic nondegeneracy"))
        simple_fold_certified || throw(ArgumentError(
            "simple-fold event lost its simple-fold conclusion"))
        fold_point_strictly_inside_event_box || throw(ArgumentError(
            "simple-fold event root is not certified strictly interior"))
        _rors_validate_sha256(certificate_sha256, "certificate_sha256")
        expected = _rofe_event_sha256(
            linear_index,
            grid_index,
            event_box,
            center,
            preconditioner,
            krawczyk_offset_image,
            augmented_residual_enclosure,
            augmented_jacobian_enclosure,
            contraction_beta,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "simple-fold event hash mismatch"))
        return new(
            version,
            linear_index,
            grid_index,
            event_box,
            center,
            preconditioner,
            krawczyk_offset_image,
            augmented_residual_enclosure,
            augmented_jacobian_enclosure,
            contraction_beta,
            true,
            true,
            true,
            true,
            true,
            true,
            certificate_sha256,
        )
    end
end

function _rofe_make_event(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    center::Tuple,
    preconditioner::ROExactMatrix,
    krawczyk_offset_image::Tuple,
    augmented_residual_enclosure::Tuple,
    augmented_jacobian_enclosure::ROExactIntervalMatrix,
    contraction_beta::_RORSExact,
)
    hash = _rofe_event_sha256(
        linear_index,
        grid_index,
        event_box,
        center,
        preconditioner,
        krawczyk_offset_image,
        augmented_residual_enclosure,
        augmented_jacobian_enclosure,
        contraction_beta,
    )
    return ROSimpleFoldEvent(
        _ROFE_VALIDATED_TOKEN,
        RO_SIMPLE_FOLD_EVENT_VERSION,
        linear_index,
        grid_index,
        event_box,
        center,
        preconditioner,
        krawczyk_offset_image,
        augmented_residual_enclosure,
        augmented_jacobian_enclosure,
        contraction_beta,
        true,
        true,
        true,
        true,
        true,
        true,
        hash,
    )
end

function _rofe_cell_sha256(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    classification::Symbol,
    excluding_augmented_equation_index::Int,
    augmented_residual_enclosure::Tuple,
    event_certificate_sha256::String,
)
    io = IOBuffer()
    _rors_write_token(io, RO_FOLD_EVENT_CENSUS_CELL_VERSION)
    _rors_write_token(io, linear_index)
    _rors_write_token(io, length(grid_index))
    for index in grid_index
        _rors_write_token(io, index)
    end
    _rors_write_interval_vector(io, event_box)
    _rors_write_token(io, classification)
    _rors_write_token(io, excluding_augmented_equation_index)
    _rors_write_interval_vector(io, augmented_residual_enclosure)
    _rors_write_token(io, event_certificate_sha256)
    return bytes2hex(SHA.sha256(take!(io)))
end

"""One exhaustive tensor-cell decision for the augmented fold system."""
struct ROFoldEventCensusCell
    version::String
    linear_index::Int
    grid_index::Tuple
    event_box::Tuple
    classification::Symbol
    excluding_augmented_equation_index::Int
    augmented_residual_enclosure::Tuple
    event_certificate_sha256::String
    evidence_sha256::String

    function ROFoldEventCensusCell(
        ::_ROFEValidatedToken,
        version::String,
        linear_index::Int,
        grid_index::Tuple,
        event_box::Tuple,
        classification::Symbol,
        excluding_augmented_equation_index::Int,
        augmented_residual_enclosure::Tuple,
        event_certificate_sha256::String,
        evidence_sha256::String,
    )
        version == RO_FOLD_EVENT_CENSUS_CELL_VERSION || throw(ArgumentError(
            "fold-event census cell version mismatch"))
        linear_index > 0 || throw(ArgumentError(
            "fold-event census cell index must be positive"))
        variable_count = length(grid_index)
        variable_count > 1 &&
            all(index -> index isa Int && index > 0, grid_index) ||
            throw(ArgumentError(
                "fold-event census grid indices must be positive Int values"))
        length(event_box) == variable_count &&
            length(augmented_residual_enclosure) == variable_count ||
            throw(DimensionMismatch(
                "fold-event census cell dimensions do not match"))
        all(interval -> interval isa ROExactInterval, event_box) &&
            all(interval -> interval isa ROExactInterval,
                augmented_residual_enclosure) || throw(ArgumentError(
            "fold-event census cell enclosures must be exact intervals"))
        classification in (
            :unique_simple_fold_event,
            :fold_free_by_augmented_residual_exclusion,
        ) || throw(ArgumentError(
            "unknown fold-event census cell classification"))
        if classification == :unique_simple_fold_event
            excluding_augmented_equation_index == 0 || throw(ArgumentError(
                "a fold-event cell cannot name an excluding equation"))
            _rors_validate_sha256(
                event_certificate_sha256,
                "event_certificate_sha256",
            )
            all(_rors_contains_zero, augmented_residual_enclosure) ||
                throw(ArgumentError(
                    "a fold-event cell residual enclosure must contain zero"))
        else
            1 <= excluding_augmented_equation_index <= variable_count ||
                throw(ArgumentError(
                    "a fold-free cell must name one excluding equation"))
            isempty(event_certificate_sha256) || throw(ArgumentError(
                "a fold-free cell cannot bind an event certificate"))
            !_rors_contains_zero(augmented_residual_enclosure[
                excluding_augmented_equation_index]) || throw(ArgumentError(
                "the named augmented equation does not exclude zero"))
        end
        _rors_validate_sha256(evidence_sha256, "cell evidence_sha256")
        expected = _rofe_cell_sha256(
            linear_index,
            grid_index,
            event_box,
            classification,
            excluding_augmented_equation_index,
            augmented_residual_enclosure,
            event_certificate_sha256,
        )
        evidence_sha256 == expected || throw(ArgumentError(
            "fold-event census cell hash mismatch"))
        return new(
            version,
            linear_index,
            grid_index,
            event_box,
            classification,
            excluding_augmented_equation_index,
            augmented_residual_enclosure,
            event_certificate_sha256,
            evidence_sha256,
        )
    end
end

function _rofe_make_cell(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    classification::Symbol,
    excluding_augmented_equation_index::Int,
    augmented_residual_enclosure::Tuple,
    event_certificate_sha256::String,
)
    hash = _rofe_cell_sha256(
        linear_index,
        grid_index,
        event_box,
        classification,
        excluding_augmented_equation_index,
        augmented_residual_enclosure,
        event_certificate_sha256,
    )
    return ROFoldEventCensusCell(
        _ROFE_VALIDATED_TOKEN,
        RO_FOLD_EVENT_CENSUS_CELL_VERSION,
        linear_index,
        grid_index,
        event_box,
        classification,
        excluding_augmented_equation_index,
        augmented_residual_enclosure,
        event_certificate_sha256,
        hash,
    )
end

function _rofe_census_sha256(
    system_declaration_sha256::String,
    limits::ROFoldEventCensusLimits,
    augmented_variable_names::Tuple,
    augmented_variable_units::Tuple,
    event_axis_breaks::Tuple,
    declared_event_box::Tuple,
    event_grid_indices::Tuple,
    event_preconditioners::Tuple,
    events::Tuple,
    cells::Tuple,
    partition_cell_count::Int,
    fold_event_count::Int,
    unique_event_cell_count::Int,
    fold_free_cell_count::Int,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SIMPLE_FOLD_EVENT_CENSUS_VERSION)
    _rors_write_token(io, system_declaration_sha256)
    _rofe_write_limits(io, limits)
    for values in (augmented_variable_names, augmented_variable_units)
        _rors_write_token(io, length(values))
        for value in values
            _rors_write_token(io, value)
        end
    end
    _rors_write_token(io, length(event_axis_breaks))
    for axis in event_axis_breaks
        _rors_write_exact_vector(io, axis)
    end
    _rors_write_interval_vector(io, declared_event_box)
    _rors_write_token(io, length(event_grid_indices))
    for grid_index in event_grid_indices
        _rors_write_token(io, length(grid_index))
        for index in grid_index
            _rors_write_token(io, index)
        end
    end
    _rors_write_token(io, length(event_preconditioners))
    for preconditioner in event_preconditioners
        _rors_write_exact_matrix(io, preconditioner)
    end
    _rors_write_token(io, length(events))
    for event in events
        _rors_write_token(io, event.certificate_sha256)
    end
    _rors_write_token(io, length(cells))
    for cell in cells
        _rors_write_token(io, cell.evidence_sha256)
    end
    for value in (
        partition_cell_count,
        fold_event_count,
        unique_event_cell_count,
        fold_free_cell_count,
        analysis_interval_operation_count,
    )
        _rors_write_token(io, value)
    end
    for value in (
        true,  # fold_event_set_complete_inside_declared_domain
        true,  # all_fold_events_simple_inside_declared_domain
        true,  # singular_equilibrium_set_complete_inside_declared_domain
        false, # root_population_complete_inside_declared_domain
        false, # adjacent_regular_sheet_incidence_certified
        false, # stable_root_population_complete
        false, # hopf_event_set_complete
        false, # global_continuation_certified
        false, # true_hysteresis_certified
        RO_SIMPLE_FOLD_EVENT_CENSUS_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Complete P8s1b0 census of isolated simple folds inside one declared positive
`(x, lambda)` tensor box. The certificate does not enumerate regular roots or
attach either local fold branch to a remote regular-sheet component.
"""
struct ROCompleteSimpleFoldEventCensus
    version::String
    system_declaration_sha256::String
    limits::ROFoldEventCensusLimits
    augmented_variable_names::Tuple
    augmented_variable_units::Tuple
    event_axis_breaks::Tuple
    declared_event_box::Tuple
    event_grid_indices::Tuple
    event_preconditioners::Tuple
    events::Tuple
    cells::Tuple
    partition_cell_count::Int
    fold_event_count::Int
    unique_event_cell_count::Int
    fold_free_cell_count::Int
    analysis_interval_operation_count::Int
    fold_event_set_complete_inside_declared_domain::Bool
    all_fold_events_simple_inside_declared_domain::Bool
    singular_equilibrium_set_complete_inside_declared_domain::Bool
    root_population_complete_inside_declared_domain::Bool
    adjacent_regular_sheet_incidence_certified::Bool
    stable_root_population_complete::Bool
    hopf_event_set_complete::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROCompleteSimpleFoldEventCensus(
        ::_ROFEValidatedToken,
        version::String,
        system_declaration_sha256::String,
        limits::ROFoldEventCensusLimits,
        augmented_variable_names::Tuple,
        augmented_variable_units::Tuple,
        event_axis_breaks::Tuple,
        declared_event_box::Tuple,
        event_grid_indices::Tuple,
        event_preconditioners::Tuple,
        events::Tuple,
        cells::Tuple,
        partition_cell_count::Int,
        fold_event_count::Int,
        unique_event_cell_count::Int,
        fold_free_cell_count::Int,
        analysis_interval_operation_count::Int,
        fold_event_set_complete_inside_declared_domain::Bool,
        all_fold_events_simple_inside_declared_domain::Bool,
        singular_equilibrium_set_complete_inside_declared_domain::Bool,
        root_population_complete_inside_declared_domain::Bool,
        adjacent_regular_sheet_incidence_certified::Bool,
        stable_root_population_complete::Bool,
        hopf_event_set_complete::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_SIMPLE_FOLD_EVENT_CENSUS_VERSION ||
            throw(ArgumentError(
                "complete simple-fold event census version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        _rors_validate_sha256(certificate_sha256, "certificate_sha256")
        evidence_scope == RO_SIMPLE_FOLD_EVENT_CENSUS_SCOPE ||
            throw(ArgumentError("fold-event census evidence scope mismatch"))
        fold_event_set_complete_inside_declared_domain ||
            throw(ArgumentError("fold-event completeness was lost"))
        all_fold_events_simple_inside_declared_domain ||
            throw(ArgumentError("simple-fold completeness was lost"))
        singular_equilibrium_set_complete_inside_declared_domain ||
            throw(ArgumentError(
                "singular-equilibrium completeness was lost"))
        root_population_complete_inside_declared_domain &&
            throw(ArgumentError(
                "P8s1b0 does not enumerate the regular-root population"))
        adjacent_regular_sheet_incidence_certified &&
            throw(ArgumentError(
                "P8s1b0 does not certify adjacent regular-sheet incidence"))
        stable_root_population_complete && throw(ArgumentError(
            "P8s1b0 does not classify the stable-root population"))
        hopf_event_set_complete && throw(ArgumentError(
            "P8s1b0 does not certify a complete Hopf-event set"))
        global_continuation_certified && throw(ArgumentError(
            "P8s1b0 is not a global continuation certificate"))
        true_hysteresis_certified && throw(ArgumentError(
            "a static P8s1b0 census cannot certify true hysteresis"))

        variable_count = length(augmented_variable_names)
        variable_count > 1 || throw(ArgumentError(
            "a fold-event census requires at least one state and one control"))
        length(augmented_variable_units) == variable_count &&
            length(event_axis_breaks) == variable_count &&
            length(declared_event_box) == variable_count ||
            throw(DimensionMismatch(
                "fold-event census augmented dimensions do not match"))
        all(value -> value isa String, augmented_variable_names) &&
            all(value -> value isa String, augmented_variable_units) ||
            throw(ArgumentError(
                "fold-event census names and units must be strings"))
        allunique(augmented_variable_names) || throw(ArgumentError(
            "fold-event census variable names must be unique"))

        cell_dimensions = Vector{Int}(undef, variable_count)
        cell_count_big = BigInt(1)
        for variable in 1:variable_count
            axis = event_axis_breaks[variable]
            length(axis) >= 2 || throw(ArgumentError(
                "each fold-event axis needs at least two breakpoints"))
            _rofe_limit(
                :axis_breakpoints_per_variable,
                length(axis),
                limits.max_axis_breakpoints_per_variable,
            )
            all(value -> value isa _RORSExact, axis) ||
                throw(ArgumentError(
                    "fold-event axis breakpoints must be exact"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "fold-event axis breakpoints must be strictly increasing"))
            first(axis) > 0 || throw(ArgumentError(
                "the declared fold-event domain must stay positive"))
            declared_event_box[variable] == ROExactInterval(
                first(axis), last(axis), Val(:validated)) ||
                throw(ArgumentError(
                    "declared fold-event bounds do not match their axis"))
            cell_dimensions[variable] = length(axis) - 1
            cell_count_big *= cell_dimensions[variable]
            _rofe_limit(:cells, cell_count_big, limits.max_cells)
        end
        partition_cell_count == cell_count_big || throw(ArgumentError(
            "fold-event census partition count mismatch"))
        length(cells) == partition_cell_count || throw(ArgumentError(
            "fold-event census cell population is incomplete"))
        all(cell -> cell isa ROFoldEventCensusCell, cells) ||
            throw(ArgumentError(
                "fold-event census contains an invalid cell record"))

        event_by_grid = Dict{Tuple,ROSimpleFoldEvent}()
        all(event -> event isa ROSimpleFoldEvent, events) ||
            throw(ArgumentError(
                "fold-event census contains an invalid event record"))
        for event in events
            haskey(event_by_grid, event.grid_index) && throw(ArgumentError(
                "fold-event census contains duplicate event cells"))
            event_by_grid[event.grid_index] = event
        end
        length(event_grid_indices) == length(event_by_grid) &&
            allunique(event_grid_indices) || throw(ArgumentError(
            "fold-event census event indices are not unique"))
        length(event_preconditioners) == length(event_grid_indices) ||
            throw(DimensionMismatch(
                "fold-event preconditioners do not follow event indices"))
        all(matrix -> matrix isa ROExactMatrix,
            event_preconditioners) || throw(ArgumentError(
            "fold-event preconditioners must be exact matrices"))

        unique_count = 0
        free_count = 0
        event_hashes = String[]
        canonical_event_indices = Tuple[]
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(Tuple(cell_dimensions)))
            cell = cells[linear_index]
            grid_index = Tuple(cartesian_index)
            cell.linear_index == linear_index || throw(ArgumentError(
                "fold-event census cell order is not canonical"))
            cell.grid_index == grid_index || throw(ArgumentError(
                "fold-event census grid index is not canonical"))
            for variable in 1:variable_count
                axis = event_axis_breaks[variable]
                index = grid_index[variable]
                expected_box = ROExactInterval(
                    axis[index], axis[index + 1], Val(:validated))
                cell.event_box[variable] == expected_box ||
                    throw(ArgumentError(
                        "fold-event census cell bounds do not match their axis"))
            end
            expected_cell_hash = _rofe_cell_sha256(
                cell.linear_index,
                cell.grid_index,
                cell.event_box,
                cell.classification,
                cell.excluding_augmented_equation_index,
                cell.augmented_residual_enclosure,
                cell.event_certificate_sha256,
            )
            cell.evidence_sha256 == expected_cell_hash ||
                throw(ArgumentError(
                    "fold-event census nested cell hash mismatch"))
            if cell.classification == :unique_simple_fold_event
                unique_count += 1
                event = get(event_by_grid, grid_index, nothing)
                event === nothing && throw(ArgumentError(
                    "fold-event cell has no matching event certificate"))
                event.linear_index == linear_index || throw(ArgumentError(
                    "fold-event linear index does not match its cell"))
                event.event_box == cell.event_box || throw(ArgumentError(
                    "fold-event box does not match its cell"))
                event.certificate_sha256 ==
                    cell.event_certificate_sha256 || throw(ArgumentError(
                    "fold-event cell binds the wrong event certificate"))
                push!(canonical_event_indices, grid_index)
                push!(event_hashes, event.certificate_sha256)
            else
                free_count += 1
            end
        end
        unique_count == length(event_by_grid) || throw(ArgumentError(
            "not every fold event occupies exactly one partition cell"))
        event_grid_indices == Tuple(canonical_event_indices) ||
            throw(ArgumentError(
                "fold-event census event indices are not in cell order"))
        fold_event_count == unique_event_cell_count == unique_count ||
            throw(ArgumentError("fold-event census event counts mismatch"))
        fold_free_cell_count == free_count &&
            unique_count + free_count == partition_cell_count ||
            throw(ArgumentError("fold-event census cell counts mismatch"))
        _rofe_limit(:events, fold_event_count, limits.max_events)
        event_hashes == [event.certificate_sha256 for event in events] ||
            throw(ArgumentError("fold-event order is not canonical"))
        for index in eachindex(events)
            event = events[index]
            preconditioner = event_preconditioners[index]
            event.preconditioner.row_count == preconditioner.row_count &&
                event.preconditioner.column_count ==
                    preconditioner.column_count &&
                event.preconditioner.data == preconditioner.data ||
                throw(ArgumentError(
                    "fold-event preconditioner does not match its seed"))
            expected_event_hash = _rofe_event_sha256(
                event.linear_index,
                event.grid_index,
                event.event_box,
                event.center,
                event.preconditioner,
                event.krawczyk_offset_image,
                event.augmented_residual_enclosure,
                event.augmented_jacobian_enclosure,
                event.contraction_beta,
            )
            event.certificate_sha256 == expected_event_hash ||
                throw(ArgumentError("nested simple-fold event hash mismatch"))
        end
        analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "fold-event census operation count must be nonnegative"))
        _rofe_limit(
            :interval_operations,
            analysis_interval_operation_count,
            limits.max_interval_operations,
        )
        expected = _rofe_census_sha256(
            system_declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_box,
            event_grid_indices,
            event_preconditioners,
            events,
            cells,
            partition_cell_count,
            fold_event_count,
            unique_event_cell_count,
            fold_free_cell_count,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "complete simple-fold event census hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_box,
            event_grid_indices,
            event_preconditioners,
            events,
            cells,
            partition_cell_count,
            fold_event_count,
            unique_event_cell_count,
            fold_free_cell_count,
            analysis_interval_operation_count,
            true,
            true,
            true,
            false,
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

function _rofe_regular_limits_with_operation_cap(
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

function _rofe_parse_axes(
    raw,
    variable_count::Int,
    limits::ROFoldEventCensusLimits,
    context::_RORSContext,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "event_axis_breaks must be an ordered vector or tuple"))
    length(raw) == variable_count || throw(DimensionMismatch(
        "event_axis_breaks must match the augmented variable count"))
    axes = Vector{Tuple}(undef, variable_count)
    cell_count = BigInt(1)
    for variable in 1:variable_count
        context.cancel_check()
        axis = raw[variable]
        axis isa AbstractVector || axis isa Tuple || throw(ArgumentError(
            "each event-axis declaration must be a vector or tuple"))
        length(axis) >= 2 || throw(ArgumentError(
            "each event axis requires at least two breakpoints"))
        _rofe_limit(
            :axis_breakpoints_per_variable,
            length(axis),
            limits.max_axis_breakpoints_per_variable,
        )
        admitted = Vector{_RORSExact}(undef, length(axis))
        for index in eachindex(axis)
            _rors_tick!(context)
            admitted[index] = _rors_exact_float(
                axis[index], context.limits, "event_axis_breaks")
        end
        all(index -> admitted[index] < admitted[index + 1],
            1:(length(admitted) - 1)) || throw(ArgumentError(
            "event-axis breakpoints must be strictly increasing"))
        first(admitted) > 0 || throw(ArgumentError(
            "the declared event domain must stay strictly positive"))
        axes[variable] = Tuple(admitted)
        cell_count *= length(admitted) - 1
        _rofe_limit(:cells, cell_count, limits.max_cells)
    end
    return Tuple(axes)
end

function _rofe_admit_event_seeds(
    raw_indices,
    raw_preconditioners,
    event_axis_breaks::Tuple,
    limits::ROFoldEventCensusLimits,
    context::_RORSContext,
)
    raw_indices isa AbstractVector || raw_indices isa Tuple ||
        throw(ArgumentError(
            "event_grid_indices must be an ordered vector or tuple"))
    raw_preconditioners isa AbstractVector || raw_preconditioners isa Tuple ||
        throw(ArgumentError(
            "event_preconditioners must be an ordered vector or tuple"))
    length(raw_indices) == length(raw_preconditioners) ||
        throw(DimensionMismatch(
            "event_preconditioners must follow event_grid_indices"))
    _rofe_limit(:events, length(raw_indices), limits.max_events)
    variable_count = length(event_axis_breaks)
    pairs = Vector{Tuple{Tuple,Matrix{_RORSExact}}}()
    sizehint!(pairs, length(raw_indices))
    for seed in eachindex(raw_indices)
        raw_index = raw_indices[seed]
        raw_index isa AbstractVector || raw_index isa Tuple ||
            throw(ArgumentError(
                "each event grid index must be a vector or tuple"))
        length(raw_index) == variable_count || throw(DimensionMismatch(
            "event grid index $seed has the wrong dimension"))
        grid_index = Vector{Int}(undef, variable_count)
        for variable in 1:variable_count
            value = raw_index[variable]
            value isa Integer || throw(ArgumentError(
                "event grid indices must contain integers"))
            1 <= value <= length(event_axis_breaks[variable]) - 1 ||
                throw(ArgumentError(
                    "event grid index $seed is outside the partition"))
            value <= typemax(Int) || throw(ArgumentError(
                "event grid index $seed exceeds Int"))
            grid_index[variable] = Int(value)
        end
        preconditioner = _rors_exact_matrix(
            raw_preconditioners[seed],
            variable_count,
            variable_count,
            "event_preconditioners[$seed]",
            context,
        )
        push!(pairs, (Tuple(grid_index), preconditioner))
    end
    cell_dimensions = Tuple(length(axis) - 1 for axis in event_axis_breaks)
    linear_indices = LinearIndices(cell_dimensions)
    sort!(pairs; by=pair -> linear_indices[CartesianIndex(pair[1])])
    indices = [pair[1] for pair in pairs]
    allunique(indices) || throw(ArgumentError(
        "event_grid_indices must be unique"))
    return Tuple(indices), [pair[2] for pair in pairs]
end

function _rofe_system_polynomials(
    system::ROPolynomialEquilibriumSystem,
    context::_RORSContext,
)
    state_count = length(system.state_names)
    variable_count = state_count + 1
    result = Vector{_RORSPolynomial}(undef, state_count)
    for equation in 1:state_count
        context.cancel_check()
        polynomial = _RORSPolynomial()
        for term in system.equations[equation]
            coefficient = _rors_exact_float(
                term.coefficient,
                context.limits,
                "polynomial coefficient",
            )
            exponents = Tuple((
                term.state_exponents...,
                term.control_exponents[1],
            ))
            length(exponents) == variable_count || throw(DimensionMismatch(
                "augmented polynomial exponent count mismatch"))
            _rors_polynomial_add_term!(
                polynomial, exponents, coefficient, context)
        end
        result[equation] = polynomial
    end
    return result
end

function _rofe_polynomial_determinant(
    matrix::Matrix{_RORSPolynomial},
    context::_RORSContext,
)
    rows, columns = size(matrix)
    rows == columns && rows > 0 || throw(DimensionMismatch(
        "polynomial determinant requires a nonempty square matrix"))
    rows == 1 && return copy(matrix[1, 1])
    result = _RORSPolynomial()
    for column in 1:columns
        context.cancel_check()
        minor = Matrix{_RORSPolynomial}(undef, rows - 1, columns - 1)
        minor_column = 0
        for source_column in 1:columns
            source_column == column && continue
            minor_column += 1
            for source_row in 2:rows
                minor[source_row - 1, minor_column] =
                    matrix[source_row, source_column]
            end
        end
        cofactor = _rofe_polynomial_determinant(minor, context)
        product = _rors_polynomial_multiply(
            matrix[1, column], cofactor, context)
        _rors_polynomial_scaled_accumulate!(
            result,
            product,
            isodd(column) ? one(_RORSExact) : -one(_RORSExact),
            context,
        )
    end
    return result
end

function _rofe_augmented_polynomials(
    system::ROPolynomialEquilibriumSystem,
    context::_RORSContext,
)
    residual = _rofe_system_polynomials(system, context)
    state_count = length(residual)
    state_jacobian = Matrix{_RORSPolynomial}(
        undef, state_count, state_count)
    for equation in 1:state_count, state in 1:state_count
        state_jacobian[equation, state] = _rors_polynomial_derivative(
            residual[equation], state, context)
    end
    determinant = _rofe_polynomial_determinant(
        state_jacobian, context)
    return vcat(residual, [determinant])
end

function _rofe_centered_variable_polynomials(
    center::Vector{_RORSExact},
    context::_RORSContext,
)
    variable_count = length(center)
    zero_exponents = _rors_zero_exponents(variable_count)
    result = Vector{_RORSPolynomial}(undef, variable_count)
    for variable in 1:variable_count
        polynomial = _RORSPolynomial()
        _rors_polynomial_add_term!(
            polynomial, zero_exponents, center[variable], context)
        exponents = collect(zero_exponents)
        exponents[variable] = 1
        _rors_polynomial_add_term!(
            polynomial,
            Tuple(exponents),
            one(_RORSExact),
            context,
        )
        result[variable] = polynomial
    end
    return result
end

function _rofe_translate_polynomial(
    polynomial::_RORSPolynomial,
    centered_variables::Vector{_RORSPolynomial},
    context::_RORSContext,
)
    variable_count = length(centered_variables)
    translated = _RORSPolynomial()
    for exponents in sort!(collect(keys(polynomial)))
        length(exponents) == variable_count || throw(DimensionMismatch(
            "polynomial exponent tuple does not match the event domain"))
        monomial = _rors_polynomial_constant(
            polynomial[exponents], variable_count, context)
        for variable in 1:variable_count
            exponent = exponents[variable]
            iszero(exponent) && continue
            monomial = _rors_polynomial_multiply(
                monomial,
                _rors_polynomial_power(
                    centered_variables[variable],
                    exponent,
                    variable_count,
                    context,
                ),
                context,
            )
        end
        _rors_polynomial_accumulate!(translated, monomial, context)
    end
    return translated
end

function _rofe_translate_augmented_polynomials(
    augmented_polynomials::Vector{_RORSPolynomial},
    center::Vector{_RORSExact},
    context::_RORSContext,
)
    centered_variables = _rofe_centered_variable_polynomials(center, context)
    return [
        _rofe_translate_polynomial(
            polynomial, centered_variables, context)
        for polynomial in augmented_polynomials
    ]
end

function _rofe_cell_geometry(
    grid_index::Tuple,
    event_axis_breaks::Tuple,
    context::_RORSContext,
)
    variable_count = length(event_axis_breaks)
    event_box = Vector{ROExactInterval}(undef, variable_count)
    center = Vector{_RORSExact}(undef, variable_count)
    centered_box = Vector{ROExactInterval}(undef, variable_count)
    for variable in 1:variable_count
        axis = event_axis_breaks[variable]
        index = grid_index[variable]
        event_box[variable] = _rors_interval(
            context, axis[index], axis[index + 1])
        center[variable] = _rors_exact_divide(
            context,
            _rors_exact_add(context, axis[index], axis[index + 1]),
            _RORSExact(2),
        )
        centered_box[variable] = _rors_interval(
            context,
            _rors_exact_subtract(context, axis[index], center[variable]),
            _rors_exact_subtract(
                context, axis[index + 1], center[variable]),
        )
    end
    return event_box, center, centered_box
end

function _rofe_augmented_enclosures(
    translated::Vector{_RORSPolynomial},
    centered_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    variable_count = length(translated)
    residual = Vector{ROExactInterval}(undef, variable_count)
    jacobian = Matrix{ROExactInterval}(
        undef, variable_count, variable_count)
    for equation in 1:variable_count
        residual[equation] = _rors_evaluate_polynomial(
            translated[equation], centered_box, context)
        for variable in 1:variable_count
            derivative = _rors_polynomial_derivative(
                translated[equation], variable, context)
            jacobian[equation, variable] = _rors_evaluate_polynomial(
                derivative, centered_box, context)
        end
    end
    return residual, jacobian
end

function _rofe_point_residual(
    translated::Vector{_RORSPolynomial},
    context::_RORSContext,
)
    variable_count = length(translated)
    zero_exponents = _rors_zero_exponents(variable_count)
    result = Vector{ROExactInterval}(undef, variable_count)
    for equation in 1:variable_count
        coefficient = get(
            translated[equation], zero_exponents, zero(_RORSExact))
        result[equation] = _rors_point(context, coefficient)
    end
    return result
end

function _rofe_excluding_equation(
    residual::Vector{ROExactInterval},
)
    # Prefer det(F_x), because it is the defining singularity equation and
    # produces a stable canonical decision when both F and det(F_x) exclude.
    determinant_equation = length(residual)
    !_rors_contains_zero(residual[determinant_equation]) &&
        return determinant_equation
    for equation in 1:(determinant_equation - 1)
        !_rors_contains_zero(residual[equation]) && return equation
    end
    return nothing
end

function _rofe_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    event_axis_breaks::Tuple,
    event_grid_indices::Tuple,
    event_preconditioners::Vector{Matrix{_RORSExact}},
    limits::ROFoldEventCensusLimits,
    cancel_check,
)
    validate_ro_polynomial_equilibrium_system(system)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1b0 requires exactly one control coordinate"))
    state_count = length(system.state_names)
    variable_count = state_count + 1
    length(event_axis_breaks) == variable_count || throw(DimensionMismatch(
        "event axes do not match the augmented variable count"))
    length(event_grid_indices) == length(event_preconditioners) ||
        throw(DimensionMismatch(
            "event preconditioners do not follow event grid indices"))
    _rofe_limit(:events, length(event_grid_indices), limits.max_events)

    context_limits = _rofe_regular_limits_with_operation_cap(
        system.limits, limits.max_interval_operations)
    context = _RORSContext(context_limits, cancel_check)
    cells = ROFoldEventCensusCell[]
    events = ROSimpleFoldEvent[]
    try
        cell_dimensions = Vector{Int}(undef, variable_count)
        cell_count_big = BigInt(1)
        declared_event_box = Vector{ROExactInterval}(
            undef, variable_count)
        for variable in 1:variable_count
            axis = event_axis_breaks[variable]
            length(axis) >= 2 || throw(ArgumentError(
                "each event axis needs at least two breakpoints"))
            _rofe_limit(
                :axis_breakpoints_per_variable,
                length(axis),
                limits.max_axis_breakpoints_per_variable,
            )
            all(value -> value isa _RORSExact, axis) ||
                throw(ArgumentError(
                    "event-axis breakpoints must be exact"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "event-axis breakpoints must be strictly increasing"))
            first(axis) > 0 || throw(ROFoldEventCensusRejected(
                :nonpositive_declared_event_domain,
                "augmented variable $variable leaves positive coordinates",
            ))
            declared_event_box[variable] = _rors_interval(
                context, first(axis), last(axis))
            cell_dimensions[variable] = length(axis) - 1
            cell_count_big *= cell_dimensions[variable]
            _rofe_limit(:cells, cell_count_big, limits.max_cells)
        end

        seed_by_grid = Dict{Tuple,Matrix{_RORSExact}}()
        for seed in eachindex(event_grid_indices)
            context.cancel_check()
            grid_index = event_grid_indices[seed]
            length(grid_index) == variable_count || throw(DimensionMismatch(
                "event grid index $seed has the wrong dimension"))
            for variable in 1:variable_count
                index = grid_index[variable]
                index isa Int &&
                    1 <= index <= cell_dimensions[variable] ||
                    throw(ArgumentError(
                        "event grid index $seed lies outside the partition"))
            end
            haskey(seed_by_grid, grid_index) && throw(ArgumentError(
                "event grid indices must be unique"))
            preconditioner = event_preconditioners[seed]
            size(preconditioner) == (variable_count, variable_count) ||
                throw(DimensionMismatch(
                    "event preconditioner $seed has the wrong shape"))
            _rors_exact_rank(preconditioner, context) == variable_count ||
                throw(ArgumentError(
                    "event preconditioner $seed must have exact full rank"))
            seed_by_grid[grid_index] = preconditioner
        end

        augmented_polynomials = _rofe_augmented_polynomials(system, context)
        length(augmented_polynomials) == variable_count ||
            throw(DimensionMismatch(
                "augmented fold system has the wrong dimension"))
        sizehint!(cells, Int(cell_count_big))
        sizehint!(events, length(event_grid_indices))
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(Tuple(cell_dimensions)))
            context.cancel_check()
            grid_index = Tuple(cartesian_index)
            event_box, center, centered_box = _rofe_cell_geometry(
                grid_index, event_axis_breaks, context)
            translated = _rofe_translate_augmented_polynomials(
                augmented_polynomials, center, context)
            residual, augmented_jacobian = _rofe_augmented_enclosures(
                translated, centered_box, context)
            preconditioner = get(seed_by_grid, grid_index, nothing)
            if preconditioner === nothing
                excluding_equation = _rofe_excluding_equation(residual)
                excluding_equation === nothing && throw(
                    ROFoldEventCensusRejected(
                        :unresolved_event_partition_cell,
                        "partition cell $grid_index neither excludes an augmented root nor binds one event seed",
                    ))
                push!(cells, _rofe_make_cell(
                    linear_index,
                    grid_index,
                    Tuple(event_box),
                    :fold_free_by_augmented_residual_exclusion,
                    excluding_equation,
                    Tuple(residual),
                    "",
                ))
                continue
            end

            excluding_equation = _rofe_excluding_equation(residual)
            excluding_equation === nothing || throw(
                ROFoldEventCensusRejected(
                    :event_seed_excludes_augmented_root,
                    "event seed $grid_index excludes zero in augmented equation $excluding_equation",
                ))
            preconditioned_jacobian =
                _rors_point_interval_matrix_product(
                    preconditioner, augmented_jacobian, context)
            error_matrix = _rors_error_matrix(
                preconditioned_jacobian, context)
            beta = _rors_beta(error_matrix, context)
            beta < 1 || throw(ROFoldEventCensusRejected(
                :augmented_contraction_not_proven,
                "event seed $grid_index has beta=$beta, requiring beta < 1",
            ))
            point_residual = _rofe_point_residual(translated, context)
            preconditioned_residual =
                _rors_point_interval_vector_product(
                    preconditioner, point_residual, context)
            newton_offset = _rors_negate_vector(
                preconditioned_residual, context)
            error_image = _rors_interval_vector_product(
                error_matrix, centered_box, context)
            krawczyk_image = Vector{ROExactInterval}(
                undef, variable_count)
            for variable in 1:variable_count
                krawczyk_image[variable] = _rors_add(
                    context,
                    newton_offset[variable],
                    error_image[variable],
                )
                _rors_strict_subset(
                    krawczyk_image[variable], centered_box[variable]) ||
                    throw(ROFoldEventCensusRejected(
                        :augmented_krawczyk_inclusion_not_proven,
                        "event seed $grid_index Krawczyk image is not strictly interior in variable $variable",
                    ))
            end
            # The strict Krawczyk inclusion gives one interior zero of
            # H=(F,det(F_x)); beta<1 and exact full rank of C make every
            # augmented Jacobian in the enclosure nonsingular. At an H-zero,
            # corank(F_x)>=2 would make adj(F_x)=0 and hence the determinant
            # derivative row vanish, contradicting that nonsingularity. Thus
            # corank(F_x)=1. With left/right nullvectors w,v, singularity of
            # DH would likewise follow if either w'F_lambda or
            # w'F_xx[v,v] vanished. Therefore both standard one-control fold
            # nondegeneracy conditions follow without choosing a kernel
            # normalization chart.
            event = _rofe_make_event(
                linear_index,
                grid_index,
                Tuple(event_box),
                Tuple(center),
                _rors_exact_matrix_wrapper(preconditioner),
                Tuple(krawczyk_image),
                Tuple(residual),
                _rors_interval_matrix_wrapper(augmented_jacobian),
                beta,
            )
            push!(events, event)
            push!(cells, _rofe_make_cell(
                linear_index,
                grid_index,
                Tuple(event_box),
                :unique_simple_fold_event,
                0,
                Tuple(residual),
                event.certificate_sha256,
            ))
        end
        length(events) == length(seed_by_grid) || throw(ArgumentError(
            "not every event seed entered the partition"))
        context.cancel_check()
        context.operations <= typemax(Int) || throw(
            ROFoldEventCensusLimitExceeded(
                :interval_operations,
                context.operations,
                limits.max_interval_operations,
            ))
        analysis_operations = Int(context.operations)
        event_tuple = Tuple(events)
        cell_tuple = Tuple(cells)
        event_indices = Tuple(event.grid_index for event in events)
        preconditioner_wrappers = Tuple(
            event.preconditioner for event in events)
        unique_count = length(events)
        free_count = length(cells) - unique_count
        augmented_variable_names = Tuple((
            system.state_names...,
            system.control_names[1],
        ))
        augmented_variable_units = Tuple((
            system.state_units...,
            system.control_units[1],
        ))
        declared_event_tuple = Tuple(declared_event_box)
        certificate_sha256 = _rofe_census_sha256(
            system.declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_tuple,
            event_indices,
            preconditioner_wrappers,
            event_tuple,
            cell_tuple,
            length(cells),
            unique_count,
            unique_count,
            free_count,
            analysis_operations,
        )
        return ROCompleteSimpleFoldEventCensus(
            _ROFE_VALIDATED_TOKEN,
            RO_SIMPLE_FOLD_EVENT_CENSUS_VERSION,
            system.declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_tuple,
            event_indices,
            preconditioner_wrappers,
            event_tuple,
            cell_tuple,
            length(cells),
            unique_count,
            unique_count,
            free_count,
            analysis_operations,
            true,
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            RO_SIMPLE_FOLD_EVENT_CENSUS_SCOPE,
            certificate_sha256,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROFoldEventCensusLimitExceeded(
                :interval_operations,
                err.requested,
                limits.max_interval_operations,
            ))
        end
        rethrow()
    end
end

"""
    certify_ro_complete_simple_fold_event_census(system; ...)

Exhaustively classify a positive exact-dyadic tensor partition for the
normalization-free augmented system `(F, det(F_x))`. Each admitted event seed
must pass one strict Krawczyk proof; every other cell must exclude zero in one
augmented equation.
"""
function certify_ro_complete_simple_fold_event_census(
    system::ROPolynomialEquilibriumSystem;
    event_axis_breaks,
    event_grid_indices,
    event_preconditioners,
    limits::ROFoldEventCensusLimits=ROFoldEventCensusLimits(),
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1b0 requires exactly one control coordinate"))
    state_count = length(system.state_names)
    variable_count = state_count + 1
    parse_limits = _rofe_regular_limits_with_operation_cap(
        system.limits, limits.max_interval_operations)
    context = _RORSContext(parse_limits, cancel_check)
    try
        admitted_axes = _rofe_parse_axes(
            event_axis_breaks,
            variable_count,
            limits,
            context,
        )
        admitted_indices, admitted_preconditioners =
            _rofe_admit_event_seeds(
                event_grid_indices,
                event_preconditioners,
                admitted_axes,
                limits,
                context,
            )
        return _rofe_certify_exact(
            system,
            admitted_axes,
            admitted_indices,
            admitted_preconditioners,
            limits,
            cancel_check,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROFoldEventCensusLimitExceeded(
                :interval_operations,
                err.requested,
                limits.max_interval_operations,
            ))
        end
        rethrow()
    end
end

function replay_ro_complete_simple_fold_event_census(
    system::ROPolynomialEquilibriumSystem,
    certificate::ROCompleteSimpleFoldEventCensus;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "fold-event census belongs to a different polynomial system"))
    preconditioners = Matrix{_RORSExact}[]
    sizehint!(preconditioners, length(certificate.event_preconditioners))
    for preconditioner in certificate.event_preconditioners
        push!(preconditioners, _rors_exact_matrix_values(preconditioner))
    end
    rebuilt = _rofe_certify_exact(
        system,
        certificate.event_axis_breaks,
        certificate.event_grid_indices,
        preconditioners,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "complete simple-fold event census replay changed its hash"))
    return rebuilt
end

function validate_ro_complete_simple_fold_event_census(
    system::ROPolynomialEquilibriumSystem,
    certificate::ROCompleteSimpleFoldEventCensus;
    cancel_check=() -> nothing,
)
    replay_ro_complete_simple_fold_event_census(
        system,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
