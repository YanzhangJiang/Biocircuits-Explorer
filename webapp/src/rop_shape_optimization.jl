"""
Standalone LP core for fixed-cell ROP witness optimization.

The existing Polyhedra/CDDLib stack remains the owner of path geometry, but its
current Chebyshev helper does not expose a uniform infeasible/unbounded/error
contract. This module therefore takes compiled matrices as input and uses the
direct JuMP/MOI/Clarabel dependencies for auditable optimization statuses. It
does not construct engine cells or mutate the existing Designability flow.
"""
module ROPShapeOptimization

using LinearAlgebra
import Clarabel
import JuMP
import MathOptInterface as MOI

export DesignabilityCellGeometry, CompiledWitnessCell
export WitnessTerm, LinearWitnessConstraint, AbstractWitnessObjective
export LinearWitnessObjective, WitnessImprovement, BalancedMaxMinWitnessObjective
export ParameterSubspace, ActiveRowResidual
export OptimizationStatus, OPTIMAL, INFEASIBLE, UNBOUNDED, NUMERICAL_ERROR
export PrimaryEffectResult, ConditionalMarginResult, CellOptimizationResult
export UnionOptimizationResult, DirectionalIntervalResult, status_name
export parameter_subspace, maximize_effect, conditional_parameter_margin
export optimize_fixed_cell, optimize_cell_union, directional_request_interval

"""
    OptimizationStatus

Closed status vocabulary for the ROP shape-optimization core. `NUMERICAL_ERROR`
also covers an ambiguous solver status such as `INFEASIBLE_OR_UNBOUNDED`; the
core never guesses which mathematical claim the solver meant.
"""
@enum OptimizationStatus::UInt8 begin
    OPTIMAL
    INFEASIBLE
    UNBOUNDED
    NUMERICAL_ERROR
end

status_name(status::OptimizationStatus) = if status == OPTIMAL
    "optimal"
elseif status == INFEASIBLE
    "infeasible"
elseif status == UNBOUNDED
    "unbounded"
else
    "numerical_error"
end

function _nonempty_id(raw, name::AbstractString)
    value = strip(String(raw))
    isempty(value) && throw(ArgumentError("$name must be a non-empty string"))
    return value
end

function _finite_float(raw, name::AbstractString)
    (raw isa Real && !(raw isa Bool)) ||
        throw(ArgumentError("$name must be a finite non-boolean real"))
    value = try
        Float64(raw)
    catch
        throw(ArgumentError("$name must be a finite non-boolean real"))
    end
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    return value
end

function _finite_nonnegative(raw, name::AbstractString)
    value = _finite_float(raw, name)
    value >= 0 || throw(ArgumentError("$name must be nonnegative"))
    return value
end

function _finite_positive(raw, name::AbstractString)
    value = _finite_float(raw, name)
    value > 0 || throw(ArgumentError("$name must be positive"))
    return value
end

function _finite_matrix(raw, name::AbstractString)
    raw isa AbstractMatrix || throw(ArgumentError("$name must be a matrix"))
    value = try
        Matrix{Float64}(raw)
    catch
        throw(ArgumentError("$name must contain only finite real values"))
    end
    all(isfinite, value) || throw(ArgumentError("$name must contain only finite values"))
    return value
end

function _finite_vector(raw, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name must be a vector"))
    value = try
        Vector{Float64}(raw)
    catch
        throw(ArgumentError("$name must contain only finite real values"))
    end
    all(isfinite, value) || throw(ArgumentError("$name must contain only finite values"))
    return value
end

function _symbol_vector(raw, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name must be a vector"))
    values = Symbol.(raw)
    length(unique(values)) == length(values) ||
        throw(ArgumentError("$name must not contain duplicates"))
    return values
end

function _id_vector(raw, expected::Int, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name must be a vector"))
    values = [_nonempty_id(value, "$name[$idx]") for (idx, value) in enumerate(raw)]
    length(values) == expected ||
        throw(ArgumentError("$name must contain exactly $expected entries"))
    length(unique(values)) == length(values) ||
        throw(ArgumentError("$name must not contain duplicates"))
    return values
end

"""
    DesignabilityCellGeometry(...)

Auditable matrix form of one fixed path/witness cell. Rows use the convention
`Aineq * z <= bineq` and `Aeq * z == beq`. Coordinates are partitioned into
background parameters and finite-window witnesses; auxiliary or silently
unclassified coordinates are intentionally rejected by this standalone core.
"""
struct DesignabilityCellGeometry
    Aeq::Matrix{Float64}
    beq::Vector{Float64}
    Aineq::Matrix{Float64}
    bineq::Vector{Float64}
    coordinates::Vector{Symbol}
    parameter_coordinates::Vector{Symbol}
    witness_coordinates::Vector{Symbol}
    equality_row_ids::Vector{String}
    inequality_row_ids::Vector{String}
    path_identity::String
    witness_identity::Vector{String}

    function DesignabilityCellGeometry(
        Aeq::AbstractMatrix,
        beq::AbstractVector,
        Aineq::AbstractMatrix,
        bineq::AbstractVector;
        coordinates::AbstractVector,
        parameter_coordinates::AbstractVector,
        witness_coordinates::AbstractVector,
        equality_row_ids::AbstractVector,
        inequality_row_ids::AbstractVector,
        path_identity,
        witness_identity::AbstractVector,
    )
        eq = _finite_matrix(Aeq, "Aeq")
        eq_rhs = _finite_vector(beq, "beq")
        ineq = _finite_matrix(Aineq, "Aineq")
        ineq_rhs = _finite_vector(bineq, "bineq")
        coords = _symbol_vector(coordinates, "coordinates")
        parameters = _symbol_vector(parameter_coordinates, "parameter_coordinates")
        witnesses = _symbol_vector(witness_coordinates, "witness_coordinates")

        size(eq, 2) == length(coords) ||
            throw(ArgumentError("Aeq must have one column per coordinate"))
        size(ineq, 2) == length(coords) ||
            throw(ArgumentError("Aineq must have one column per coordinate"))
        size(eq, 1) == length(eq_rhs) ||
            throw(ArgumentError("Aeq and beq row counts differ"))
        size(ineq, 1) == length(ineq_rhs) ||
            throw(ArgumentError("Aineq and bineq row counts differ"))
        isempty(coords) && throw(ArgumentError("coordinates must not be empty"))
        isempty(witnesses) && throw(ArgumentError("at least one witness coordinate is required"))

        partition = vcat(parameters, witnesses)
        length(unique(partition)) == length(partition) ||
            throw(ArgumentError("parameter and witness coordinates must be disjoint"))
        Set(partition) == Set(coords) ||
            throw(ArgumentError("parameter and witness coordinates must partition coordinates"))

        eq_ids = _id_vector(equality_row_ids, size(eq, 1), "equality_row_ids")
        ineq_ids = _id_vector(inequality_row_ids, size(ineq, 1), "inequality_row_ids")
        isempty(intersect(Set(eq_ids), Set(ineq_ids))) ||
            throw(ArgumentError("equality and inequality row IDs must be globally unique"))
        for row in axes(eq, 1)
            norm(view(eq, row, :)) > 0 ||
                throw(ArgumentError("Aeq row $(eq_ids[row]) has no coefficients"))
        end
        for row in axes(ineq, 1)
            norm(view(ineq, row, :)) > 0 ||
                throw(ArgumentError("Aineq row $(ineq_ids[row]) has no coefficients"))
        end

        path_id = _nonempty_id(path_identity, "path_identity")
        witness_ids = _id_vector(
            witness_identity, length(witnesses), "witness_identity")
        return new(
            eq, eq_rhs, ineq, ineq_rhs, coords, parameters, witnesses,
            eq_ids, ineq_ids, path_id, witness_ids,
        )
    end
end

const CompiledWitnessCell = DesignabilityCellGeometry

"A nonzero coefficient attached to one named witness coordinate."
struct WitnessTerm
    coordinate::Symbol
    coefficient::Float64

    function WitnessTerm(coordinate, coefficient)
        coord = Symbol(coordinate)
        value = _finite_float(coefficient, "witness coefficient")
        value != 0 || throw(ArgumentError("witness coefficients must be nonzero"))
        return new(coord, value)
    end
end

function _validated_terms(raw, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name terms must be a vector"))
    terms = WitnessTerm[term isa WitnessTerm ? term : WitnessTerm(term.first, term.second)
                        for term in raw]
    isempty(terms) && throw(ArgumentError("$name must contain at least one term"))
    coords = getfield.(terms, :coordinate)
    length(unique(coords)) == length(coords) ||
        throw(ArgumentError("$name must not repeat a witness coordinate"))
    return terms
end

function _relation(raw)
    value = Symbol(raw)
    value in (:le, :<=) && return :le
    value in (:ge, :>=) && return :ge
    value in (:eq, :(==), :(=)) && return :eq
    throw(ArgumentError("relation must be one of :le, :ge, or :eq"))
end

"A typed affine constraint over named finite-window witness coordinates."
struct LinearWitnessConstraint
    id::String
    terms::Vector{WitnessTerm}
    relation::Symbol
    rhs::Float64

    function LinearWitnessConstraint(id, terms::AbstractVector, relation, rhs)
        constraint_id = _nonempty_id(id, "constraint id")
        return new(
            constraint_id,
            _validated_terms(terms, "constraint $constraint_id"),
            _relation(relation),
            _finite_float(rhs, "constraint $constraint_id rhs"),
        )
    end
end

abstract type AbstractWitnessObjective end

"A nonconstant linear effect objective over named witness coordinates."
struct LinearWitnessObjective <: AbstractWitnessObjective
    id::String
    terms::Vector{WitnessTerm}
    sense::Symbol

    function LinearWitnessObjective(id, terms::AbstractVector; sense=:maximize)
        objective_id = _nonempty_id(id, "objective id")
        direction = Symbol(sense)
        direction in (:maximize, :minimize) ||
            throw(ArgumentError("objective sense must be :maximize or :minimize"))
        return new(
            objective_id,
            _validated_terms(terms, "objective $objective_id"),
            direction,
        )
    end
end

"One reference-relative linear improvement used by a balanced max-min objective."
struct WitnessImprovement
    id::String
    terms::Vector{WitnessTerm}
    reference_value::Float64

    function WitnessImprovement(id, terms::AbstractVector, reference_value)
        improvement_id = _nonempty_id(id, "improvement id")
        return new(
            improvement_id,
            _validated_terms(terms, "improvement $improvement_id"),
            _finite_float(reference_value, "improvement $improvement_id reference_value"),
        )
    end
end

"""
Maximize the common minimum of several reference-relative witness improvements.

For groups `g`, this compiles an epigraph variable `alpha` with
`alpha <= a_g' * tau - reference_g`, then maximizes `alpha`. It is a true
balanced max-min objective, not a weighted sum that lets one group hide another.
"""
struct BalancedMaxMinWitnessObjective <: AbstractWitnessObjective
    id::String
    improvements::Vector{WitnessImprovement}

    function BalancedMaxMinWitnessObjective(id, improvements::AbstractVector)
        objective_id = _nonempty_id(id, "objective id")
        groups = WitnessImprovement[
            group isa WitnessImprovement ? group :
            throw(ArgumentError("balanced objective groups must be WitnessImprovement values"))
            for group in improvements
        ]
        isempty(groups) && throw(ArgumentError(
            "balanced max-min objective must contain at least one improvement"))
        ids = getfield.(groups, :id)
        length(unique(ids)) == length(ids) || throw(ArgumentError(
            "balanced max-min improvement IDs must be unique"))
        return new(objective_id, groups)
    end
end

"Parameter-only equality nullspace, with an explicit zero-dimensional convention."
struct ParameterSubspace
    coordinates::Vector{Symbol}
    basis::Matrix{Float64}
    dimension::Int
    equality_rank::Int
    rank_relative_tolerance::Float64
    rank_absolute_threshold::Float64
end

"""
Residual and optional sensitivity for an active source row.

`dual` is the raw JuMP/MOI conic dual. `shadow_price` is the derivative of the
optimal objective with respect to the row's *compiled* RHS (`Aineq*z <= bineq`
or `Aeq*z = beq`). For a maximization it is `-dual`; for a minimization it is
`dual`. Both are `nothing` when the solver has no finite dual or when active-row
normals are rank deficient and the multiplier is not safely interpretable.
"""
struct ActiveRowResidual
    row_id::String
    row_kind::Symbol
    point_residual::Float64
    ball_residual::Float64
    normalized_residual::Float64
    dual::Union{Nothing, Float64}
    shadow_price::Union{Nothing, Float64}
end

struct PrimaryEffectResult
    status::OptimizationStatus
    effect::Union{Nothing, Float64}
    solution::Union{Nothing, Vector{Float64}}
    subspace::ParameterSubspace
    active_rows::Vector{ActiveRowResidual}
    minimum_parameter_margin::Float64
    solver_status::String
    message::String
end

struct ConditionalMarginResult
    status::OptimizationStatus
    effect::Union{Nothing, Float64}
    parameter_margin::Union{Nothing, Float64}
    solution::Union{Nothing, Vector{Float64}}
    subspace::ParameterSubspace
    active_rows::Vector{ActiveRowResidual}
    effect_bound::Float64
    solver_status::String
    message::String
end

struct CellOptimizationResult
    status::OptimizationStatus
    path_identity::String
    witness_identity::Vector{String}
    primary::PrimaryEffectResult
    margin::Union{Nothing, ConditionalMarginResult}
    selected_for_margin::Bool
end

struct UnionOptimizationResult
    status::OptimizationStatus
    global_effect::Union{Nothing, Float64}
    selected_cell_index::Union{Nothing, Int}
    selected::Union{Nothing, CellOptimizationResult}
    cell_results::Vector{CellOptimizationResult}
    effect_tolerance::Float64
    message::String
end

struct DirectionalIntervalResult
    status::OptimizationStatus
    alpha_min::Union{Nothing, Float64}
    alpha_max::Union{Nothing, Float64}
    lower_status::OptimizationStatus
    upper_status::OptimizationStatus
    reference_witness::Vector{Float64}
    direction::Vector{Float64}
    direction_norm::Float64
    witness_coordinates::Vector{Symbol}
    path_identity::String
    witness_identity::Vector{String}
    lower_solution::Union{Nothing, Vector{Float64}}
    upper_solution::Union{Nothing, Vector{Float64}}
    message::String
end

struct _CompiledSystem
    Aeq::Matrix{Float64}
    beq::Vector{Float64}
    Aineq::Matrix{Float64}
    bineq::Vector{Float64}
    equality_row_ids::Vector{String}
    inequality_row_ids::Vector{String}
    equality_row_kinds::Vector{Symbol}
    inequality_row_kinds::Vector{Symbol}
end

struct _LPSolution
    status::OptimizationStatus
    objective::Union{Nothing, Float64}
    solution::Union{Nothing, Vector{Float64}}
    solver_status::String
    message::String
    equality_duals::Union{Nothing, Vector{Float64}}
    inequality_duals::Union{Nothing, Vector{Float64}}
end

struct _ObjectiveProgram
    system::_CompiledSystem
    objective_vector::Vector{Float64}
    sense::Symbol
    base_variable_count::Int
end

function _coordinate_index(cell::DesignabilityCellGeometry)
    return Dict(coordinate => idx for (idx, coordinate) in enumerate(cell.coordinates))
end

function _parameter_indices(cell::DesignabilityCellGeometry)
    indices = _coordinate_index(cell)
    return [indices[coordinate] for coordinate in cell.parameter_coordinates]
end

function _witness_row(
    cell::DesignabilityCellGeometry,
    terms::Vector{WitnessTerm},
    owner::AbstractString,
)
    indices = _coordinate_index(cell)
    witnesses = Set(cell.witness_coordinates)
    row = zeros(Float64, length(cell.coordinates))
    for term in terms
        term.coordinate in witnesses || throw(ArgumentError(
            "$owner references $(term.coordinate), which is not a witness coordinate in " *
            "path $(cell.path_identity)",
        ))
        row[indices[term.coordinate]] = term.coefficient
    end
    norm(row) > 0 || throw(ArgumentError("$owner compiles to a zero row"))
    return row
end

function _compile_system(
    cell::DesignabilityCellGeometry,
    constraints::AbstractVector{<:LinearWitnessConstraint},
)
    Aeq = copy(cell.Aeq)
    beq = copy(cell.beq)
    Aineq = copy(cell.Aineq)
    bineq = copy(cell.bineq)
    eq_ids = copy(cell.equality_row_ids)
    ineq_ids = copy(cell.inequality_row_ids)
    eq_kinds = fill(:cell_equality, length(eq_ids))
    ineq_kinds = fill(:cell_inequality, length(ineq_ids))
    used_ids = Set(vcat(eq_ids, ineq_ids))

    for constraint in constraints
        constraint.id in used_ids && throw(ArgumentError(
            "constraint ID $(constraint.id) collides with an existing row ID",
        ))
        push!(used_ids, constraint.id)
        row = _witness_row(cell, constraint.terms, "constraint $(constraint.id)")
        if constraint.relation == :eq
            Aeq = vcat(Aeq, reshape(row, 1, :))
            push!(beq, constraint.rhs)
            push!(eq_ids, constraint.id)
            push!(eq_kinds, :witness_constraint)
        elseif constraint.relation == :le
            Aineq = vcat(Aineq, reshape(row, 1, :))
            push!(bineq, constraint.rhs)
            push!(ineq_ids, constraint.id)
            push!(ineq_kinds, :witness_constraint)
        else
            Aineq = vcat(Aineq, reshape(-row, 1, :))
            push!(bineq, -constraint.rhs)
            push!(ineq_ids, constraint.id)
            push!(ineq_kinds, :witness_constraint)
        end
    end
    return _CompiledSystem(
        Aeq, beq, Aineq, bineq, eq_ids, ineq_ids, eq_kinds, ineq_kinds)
end

_objective_id(objective::AbstractWitnessObjective) = objective.id
_objective_sense(objective::LinearWitnessObjective) = objective.sense
_objective_sense(::BalancedMaxMinWitnessObjective) = :maximize

function _compile_objective(
    cell::DesignabilityCellGeometry,
    system::_CompiledSystem,
    objective::LinearWitnessObjective,
)
    vector = _witness_row(cell, objective.terms, "objective $(objective.id)")
    return _ObjectiveProgram(system, vector, objective.sense, length(cell.coordinates))
end

function _compile_objective(
    cell::DesignabilityCellGeometry,
    system::_CompiledSystem,
    objective::BalancedMaxMinWitnessObjective,
)
    base_count = length(cell.coordinates)
    Aeq = hcat(system.Aeq, zeros(Float64, size(system.Aeq, 1)))
    Aineq = hcat(system.Aineq, zeros(Float64, size(system.Aineq, 1)))
    bineq = copy(system.bineq)
    ineq_ids = copy(system.inequality_row_ids)
    ineq_kinds = copy(system.inequality_row_kinds)
    used_ids = Set(vcat(system.equality_row_ids, ineq_ids))
    for improvement in objective.improvements
        row_id = "objective:$(objective.id):improvement:$(improvement.id)"
        row_id in used_ids && throw(ArgumentError(
            "balanced-objective row ID $row_id collides with an existing row ID"))
        push!(used_ids, row_id)
        witness_row = _witness_row(
            cell, improvement.terms,
            "objective $(objective.id) improvement $(improvement.id)",
        )
        # alpha <= a' * tau - reference  ==>  alpha - a' * tau <= -reference
        epigraph_row = vcat(-witness_row, 1.0)
        Aineq = vcat(Aineq, reshape(epigraph_row, 1, :))
        push!(bineq, -improvement.reference_value)
        push!(ineq_ids, row_id)
        push!(ineq_kinds, :balanced_objective)
    end
    augmented = _CompiledSystem(
        Aeq, system.beq, Aineq, bineq,
        system.equality_row_ids, ineq_ids,
        system.equality_row_kinds, ineq_kinds,
    )
    objective_vector = vcat(zeros(Float64, base_count), 1.0)
    return _ObjectiveProgram(augmented, objective_vector, :maximize, base_count)
end

function _effect_value(
    cell::DesignabilityCellGeometry,
    objective::LinearWitnessObjective,
    solution::Vector{Float64},
)
    row = _witness_row(cell, objective.terms, "objective $(objective.id)")
    return dot(row, solution)
end

function _effect_value(
    cell::DesignabilityCellGeometry,
    objective::BalancedMaxMinWitnessObjective,
    solution::Vector{Float64},
)
    return minimum(
        dot(_witness_row(
                cell, improvement.terms,
                "objective $(objective.id) improvement $(improvement.id)"),
            solution) - improvement.reference_value
        for improvement in objective.improvements
    )
end

function parameter_subspace(
    cell::DesignabilityCellGeometry;
    rank_tolerance::Real=sqrt(eps(Float64)),
)
    return _parameter_subspace(
        cell, cell.Aeq; rank_tolerance=rank_tolerance)
end

function _parameter_subspace(
    cell::DesignabilityCellGeometry,
    Aeq::Matrix{Float64};
    rank_tolerance::Real,
)
    relative_tolerance = _finite_positive(rank_tolerance, "rank_tolerance")
    relative_tolerance < 1 ||
        throw(ArgumentError("rank_tolerance must be less than 1"))
    parameter_indices = _parameter_indices(cell)
    parameter_count = length(parameter_indices)
    parameter_count == 0 && return ParameterSubspace(
        copy(cell.parameter_coordinates), zeros(Float64, 0, 0), 0, 0,
        relative_tolerance, 0.0,
    )
    isempty(Aeq) && return ParameterSubspace(
        copy(cell.parameter_coordinates), Matrix{Float64}(I, parameter_count, parameter_count),
        parameter_count, 0, relative_tolerance, 0.0,
    )

    parameter_equalities = Matrix(Aeq[:, parameter_indices])
    # Rank and nullspace are invariant to nonzero row scaling. Normalize each
    # parameter-bearing row before applying the relative SVD tolerance so that
    # a provenance-preserving rescale cannot silently change the 0D decision.
    for row in axes(parameter_equalities, 1)
        row_norm = norm(view(parameter_equalities, row, :))
        row_norm > 0 && (parameter_equalities[row, :] ./= row_norm)
    end
    decomposition = svd(parameter_equalities; full=true)
    largest = isempty(decomposition.S) ? 0.0 : maximum(decomposition.S)
    absolute_threshold = relative_tolerance * largest
    equality_rank = largest == 0 ? 0 : count(value -> value > absolute_threshold, decomposition.S)
    right_vectors = Matrix(decomposition.V)
    basis = equality_rank == parameter_count ?
        zeros(Float64, parameter_count, 0) :
        Matrix(right_vectors[:, (equality_rank + 1):parameter_count])
    return ParameterSubspace(
        copy(cell.parameter_coordinates), basis, size(basis, 2), equality_rank,
        relative_tolerance, absolute_threshold,
    )
end

function _row_scale(row::AbstractVector{<:Real}, rhs::Real)
    # Every accepted row has a nonzero coefficient, so this remains positive
    # and scales exactly when both row and RHS are multiplied by one factor.
    return max(norm(row), abs(Float64(rhs)))
end

function _validate_solution(
    Aeq::Matrix{Float64},
    beq::Vector{Float64},
    Aineq::Matrix{Float64},
    bineq::Vector{Float64},
    solution::Vector{Float64},
    tolerance::Float64,
)
    all(isfinite, solution) || return false, "solver returned a non-finite primal point"
    for idx in axes(Aeq, 1)
        residual = dot(view(Aeq, idx, :), solution) - beq[idx]
        abs(residual) <= tolerance * _row_scale(view(Aeq, idx, :), beq[idx]) ||
            return false, "equality row $idx failed residual validation"
    end
    for idx in axes(Aineq, 1)
        residual = dot(view(Aineq, idx, :), solution) - bineq[idx]
        residual <= tolerance * _row_scale(view(Aineq, idx, :), bineq[idx]) ||
            return false, "inequality row $idx failed residual validation"
    end
    return true, ""
end

function _solve_lp(
    Aeq::Matrix{Float64},
    beq::Vector{Float64},
    Aineq::Matrix{Float64},
    bineq::Vector{Float64},
    objective::Vector{Float64},
    sense::Symbol;
    optimizer_factory,
    validation_tolerance::Real,
)
    tolerance = _finite_positive(validation_tolerance, "validation_tolerance")
    size(Aeq, 2) == length(objective) ||
        throw(ArgumentError("objective length does not match equality columns"))
    size(Aineq, 2) == length(objective) ||
        throw(ArgumentError("objective length does not match inequality columns"))
    sense in (:maximize, :minimize) || throw(ArgumentError("invalid LP sense"))

    try
        model = JuMP.Model(optimizer_factory)
        JuMP.set_silent(model)
        variable_count = length(objective)
        JuMP.@variable(model, z[1:variable_count])
        equality_refs = JuMP.ConstraintRef[]
        inequality_refs = JuMP.ConstraintRef[]
        for idx in axes(Aeq, 1)
            constraint_ref = JuMP.@constraint(
                model,
                sum(Aeq[idx, col] * z[col] for col in 1:variable_count) == beq[idx],
            )
            push!(equality_refs, constraint_ref)
        end
        for idx in axes(Aineq, 1)
            constraint_ref = JuMP.@constraint(
                model,
                sum(Aineq[idx, col] * z[col] for col in 1:variable_count) <= bineq[idx],
            )
            push!(inequality_refs, constraint_ref)
        end
        if sense == :maximize
            JuMP.@objective(
                model, Max,
                sum(objective[col] * z[col] for col in 1:variable_count),
            )
        else
            JuMP.@objective(
                model, Min,
                sum(objective[col] * z[col] for col in 1:variable_count),
            )
        end
        JuMP.optimize!(model)
        termination = JuMP.termination_status(model)
        raw_status = string(termination)
        if termination == MOI.INFEASIBLE &&
           JuMP.dual_status(model) == MOI.INFEASIBILITY_CERTIFICATE
            return _LPSolution(INFEASIBLE, nothing, nothing, raw_status,
                               "the compiled cell is infeasible", nothing, nothing)
        elseif termination == MOI.DUAL_INFEASIBLE &&
               JuMP.primal_status(model) == MOI.INFEASIBILITY_CERTIFICATE
            return _LPSolution(UNBOUNDED, nothing, nothing, raw_status,
                               "the requested objective is unbounded", nothing, nothing)
        elseif termination != MOI.OPTIMAL
            return _LPSolution(
                NUMERICAL_ERROR, nothing, nothing, raw_status,
                "solver did not return an unambiguous optimal/infeasible/unbounded status",
                nothing, nothing,
            )
        end
        JuMP.primal_status(model) == MOI.FEASIBLE_POINT || return _LPSolution(
            NUMERICAL_ERROR, nothing, nothing, raw_status,
            "solver reported optimal without a feasible primal point",
            nothing, nothing,
        )
        solution = Float64.(JuMP.value.(z))
        valid, validation_message = _validate_solution(
            Aeq, beq, Aineq, bineq, solution, tolerance)
        valid || return _LPSolution(
            NUMERICAL_ERROR, nothing, nothing, raw_status, validation_message,
            nothing, nothing)
        value = dot(objective, solution)
        isfinite(value) || return _LPSolution(
            NUMERICAL_ERROR, nothing, nothing, raw_status,
            "solver returned a non-finite objective value",
            nothing, nothing,
        )
        equality_duals = nothing
        inequality_duals = nothing
        try
            candidate_eq = Float64[JuMP.dual(ref) for ref in equality_refs]
            candidate_ineq = Float64[JuMP.dual(ref) for ref in inequality_refs]
            if all(isfinite, candidate_eq) && all(isfinite, candidate_ineq)
                equality_duals = candidate_eq
                inequality_duals = candidate_ineq
            end
        catch
            # A valid primal optimum remains usable when an optimizer does not
            # implement row duals. Active-row reporting will record `nothing`.
        end
        return _LPSolution(
            OPTIMAL, value, solution, raw_status, "optimal LP solution",
            equality_duals, inequality_duals,
        )
    catch error
        return _LPSolution(
            NUMERICAL_ERROR, nothing, nothing, "exception", sprint(showerror, error),
            nothing, nothing)
    end
end

function _parameter_row_norms(
    cell::DesignabilityCellGeometry,
    system::_CompiledSystem,
    subspace::ParameterSubspace,
)
    parameter_indices = _parameter_indices(cell)
    values = zeros(Float64, size(system.Aineq, 1))
    subspace.dimension == 0 && return values
    for idx in axes(system.Aineq, 1)
        parameter_row = Vector(system.Aineq[idx, parameter_indices])
        values[idx] = norm(transpose(subspace.basis) * parameter_row)
    end
    return values
end

function _active_rows(
    system::_CompiledSystem,
    solution::Vector{Float64};
    radius::Real=0.0,
    parameter_row_norms::AbstractVector=zeros(size(system.Aineq, 1)),
    tolerance::Real=1e-7,
    equality_duals::Union{Nothing, AbstractVector}=nothing,
    inequality_duals::Union{Nothing, AbstractVector}=nothing,
    objective_sense::Symbol=:maximize,
    radius_is_variable::Bool=false,
)
    active_tolerance = _finite_positive(tolerance, "active_tolerance")
    radius_value = _finite_nonnegative(radius, "radius")
    length(parameter_row_norms) == size(system.Aineq, 1) ||
        throw(ArgumentError("parameter-row norm count does not match inequalities"))
    objective_sense in (:maximize, :minimize) ||
        throw(ArgumentError("objective_sense must be :maximize or :minimize"))
    active_inequality_indices = Int[]
    inequality_residuals = Dict{Int, Tuple{Float64, Float64, Float64}}()
    for idx in axes(system.Aineq, 1)
        point_residual = dot(view(system.Aineq, idx, :), solution) - system.bineq[idx]
        ball_residual = point_residual + Float64(parameter_row_norms[idx]) * radius_value
        normalized = ball_residual /
                     _row_scale(view(system.Aineq, idx, :), system.bineq[idx])
        inequality_residuals[idx] = (point_residual, ball_residual, normalized)
        abs(normalized) <= active_tolerance && push!(active_inequality_indices, idx)
    end

    actual_variable_count = length(solution) + (radius_is_variable ? 1 : 0)
    active_normals = Vector{Vector{Float64}}()
    for idx in axes(system.Aeq, 1)
        normal = Vector(view(system.Aeq, idx, :))
        radius_is_variable && push!(normal, 0.0)
        push!(active_normals, normal)
    end
    for idx in active_inequality_indices
        normal = Vector(view(system.Aineq, idx, :))
        radius_is_variable && push!(normal, Float64(parameter_row_norms[idx]))
        push!(active_normals, normal)
    end
    if radius_is_variable && radius_value <= active_tolerance
        push!(active_normals, vcat(zeros(Float64, length(solution)), -1.0))
    end
    active_matrix = isempty(active_normals) ?
        zeros(Float64, 0, actual_variable_count) : reduce(vcat, transpose.(active_normals))
    for idx in axes(active_matrix, 1)
        active_matrix[idx, :] ./= norm(view(active_matrix, idx, :))
    end
    singular_values = svdvals(active_matrix)
    largest = isempty(singular_values) ? 0.0 : maximum(singular_values)
    rank_threshold = sqrt(eps(Float64)) * largest
    active_rank = largest == 0 ? 0 : count(value -> value > rank_threshold, singular_values)
    duals_available = equality_duals !== nothing && inequality_duals !== nothing &&
        length(something(equality_duals)) == size(system.Aeq, 1) &&
        length(something(inequality_duals)) >= size(system.Aineq, 1) &&
        all(isfinite, something(equality_duals)) &&
        all(isfinite, something(inequality_duals))
    duals_reliable = duals_available && active_rank == size(active_matrix, 1)
    dual_note = if !duals_available
        "active-row duals unavailable from the LP backend"
    elseif !duals_reliable
        "active-row duals suppressed because active-row normals are rank deficient"
    else
        ""
    end
    shadow_sign = objective_sense == :maximize ? -1.0 : 1.0

    rows = ActiveRowResidual[]
    for idx in axes(system.Aeq, 1)
        residual = dot(view(system.Aeq, idx, :), solution) - system.beq[idx]
        normalized = residual / _row_scale(view(system.Aeq, idx, :), system.beq[idx])
        dual = duals_reliable ? Float64(something(equality_duals)[idx]) : nothing
        push!(rows, ActiveRowResidual(
            system.equality_row_ids[idx], system.equality_row_kinds[idx], residual,
            residual, normalized, dual,
            dual === nothing ? nothing : shadow_sign * dual,
        ))
    end
    for idx in active_inequality_indices
        point_residual, ball_residual, normalized = inequality_residuals[idx]
        dual = duals_reliable ? Float64(something(inequality_duals)[idx]) : nothing
        push!(rows, ActiveRowResidual(
            system.inequality_row_ids[idx], system.inequality_row_kinds[idx],
            point_residual, ball_residual, normalized, dual,
            dual === nothing ? nothing : shadow_sign * dual,
        ))
    end
    return rows, dual_note
end

function maximize_effect(
    cell::DesignabilityCellGeometry,
    objective::AbstractWitnessObjective;
    constraints::AbstractVector{<:LinearWitnessConstraint}=LinearWitnessConstraint[],
    minimum_parameter_margin::Real=0.0,
    rank_tolerance::Real=sqrt(eps(Float64)),
    active_tolerance::Real=1e-7,
    validation_tolerance::Real=1e-7,
    optimizer_factory=Clarabel.Optimizer,
)
    minimum_margin = _finite_nonnegative(
        minimum_parameter_margin, "minimum_parameter_margin")
    _finite_positive(active_tolerance, "active_tolerance")
    base_system = _compile_system(cell, constraints)
    program = _compile_objective(cell, base_system, objective)
    system = program.system
    objective_vector = program.objective_vector
    subspace = _parameter_subspace(
        cell, system.Aeq; rank_tolerance=rank_tolerance)
    row_norms = _parameter_row_norms(cell, system, subspace)
    if subspace.dimension == 0 && minimum_margin > 0
        return PrimaryEffectResult(
            INFEASIBLE, nothing, nothing, subspace, ActiveRowResidual[], minimum_margin,
            "zero_dimensional_parameter_subspace",
            "positive parameter margin is impossible under the explicit 0D convention",
        )
    end

    # A fixed minimum radius is exactly a RHS contraction in each inequality.
    contracted_rhs = system.bineq .- row_norms .* minimum_margin
    raw = _solve_lp(
        system.Aeq, system.beq, system.Aineq, contracted_rhs,
        objective_vector, program.sense;
        optimizer_factory=optimizer_factory,
        validation_tolerance=validation_tolerance,
    )
    raw.status == OPTIMAL || return PrimaryEffectResult(
        raw.status, nothing, nothing, subspace, ActiveRowResidual[], minimum_margin,
        raw.solver_status, raw.message,
    )
    full_solution = something(raw.solution)
    solution = full_solution[1:program.base_variable_count]
    active, dual_note = _active_rows(
        system, full_solution; radius=minimum_margin, parameter_row_norms=row_norms,
        tolerance=active_tolerance, equality_duals=raw.equality_duals,
        inequality_duals=raw.inequality_duals, objective_sense=program.sense,
    )
    return PrimaryEffectResult(
        OPTIMAL, _effect_value(cell, objective, solution), solution, subspace, active,
        minimum_margin, raw.solver_status,
        isempty(dual_note) ? raw.message : "$(raw.message); $dual_note",
    )
end

function _with_effect_bound(
    system::_CompiledSystem,
    objective_vector::Vector{Float64},
    objective::AbstractWitnessObjective,
    effect_bound::Float64,
)
    row_id = "optimization:$(_objective_id(objective)):effect_bound"
    row_id in Set(vcat(system.equality_row_ids, system.inequality_row_ids)) &&
        throw(ArgumentError("effect-bound row ID collides with an existing row ID"))
    if _objective_sense(objective) == :maximize
        row = -objective_vector
        rhs = -effect_bound
    else
        row = objective_vector
        rhs = effect_bound
    end
    return _CompiledSystem(
        system.Aeq,
        system.beq,
        vcat(system.Aineq, reshape(row, 1, :)),
        vcat(system.bineq, rhs),
        system.equality_row_ids,
        vcat(system.inequality_row_ids, row_id),
        system.equality_row_kinds,
        vcat(system.inequality_row_kinds, :effect_bound),
    )
end

function conditional_parameter_margin(
    cell::DesignabilityCellGeometry,
    objective::AbstractWitnessObjective,
    effect_bound::Real;
    constraints::AbstractVector{<:LinearWitnessConstraint}=LinearWitnessConstraint[],
    rank_tolerance::Real=sqrt(eps(Float64)),
    active_tolerance::Real=1e-7,
    validation_tolerance::Real=1e-7,
    optimizer_factory=Clarabel.Optimizer,
)
    bound = _finite_float(effect_bound, "effect_bound")
    _finite_positive(active_tolerance, "active_tolerance")
    compiled_constraints = _compile_system(cell, constraints)
    program = _compile_objective(cell, compiled_constraints, objective)
    objective_vector = program.objective_vector
    system = _with_effect_bound(
        program.system, objective_vector, objective, bound)
    subspace = _parameter_subspace(
        cell, system.Aeq; rank_tolerance=rank_tolerance)
    row_norms = _parameter_row_norms(cell, system, subspace)

    # A zero-dimensional parameter fiber has radius exactly zero by contract,
    # never +Inf. Re-optimize effect only to retain a deterministic witness.
    if subspace.dimension == 0
        raw = _solve_lp(
            system.Aeq, system.beq, system.Aineq, system.bineq,
            objective_vector, program.sense;
            optimizer_factory=optimizer_factory,
            validation_tolerance=validation_tolerance,
        )
        raw.status == OPTIMAL || return ConditionalMarginResult(
            raw.status, nothing, nothing, nothing, subspace, ActiveRowResidual[], bound,
            raw.solver_status, raw.message,
        )
        full_solution = something(raw.solution)
        solution = full_solution[1:program.base_variable_count]
        active, dual_note = _active_rows(
            system, full_solution; radius=0.0, parameter_row_norms=row_norms,
            tolerance=active_tolerance, equality_duals=raw.equality_duals,
            inequality_duals=raw.inequality_duals, objective_sense=program.sense,
        )
        return ConditionalMarginResult(
            OPTIMAL, _effect_value(cell, objective, solution), 0.0,
            solution, subspace, active,
            bound, raw.solver_status,
            "optimal with the explicit zero-dimensional parameter-margin convention" *
            (isempty(dual_note) ? "" : "; $dual_note"),
        )
    end

    variable_count = length(objective_vector)
    augmented_Aeq = hcat(system.Aeq, zeros(Float64, size(system.Aeq, 1)))
    augmented_Aineq = hcat(system.Aineq, row_norms)
    augmented_bineq = copy(system.bineq)
    # r >= 0
    augmented_Aineq = vcat(
        augmented_Aineq,
        reshape(vcat(zeros(Float64, variable_count), -1.0), 1, :),
    )
    push!(augmented_bineq, 0.0)
    radius_objective = vcat(zeros(Float64, variable_count), 1.0)
    raw = _solve_lp(
        augmented_Aeq, system.beq, augmented_Aineq, augmented_bineq,
        radius_objective, :maximize;
        optimizer_factory=optimizer_factory,
        validation_tolerance=validation_tolerance,
    )
    raw.status == OPTIMAL || return ConditionalMarginResult(
        raw.status, nothing, nothing, nothing, subspace, ActiveRowResidual[], bound,
        raw.solver_status, raw.message,
    )
    augmented_solution = something(raw.solution)
    full_solution = augmented_solution[1:variable_count]
    solution = full_solution[1:program.base_variable_count]
    radius = augmented_solution[end]
    radius >= -Float64(validation_tolerance) || return ConditionalMarginResult(
        NUMERICAL_ERROR, nothing, nothing, nothing, subspace, ActiveRowResidual[], bound,
        raw.solver_status, "solver returned a negative parameter radius",
    )
    radius = max(0.0, radius)
    active, dual_note = _active_rows(
        system, full_solution; radius=radius, parameter_row_norms=row_norms,
        tolerance=active_tolerance, equality_duals=raw.equality_duals,
        inequality_duals=raw.inequality_duals, objective_sense=:maximize,
        radius_is_variable=true,
    )
    return ConditionalMarginResult(
        OPTIMAL, _effect_value(cell, objective, solution), radius,
        solution, subspace, active, bound, raw.solver_status,
        isempty(dual_note) ? raw.message : "$(raw.message); $dual_note",
    )
end

function _effect_bound(
    best_effect::Float64,
    objective::AbstractWitnessObjective,
    epsilon::Float64,
)
    return _objective_sense(objective) == :maximize ?
        best_effect - epsilon : best_effect + epsilon
end

function optimize_fixed_cell(
    cell::DesignabilityCellGeometry,
    objective::AbstractWitnessObjective;
    constraints::AbstractVector{<:LinearWitnessConstraint}=LinearWitnessConstraint[],
    effect_tolerance::Real=0.0,
    minimum_parameter_margin::Real=0.0,
    rank_tolerance::Real=sqrt(eps(Float64)),
    active_tolerance::Real=1e-7,
    validation_tolerance::Real=1e-7,
    optimizer_factory=Clarabel.Optimizer,
)
    epsilon = _finite_nonnegative(effect_tolerance, "effect_tolerance")
    primary = maximize_effect(
        cell, objective; constraints=constraints,
        minimum_parameter_margin=minimum_parameter_margin,
        rank_tolerance=rank_tolerance, active_tolerance=active_tolerance,
        validation_tolerance=validation_tolerance, optimizer_factory=optimizer_factory,
    )
    primary.status == OPTIMAL || return CellOptimizationResult(
        primary.status, cell.path_identity, copy(cell.witness_identity),
        primary, nothing, false,
    )
    bound = _effect_bound(something(primary.effect), objective, epsilon)
    margin = conditional_parameter_margin(
        cell, objective, bound; constraints=constraints, rank_tolerance=rank_tolerance,
        active_tolerance=active_tolerance, validation_tolerance=validation_tolerance,
        optimizer_factory=optimizer_factory,
    )
    return CellOptimizationResult(
        margin.status, cell.path_identity, copy(cell.witness_identity),
        primary, margin, true,
    )
end

function _effect_better(
    left::Float64,
    right::Float64,
    objective::AbstractWitnessObjective,
)
    return _objective_sense(objective) == :maximize ? left > right : left < right
end

function _effect_near_global(
    value::Float64,
    global_effect::Float64,
    objective::AbstractWitnessObjective,
    epsilon::Float64,
    comparison_tolerance::Float64,
)
    if _objective_sense(objective) == :maximize
        return value >= global_effect - epsilon - comparison_tolerance
    end
    return value <= global_effect + epsilon + comparison_tolerance
end

"""
    optimize_cell_union(cells, objective; effect_tolerance=0, ...)

Run the primary effect LP for every supplied cell, compute one global best
effect, and only then maximize parameter margin in the *global* epsilon-near
set. This ordering is deliberate: cell-local lexicographic solves followed by
pairwise comparison do not implement a global epsilon policy on a disconnected
union.
"""
function optimize_cell_union(
    cells::AbstractVector{<:DesignabilityCellGeometry},
    objective::AbstractWitnessObjective;
    constraints::AbstractVector{<:LinearWitnessConstraint}=LinearWitnessConstraint[],
    effect_tolerance::Real=0.0,
    minimum_parameter_margin::Real=0.0,
    rank_tolerance::Real=sqrt(eps(Float64)),
    active_tolerance::Real=1e-7,
    validation_tolerance::Real=1e-7,
    comparison_tolerance::Real=1e-7,
    optimizer_factory=Clarabel.Optimizer,
    cancel_check::Function=() -> nothing,
)
    isempty(cells) && throw(ArgumentError("cells must not be empty"))
    epsilon = _finite_nonnegative(effect_tolerance, "effect_tolerance")
    comparison = _finite_positive(comparison_tolerance, "comparison_tolerance")

    primary_results = PrimaryEffectResult[]
    for cell in cells
        cancel_check()
        push!(primary_results, maximize_effect(
            cell, objective; constraints=constraints,
            minimum_parameter_margin=minimum_parameter_margin,
            rank_tolerance=rank_tolerance, active_tolerance=active_tolerance,
            validation_tolerance=validation_tolerance,
            optimizer_factory=optimizer_factory,
        ))
    end
    initial = CellOptimizationResult[
        CellOptimizationResult(
            result.status, cells[idx].path_identity, copy(cells[idx].witness_identity),
            result, nothing, false,
        ) for (idx, result) in enumerate(primary_results)
    ]

    any(result -> result.status == UNBOUNDED, primary_results) &&
        return UnionOptimizationResult(
            UNBOUNDED, nothing, nothing, nothing, initial, epsilon,
            "at least one evaluated cell has an unbounded primary effect",
        )
    any(result -> result.status == NUMERICAL_ERROR, primary_results) &&
        return UnionOptimizationResult(
            NUMERICAL_ERROR, nothing, nothing, nothing, initial, epsilon,
            "at least one evaluated cell has an unresolved numerical error",
        )
    optimal_indices = findall(result -> result.status == OPTIMAL, primary_results)
    isempty(optimal_indices) && return UnionOptimizationResult(
        INFEASIBLE, nothing, nothing, nothing, initial, epsilon,
        "all evaluated cells are infeasible",
    )

    global_effect = something(primary_results[first(optimal_indices)].effect)
    for idx in Iterators.drop(optimal_indices, 1)
        value = something(primary_results[idx].effect)
        _effect_better(value, global_effect, objective) && (global_effect = value)
    end
    global_bound = _effect_bound(global_effect, objective, epsilon)
    margin_indices = [
        idx for idx in optimal_indices if _effect_near_global(
            something(primary_results[idx].effect), global_effect, objective,
            epsilon, comparison,
        )
    ]

    margin_results = Dict{Int, ConditionalMarginResult}()
    for idx in margin_indices
        cancel_check()
        margin_results[idx] = conditional_parameter_margin(
            cells[idx], objective, global_bound; constraints=constraints,
            rank_tolerance=rank_tolerance, active_tolerance=active_tolerance,
            validation_tolerance=validation_tolerance,
            optimizer_factory=optimizer_factory,
        )
    end
    cancel_check()
    combined = CellOptimizationResult[
        if haskey(margin_results, idx)
            margin = margin_results[idx]
            CellOptimizationResult(
                margin.status, cells[idx].path_identity, copy(cells[idx].witness_identity),
                primary_results[idx], margin, true,
            )
        else
            initial[idx]
        end for idx in eachindex(cells)
    ]

    any(result -> result.status == UNBOUNDED, values(margin_results)) &&
        return UnionOptimizationResult(
            UNBOUNDED, global_effect, nothing, nothing, combined, epsilon,
            "parameter margin is unbounded within the global epsilon-near effect set",
        )
    any(result -> result.status == NUMERICAL_ERROR, values(margin_results)) &&
        return UnionOptimizationResult(
            NUMERICAL_ERROR, global_effect, nothing, nothing, combined, epsilon,
            "a global epsilon-near cell has an unresolved margin-solve error",
        )
    any(result -> result.status == INFEASIBLE, values(margin_results)) &&
        return UnionOptimizationResult(
            NUMERICAL_ERROR, global_effect, nothing, nothing, combined, epsilon,
            "a primary epsilon-near cell became infeasible in the secondary solve",
        )

    secondary_indices = [idx for idx in margin_indices
                         if margin_results[idx].status == OPTIMAL]
    isempty(secondary_indices) && return UnionOptimizationResult(
        NUMERICAL_ERROR, global_effect, nothing, nothing, combined, epsilon,
        "no global epsilon-near cell produced a secondary solution",
    )
    selected_index = first(secondary_indices)
    for idx in Iterators.drop(secondary_indices, 1)
        candidate = margin_results[idx]
        incumbent = margin_results[selected_index]
        candidate_margin = something(candidate.parameter_margin)
        incumbent_margin = something(incumbent.parameter_margin)
        if candidate_margin > incumbent_margin + comparison
            selected_index = idx
        elseif abs(candidate_margin - incumbent_margin) <= comparison
            candidate_effect = something(candidate.effect)
            incumbent_effect = something(incumbent.effect)
            _effect_better(candidate_effect, incumbent_effect, objective) &&
                (selected_index = idx)
        end
    end
    selected = combined[selected_index]
    return UnionOptimizationResult(
        OPTIMAL, global_effect, selected_index, selected, combined, epsilon,
        "globally epsilon-lexicographic optimum over all supplied cells",
    )
end

"""
    directional_request_interval(cell, reference_witness, direction; constraints=[])

Compute the exact one-dimensional interval of `alpha` values for which
`tau = reference_witness + alpha * direction` intersects one compiled cell.
The supplied direction is never normalized: the returned endpoints use the
caller's original alpha units, and both the original vector and its L2 norm are
retained in the result.
"""
function directional_request_interval(
    cell::DesignabilityCellGeometry,
    reference_witness::AbstractVector,
    direction::AbstractVector;
    constraints::AbstractVector{<:LinearWitnessConstraint}=LinearWitnessConstraint[],
    validation_tolerance::Real=1e-7,
    optimizer_factory=Clarabel.Optimizer,
)
    reference = _finite_vector(reference_witness, "reference_witness")
    direction_values = _finite_vector(direction, "direction")
    witness_count = length(cell.witness_coordinates)
    length(reference) == witness_count || throw(ArgumentError(
        "reference_witness must contain exactly $witness_count values"))
    length(direction_values) == witness_count || throw(ArgumentError(
        "direction must contain exactly $witness_count values"))
    direction_norm = norm(direction_values)
    isfinite(direction_norm) || throw(ArgumentError("direction L2 norm must be finite"))
    direction_norm > 0 || throw(ArgumentError("direction must not be the zero vector"))

    base = _compile_system(cell, constraints)
    base_variable_count = length(cell.coordinates)
    Aeq = hcat(base.Aeq, zeros(Float64, size(base.Aeq, 1)))
    beq = copy(base.beq)
    equality_ids = copy(base.equality_row_ids)
    equality_kinds = copy(base.equality_row_kinds)
    used_ids = Set(vcat(equality_ids, base.inequality_row_ids))
    coordinate_indices = _coordinate_index(cell)
    for (idx, coordinate) in enumerate(cell.witness_coordinates)
        row_id = "directional_request:$(String(coordinate))"
        row_id in used_ids && throw(ArgumentError(
            "directional equality row ID $row_id collides with an existing row ID"))
        push!(used_ids, row_id)
        row = zeros(Float64, base_variable_count + 1)
        row[coordinate_indices[coordinate]] = 1.0
        row[end] = -direction_values[idx]
        Aeq = vcat(Aeq, reshape(row, 1, :))
        push!(beq, reference[idx])
        push!(equality_ids, row_id)
        push!(equality_kinds, :directional_request)
    end
    Aineq = hcat(base.Aineq, zeros(Float64, size(base.Aineq, 1)))
    objective = vcat(zeros(Float64, base_variable_count), 1.0)
    lower = _solve_lp(
        Aeq, beq, Aineq, base.bineq, objective, :minimize;
        optimizer_factory=optimizer_factory,
        validation_tolerance=validation_tolerance,
    )
    upper = _solve_lp(
        Aeq, beq, Aineq, base.bineq, objective, :maximize;
        optimizer_factory=optimizer_factory,
        validation_tolerance=validation_tolerance,
    )

    status = if lower.status == NUMERICAL_ERROR || upper.status == NUMERICAL_ERROR
        NUMERICAL_ERROR
    elseif lower.status == INFEASIBLE && upper.status == INFEASIBLE
        INFEASIBLE
    elseif lower.status == INFEASIBLE || upper.status == INFEASIBLE
        NUMERICAL_ERROR
    elseif lower.status == UNBOUNDED || upper.status == UNBOUNDED
        UNBOUNDED
    elseif lower.status == OPTIMAL && upper.status == OPTIMAL
        OPTIMAL
    else
        NUMERICAL_ERROR
    end
    alpha_min = lower.status == OPTIMAL ? lower.objective : nothing
    alpha_max = upper.status == OPTIMAL ? upper.objective : nothing
    lower_solution = lower.status == OPTIMAL ?
        something(lower.solution)[1:base_variable_count] : nothing
    upper_solution = upper.status == OPTIMAL ?
        something(upper.solution)[1:base_variable_count] : nothing
    message = "directional interval lower=$(status_name(lower.status)), " *
              "upper=$(status_name(upper.status)); " *
              "lower solver: $(lower.message); upper solver: $(upper.message)"
    return DirectionalIntervalResult(
        status, alpha_min, alpha_max, lower.status, upper.status,
        reference, direction_values, direction_norm,
        copy(cell.witness_coordinates), cell.path_identity,
        copy(cell.witness_identity), lower_solution, upper_solution, message,
    )
end

end # module ROPShapeOptimization
