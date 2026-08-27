using Test
using LinearAlgebra
using BindingAndCatalysis

if !isdefined(BindingAndCatalysis, :ROScientificSourceIdentity)
    Base.include(BindingAndCatalysis,
        joinpath(@__DIR__, "..", "src", "rop", "ro_field_uncertainty.jl"))
end

const _ROU = BindingAndCatalysis

struct _ROUTestCancelled <: Exception end

struct _ROUExternalPopulationPolicy <: _ROU.ROPopulationPolicy end

struct _ROUMatrixReadProbe <: AbstractMatrix{Float64}
    dimensions::Tuple{Int,Int}
    reads::Base.RefValue{Int}
end

Base.size(probe::_ROUMatrixReadProbe) = probe.dimensions
function Base.getindex(probe::_ROUMatrixReadProbe, indices...)
    probe.reads[] += 1
    return 0.0
end

struct _ROUVectorReadProbe{T} <: AbstractVector{T}
    count::Int
    reads::Base.RefValue{Int}
    value::T
end

struct _ROUUnsizedReadProbe{T}
    reads::Base.RefValue{Int}
    value::T
end

struct _ROUKeyMasquerade
    visible::NamedTuple
    hidden::Bool
end

Base.keys(value::_ROUKeyMasquerade) = keys(getfield(value, :visible))
function Base.getproperty(value::_ROUKeyMasquerade, name::Symbol)
    name in (:visible, :hidden) && return getfield(value, name)
    return getproperty(getfield(value, :visible), name)
end

Base.IteratorSize(::Type{<:_ROUUnsizedReadProbe}) = Base.SizeUnknown()
function Base.iterate(probe::_ROUUnsizedReadProbe, state::Int=1)
    probe.reads[] += 1
    return probe.value, state + 1
end


Base.size(probe::_ROUVectorReadProbe) = (probe.count,)
function Base.getindex(probe::_ROUVectorReadProbe, index::Int)
    probe.reads[] += 1
    return probe.value
end

function _forge_rou(result; kwargs...)
    names = fieldnames(typeof(result))
    values = ntuple(length(names)) do index
        name = names[index]
        return haskey(kwargs, name) ? kwargs[name] :
            getfield(result, name)
    end
    return typeof(result)(_ROU._ROU_CONSTRUCTION_TOKEN, values...)
end

function _rou_captured_error(action)
    try
        action()
        return nothing
    catch error
        return error
    end
end

function _rou_policy_payload_error(payload, limits)
    return _rou_captured_error() do
        _ROU._rou_validate_population_policy_payload(payload; limits)
    end
end

_rou_hash(character::AbstractString) = repeat(character, 64)

_rou_replicate_coordinates(count::Int) =
    hcat(collect(1.0:count), collect(101.0:(100 + count)))

function _uncertainty_identity(
    outputs::Vector{String};
    parameters=["log_k1", "log_k2"],
    parameter_units=fill("log10 molar", length(parameters)),
    parameter_scales=fill("log10", length(parameters)),
)
    return _ROU.ROFieldEvidenceIdentity(
        input_order=["u1", "u2"],
        input_units=["log10 molar", "log10 molar"],
        input_scales=["log10", "log10"],
        parameter_order=parameters,
        parameter_units=parameter_units,
        parameter_scales=parameter_scales,
        output_order=outputs,
        output_units=fill("log10 molar", length(outputs)),
        output_scales=fill("log10", length(outputs)),
    )
end

function _uncertainty_source(
    outputs::Vector{String};
    local_coordinates=[-1.0, 0.5],
    nominal_parameters=[0.1, -0.2],
    field_hash=_rou_hash("a"),
    request_hash=_rou_hash("b"),
    network_hash=_rou_hash("c"),
    solver_revision="solver/newton-v3",
    algorithm_revision="uncertainty/svd-v1",
)
    return _ROU.ROScientificSourceIdentity(
        source_field_sha256=field_hash,
        field_request_sha256=request_hash,
        network_sha256=network_hash,
        local_coordinates=local_coordinates,
        nominal_parameters=nominal_parameters,
        observation_schedule=["$(output)@t=$(index)" for
            (index, output) in enumerate(outputs)],
        solver_revision=solver_revision,
        algorithm_revision=algorithm_revision,
    )
end


function _ensemble_policy(
    draw_count::Int;
    uncertainty_class=:parametric,
    policy_id="ensemble-policy",
    policy_revision="v1",
    distribution_family=:lognormal,
    distribution_specification=(seed=17,),
    sampling_revision="sampler/v1",
    cancel_check=()->nothing,
    limits=_ROU.ROUncertaintyLimits(),
)
    return _ROU.ROEnsemblePopulationPolicy(;
        uncertainty_class,
        policy_id,
        policy_revision,
        draw_count,
        distribution_family,
        distribution_specification,
        sampling_revision,
        cancel_check,
        limits,
    )
end

function _interval_policy(
    population_count::Int;
    uncertainty_class=:experimental,
    policy_id="interval-policy",
    policy_revision="v1",
    interval_semantics=:bounded_explicit_sensor_coordinate_population,
    enumeration=:explicit_complete_coordinate_population,
    coordinate_ids=("sensor-lower", "sensor-upper"),
    coordinate_units=("log10 molar", "log10 molar"),
    coordinate_lower=(1.0, 101.0),
    coordinate_upper=(2.0, 102.0),
    interval_definition_sha256=nothing,
    interval_certificate_sha256=_rou_hash("9"),
    interval_specification=(confidence_policy=:deterministic_sensor_limit,),
)
    return _ROU.ROCertifiedIntervalPopulationPolicy(;
        uncertainty_class,
        policy_id,
        policy_revision,
        population_count,
        enumeration,
        interval_semantics,
        coordinate_ids,
        coordinate_units,
        coordinate_lower,
        coordinate_upper,
        interval_definition_sha256,
        interval_certificate_sha256,
        interval_specification,
    )
end

function _local_analysis(
    sensitivity;
    outputs=["y$(index)" for index in axes(sensitivity, 1)],
    kwargs...,
)
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    return _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(size(sensitivity, 1)),
        kwargs...,
    )
end

@testset "mandatory provenance and whitened-sensitivity SVD" begin
    sensitivity = [1.0 0.0; 0.0 2.0; 1.0 1.0]
    weights = [1.0, 0.25, 1.0]
    outputs = ["y1@a", "y1@b", "y2@a"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    result = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=weights,
    )

    whitened = sensitivity .* sqrt.(weights)
    singular_values = svdvals(whitened)
    expected_fim = transpose(whitened) * whitened
    @test result.schema_version == _ROU.RO_LOCAL_IDENTIFIABILITY_VERSION
    @test result.status == :complete
    @test result.rank_method == :whitened_sensitivity_svd
    @test result.whitened_sensitivity_singular_values ≈ singular_values atol=1e-14
    @test result.fim_singular_values ≈ singular_values .^ 2 atol=1e-14
    @test result.fim ≈ expected_fim atol=1e-14
    @test result.rank_lower_bound == 2
    @test result.rank_upper_bound == 2
    @test result.numerical_rank == 2
    @test result.rank_status == :full_rank
    @test result.condition_status == :finite
    @test result.condition_number ≈ cond(expected_fim) rtol=1e-12
    @test result.practical_parameter_covariance ≈ inv(expected_fim) atol=1e-13
    @test result.practical_parameter_standard_errors ≈
        sqrt.(diag(inv(expected_fim))) atol=1e-13
    @test result.structural_local_identifiability ==
        :full_rank_local_sensitivity_under_declared_model
    @test result.evidence_scope == :local_declared_observation_model_only
    @test !result.global_identifiability_claimed
    @test !result.causal_claimed
    @test !result.experimentally_validated

    @test result.source === source
    @test length(result.identity_sha256) == 64
    @test result.identity_payload.scientific_source.source_field_sha256 ==
        _rou_hash("a")
    @test result.identity_payload.scientific_source.local_coordinates ==
        (-1.0, 0.5)
    @test result.identity_payload.scientific_source.nominal_parameters ==
        (0.1, -0.2)
    @test result.identity_payload.scientific_source.observation_schedule ==
        ("y1@a@t=1", "y1@b@t=2", "y2@a@t=3")
    @test result.identity_payload.scientific_source.solver_revision ==
        "solver/newton-v3"
    @test result.identity_payload.scientific_source.algorithm_revision ==
        "uncertainty/svd-v1"
    @test result.identity_payload.numerical_rank_policy.method ==
        :whitened_sensitivity_svd
    @test _ROU.validate_ro_local_identifiability_analysis(result)
    tampered_local = deepcopy(result)
    tampered_local.fim[1, 1] += 1.0
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(tampered_local)
    end
    public_result_fields = Tuple(
        getfield(result, name) for name in fieldnames(typeof(result)))
    @test_throws MethodError typeof(result)(public_result_fields...)
    public_partition_fields = Tuple(
        getfield(result.uncertainty, name) for
        name in fieldnames(typeof(result.uncertainty)))
    @test_throws MethodError typeof(result.uncertainty)(
        public_partition_fields...)
    forged_partition = _forge_rou(
        result.uncertainty;
        experimental_uncertainty=:experimentally_proven,
    )
    forged_experimental = _forge_rou(
        result; uncertainty=forged_partition)
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_experimental)
    end
    forged_method = _forge_rou(result; rank_method=:forged_rank_method)
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_method)
    end
    forged_threshold = _forge_rou(
        result; rank_threshold_high=2 * result.rank_threshold_high)
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_threshold)
    end
    forged_schema = _forge_rou(result; schema_version="forged/schema")
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_schema)
    end
    unknown_local_payload = merge(
        result.identity_payload,
        (global_identifiability_claimed=true,),
    )
    unknown_local = _forge_rou(
        result;
        identity_payload=unknown_local_payload,
        identity_sha256=_ROU._rou_sha256(unknown_local_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(unknown_local)
    end
    unknown_rank_policy = merge(
        result.identity_payload.numerical_rank_policy,
        (experimentally_proven=true,),
    )
    unknown_nested_local_payload = merge(
        result.identity_payload,
        (numerical_rank_policy=unknown_rank_policy,),
    )
    unknown_nested_local = _forge_rou(
        result;
        identity_payload=unknown_nested_local_payload,
        identity_sha256=_ROU._rou_sha256(unknown_nested_local_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(
            unknown_nested_local)
    end
    unknown_coordinate_identity = merge(
        result.identity_payload.coordinate_identity,
        (unexpected_coordinate_owner=true,),
    )
    unknown_coordinate_payload = merge(
        result.identity_payload,
        (coordinate_identity=unknown_coordinate_identity,),
    )
    unknown_coordinate = _forge_rou(
        result;
        identity_payload=unknown_coordinate_payload,
        identity_sha256=_ROU._rou_sha256(unknown_coordinate_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(unknown_coordinate)
    end
    unknown_scientific_source = merge(
        result.identity_payload.scientific_source,
        (unexpected_source_owner=true,),
    )
    unknown_source_payload = merge(
        result.identity_payload,
        (scientific_source=unknown_scientific_source,),
    )
    unknown_source = _forge_rou(
        result;
        identity_payload=unknown_source_payload,
        identity_sha256=_ROU._rou_sha256(unknown_source_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(unknown_source)
    end
    negative_zero_sensitivity_rows = collect(
        result.identity_payload.sensitivity_rows)
    negative_zero_sensitivity_rows[1] = (1.0, -0.0)
    negative_zero_local_payload = merge(
        result.identity_payload,
        (sensitivity_rows=Tuple(negative_zero_sensitivity_rows),),
    )
    negative_zero_local = _forge_rou(
        result;
        identity_payload=negative_zero_local_payload,
        identity_sha256=_ROU._rou_sha256(negative_zero_local_payload),
    )
    @test negative_zero_local.identity_sha256 != result.identity_sha256
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(negative_zero_local)
    end
    forged_source_payload = merge(
        result.identity_payload,
        (scientific_source_sha256=_rou_hash("f"),),
    )
    forged_source = _forge_rou(
        result;
        identity_payload=forged_source_payload,
        identity_sha256=_ROU._rou_sha256(forged_source_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_source)
    end
    forged_fim = copy(result.fim)
    forged_fim[1, 1] += 1.0
    forged_fim_snapshot = merge(
        result.identity_payload.result_snapshot,
        (fim=_ROU._rou_matrix_payload(forged_fim),),
    )
    forged_fim_payload = merge(
        result.identity_payload,
        (result_snapshot=forged_fim_snapshot,),
    )
    forged_consistent_fim = _forge_rou(
        result;
        fim=forged_fim,
        identity_payload=forged_fim_payload,
        identity_sha256=_ROU._rou_sha256(forged_fim_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_local_identifiability_analysis(forged_consistent_fim)
    end

    repeated = _ROU.analyze_ro_local_identifiability(
        copy(sensitivity);
        identity,
        source,
        observation_weights=copy(weights),
    )
    @test repeated.identity_payload == result.identity_payload
    @test repeated.identity_sha256 == result.identity_sha256
    changed_source = _uncertainty_source(
        outputs; network_hash=_rou_hash("d"))
    changed = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source=changed_source,
        observation_weights=weights,
    )
    @test changed.identity_sha256 != result.identity_sha256

    identity_fields = Tuple(
        getfield(identity, name) for name in fieldnames(typeof(identity)))
    @test_throws MethodError typeof(identity)(identity_fields...)

    @test_throws UndefKeywordError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        observation_weights=weights,
    )
    @test_throws ArgumentError _ROU.ROScientificSourceIdentity(
        source_field_sha256="not-a-hash",
        field_request_sha256=_rou_hash("b"),
        network_sha256=_rou_hash("c"),
        local_coordinates=[0.0, 0.0],
        nominal_parameters=[0.0, 0.0],
        observation_schedule=outputs,
        solver_revision="solver",
        algorithm_revision="algorithm",
    )
    @test_throws ArgumentError _ROU.ROScientificSourceIdentity(
        "x",
        "y",
        "z",
        (0.0, 0.0),
        (0.0, 0.0),
        ("t1", "t2", "t3"),
        "",
        "",
    )
    @test_throws ArgumentError _ROU.ROScientificSourceIdentity(
        _rou_hash("a"),
        _rou_hash("b"),
        _rou_hash("c"),
        (NaN, 0.0),
        (0.0, 0.0),
        ("t1", "t2", "t3"),
        "",
        "algorithm",
    )
end

@testset "backward-error floor owns the unresolved rank grey zone" begin
    sensitivity = Matrix(Diagonal([1.0, 6e-10]))
    outputs = ["y1", "y2"]
    result = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity=_uncertainty_identity(outputs),
        source=_uncertainty_source(outputs),
        observation_weights=ones(2),
        rank_absolute_low=0.0,
        rank_absolute_high=1e-30,
        rank_relative_low=0.0,
        rank_relative_high=1e-30,
        backward_error_multiplier=1e6,
    )
    @test result.whitened_sensitivity_singular_values == [1.0, 6e-10]
    @test result.requested_rank_threshold_high == 1e-30
    @test result.backward_error_floor ≈ 2e6 * eps(Float64)
    @test result.rank_threshold_low == result.backward_error_floor
    @test result.rank_threshold_high == 2 * result.backward_error_floor
    @test result.rank_threshold_floor_dominated
    @test result.rank_lower_bound == 1
    @test result.rank_upper_bound == 2
    @test result.numerical_rank === nothing
    @test result.rank_status == :ambiguous_threshold
    @test result.structural_local_identifiability == :ambiguous_numerical_rank
    @test result.practical_precision_status == :unavailable_ambiguous_rank
    @test result.identity_payload.numerical_rank_policy.requested_high == 1e-30
    @test result.identity_payload.numerical_rank_policy.effective_high ==
        result.rank_threshold_high
    @test result.identity_payload.numerical_rank_policy.backward_error_floor_dominated

    changed_multiplier = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity=_uncertainty_identity(outputs),
        source=_uncertainty_source(outputs),
        observation_weights=ones(2),
        rank_absolute_low=0.0,
        rank_absolute_high=1e-30,
        rank_relative_low=0.0,
        rank_relative_high=1e-30,
        backward_error_multiplier=2e6,
    )
    @test changed_multiplier.identity_sha256 != result.identity_sha256

    deficient = _local_analysis([1.0 2.0; 2.0 4.0])
    @test deficient.rank_status == :rank_deficient
    @test deficient.rank_lower_bound == 1
    @test deficient.rank_upper_bound == 1
    @test deficient.numerical_rank == 1
    @test deficient.practical_parameter_covariance === nothing
    @test_throws ArgumentError _local_analysis(
        sensitivity;
        rank_relative_low=1e-5,
        rank_relative_high=1e-6,
    )
    @test_throws ArgumentError _local_analysis(
        sensitivity;
        backward_error_multiplier=0.5,
    )
end

@testset "scale-relative covariance admission and finite whitening" begin
    sensitivity = Matrix{Float64}(I, 2, 2)
    outputs = ["y1", "y2"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    covariance = [2.0 0.5; 0.5 1.0]
    result = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=covariance,
    )
    @test result.fim ≈ inv(covariance) atol=1e-14
    @test result.rank_status == :full_rank
    @test result.uncertainty.experimental_uncertainty ==
        :observation_covariance_supplied
    @test result.identity_payload.observation_model.kind ==
        :observation_covariance

    tiny_covariance = Matrix(Diagonal([1e-200, 2e-200]))
    tiny = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=tiny_covariance,
    )
    @test tiny.fim ≈ inv(tiny_covariance) rtol=1e-12
    @test tiny.rank_status == :full_rank
    @test tiny.identity_payload.observation_model.admission_policy.covariance_scale ≈
        2e-200

    @test_throws DimensionMismatch _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=ones(2, 3),
    )
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=[1.0 0.1; 0.2 1.0],
    )
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=[1.0 2.0; 2.0 1.0],
    )
    @test_throws _ROU.ROCovarianceNumericallyAmbiguous begin
        _ROU.analyze_ro_local_identifiability(
            sensitivity;
            identity,
            source,
            observation_covariance=[1.0 0.0; 0.0 -1e-16],
        )
    end
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=[1.0 0.0; 0.0 0.0],
    )
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=[1.0, 0.0],
    )
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_covariance=covariance,
        covariance_symmetry_tolerance=1e-4,
    )
    masked_admission = _ROUKeyMasquerade(
        result.identity_payload.observation_model.admission_policy,
        true,
    )
    masked_model = merge(
        result.identity_payload.observation_model,
        (admission_policy=masked_admission,),
    )
    masked_payload = merge(
        result.identity_payload,
        (observation_model=masked_model,),
    )
    masked_result = _forge_rou(result; identity_payload=masked_payload)
    masked_error = _rou_captured_error() do
        _ROU.validate_ro_local_identifiability_analysis(masked_result)
    end
    @test masked_error isa ArgumentError
    @test occursin("must be a NamedTuple", sprint(showerror, masked_error))
end

@testset "typed numerical gaps and structural ambiguity remain unknown" begin
    sensitivity = [1.0 0.0; NaN NaN; 0.0 1.0]
    validity = BitVector([true, false, true])
    outputs = ["y1", "failed_y", "y2"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    gap_reasons = Union{Nothing,Symbol}[
        nothing, :solver_nonconvergence, nothing]
    partial = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(3),
        observation_validity=validity,
        observation_gap_reasons=gap_reasons,
    )
    @test partial.fim == Matrix{Float64}(I, 2, 2)
    @test partial.rank_status == :full_rank
    @test partial.status == :unknown_numerical_gap
    @test partial.structural_local_identifiability == :unknown_numerical_gap
    @test partial.practical_precision_status == :unknown_numerical_gap
    @test partial.practical_parameter_covariance === nothing
    @test partial.uncertainty.numerical_uncertainty ==
        :explicit_typed_invalid_gaps
    @test partial.uncertainty.invalid_gap_indices == (2,)
    @test partial.uncertainty.invalid_gap_reasons == (:solver_nonconvergence,)
    @test partial.identity_payload.observation_gap_reasons[2] ==
        :solver_nonconvergence
    @test partial.identity_payload.sensitivity_rows[2] === nothing

    other_reason = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(3),
        observation_validity=validity,
        observation_gap_reasons=Union{Nothing,Symbol}[
            nothing, :nonfinite_output, nothing],
    )
    @test other_reason.identity_sha256 != partial.identity_sha256
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(3),
        observation_validity=validity,
    )

    structural = _ROU.analyze_ro_local_identifiability(
        Matrix{Float64}(I, 2, 2);
        identity=_uncertainty_identity(["y1", "y2"]),
        source=_uncertainty_source(["y1", "y2"]),
        observation_weights=ones(2),
        structural_ambiguity_reasons=[:unselected_singular_branch],
    )
    @test structural.status == :unknown_structural_ambiguity
    @test structural.structural_local_identifiability ==
        :unknown_structural_ambiguity
    @test structural.uncertainty.structural_ambiguity_reasons ==
        (:unselected_singular_branch,)
end

@testset "delta propagation uses a PSD factor and never clips variance" begin
    jacobian = [1.0 2.0; 3.0 4.0]
    parameter_covariance = [1.0 0.2; 0.2 2.0]
    outputs = ["y1", "y2"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    result = _ROU.propagate_ro_delta_covariance(
        jacobian,
        parameter_covariance;
        identity,
        source,
    )
    expected = jacobian * parameter_covariance * transpose(jacobian)
    @test result.schema_version == _ROU.RO_DELTA_METHOD_UNCERTAINTY_VERSION
    @test result.status == :complete
    @test result.propagation_method == :psd_factor_pushforward
    @test result.parameter_covariance_factor *
        transpose(result.parameter_covariance_factor) ≈
        parameter_covariance atol=1e-13
    @test result.output_covariance_factor *
        transpose(result.output_covariance_factor) ≈ expected atol=1e-13
    @test result.output_covariance ≈ expected atol=1e-13
    @test result.output_standard_deviations ≈ sqrt.(diag(expected)) atol=1e-13
    @test result.output_psd_status in
        (:factor_propagated_psd, :factor_propagated_roundoff_grey_zone)
    @test result.identity_payload.propagation.variance_clipping_performed == false
    @test result.identity_payload.scientific_source.network_sha256 ==
        _rou_hash("c")
    @test result.calibration_status == :not_assessed
    @test !result.causal_claimed
    @test !result.experimentally_validated
    @test _ROU.validate_ro_delta_method_covariance(result)
    delta_fields = Tuple(
        getfield(result, name) for name in fieldnames(typeof(result)))
    @test_throws MethodError typeof(result)(delta_fields...)
    tampered_delta = deepcopy(result)
    tampered_delta.output_covariance[1, 1] += 1.0
    @test_throws ArgumentError begin
        _ROU.validate_ro_delta_method_covariance(tampered_delta)
    end
    forged_covariance = copy(result.output_covariance)
    forged_covariance[1, 1] += 1.0
    forged_delta_snapshot = merge(
        result.identity_payload.result_snapshot,
        (output_covariance_valid_submatrix=
            _ROU._rou_matrix_payload(forged_covariance),),
    )
    forged_delta_payload = merge(
        result.identity_payload,
        (result_snapshot=forged_delta_snapshot,),
    )
    forged_delta = _forge_rou(
        result;
        output_covariance=forged_covariance,
        identity_payload=forged_delta_payload,
        identity_sha256=_ROU._rou_sha256(forged_delta_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_delta_method_covariance(forged_delta)
    end
    unknown_delta_payload = merge(
        result.identity_payload,
        (causal_effect_proven=true,),
    )
    unknown_delta = _forge_rou(
        result;
        identity_payload=unknown_delta_payload,
        identity_sha256=_ROU._rou_sha256(unknown_delta_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_delta_method_covariance(unknown_delta)
    end
    unknown_propagation = merge(
        result.identity_payload.propagation,
        (globally_robust=true,),
    )
    unknown_nested_delta_payload = merge(
        result.identity_payload,
        (propagation=unknown_propagation,),
    )
    unknown_nested_delta = _forge_rou(
        result;
        identity_payload=unknown_nested_delta_payload,
        identity_sha256=_ROU._rou_sha256(unknown_nested_delta_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_delta_method_covariance(unknown_nested_delta)
    end
    masked_delta_admission = _ROUKeyMasquerade(
        result.identity_payload.covariance_admission_policy,
        true,
    )
    masked_delta_admission_payload = merge(
        result.identity_payload,
        (covariance_admission_policy=masked_delta_admission,),
    )
    masked_delta_admission_result = _forge_rou(
        result; identity_payload=masked_delta_admission_payload)
    masked_delta_admission_error = _rou_captured_error() do
        _ROU.validate_ro_delta_method_covariance(
            masked_delta_admission_result)
    end
    @test masked_delta_admission_error isa ArgumentError
    @test occursin(
        "must be a NamedTuple",
        sprint(showerror, masked_delta_admission_error),
    )
    masked_propagation = _ROUKeyMasquerade(
        result.identity_payload.propagation,
        true,
    )
    masked_propagation_payload = merge(
        result.identity_payload,
        (propagation=masked_propagation,),
    )
    masked_propagation_result = _forge_rou(
        result; identity_payload=masked_propagation_payload)
    masked_propagation_error = _rou_captured_error() do
        _ROU.validate_ro_delta_method_covariance(masked_propagation_result)
    end
    @test masked_propagation_error isa ArgumentError
    @test occursin(
        "must be a NamedTuple",
        sprint(showerror, masked_propagation_error),
    )
    negative_zero_admission = merge(
        result.identity_payload.covariance_admission_policy,
        (symmetry_residual=-0.0,),
    )
    negative_zero_delta_payload = merge(
        result.identity_payload,
        (covariance_admission_policy=negative_zero_admission,),
    )
    negative_zero_delta = _forge_rou(
        result;
        identity_payload=negative_zero_delta_payload,
        identity_sha256=_ROU._rou_sha256(negative_zero_delta_payload),
    )
    @test negative_zero_delta.identity_sha256 != result.identity_sha256
    @test_throws ArgumentError begin
        _ROU.validate_ro_delta_method_covariance(negative_zero_delta)
    end

    singular_psd = _ROU.propagate_ro_delta_covariance(
        Matrix{Float64}(I, 2, 2),
        [1.0 0.0; 0.0 0.0];
        identity,
        source,
    )
    @test singular_psd.output_covariance == [1.0 0.0; 0.0 0.0]
    @test singular_psd.output_standard_deviations == [1.0, 0.0]

    tiny = _ROU.propagate_ro_delta_covariance(
        Matrix{Float64}(I, 2, 2),
        Matrix(Diagonal([1e-200, 2e-200]));
        identity,
        source,
    )
    @test tiny.output_covariance ≈ Matrix(Diagonal([1e-200, 2e-200]))
    @test tiny.output_standard_deviations ≈ [1e-100, sqrt(2e-200)]

    partial = _ROU.propagate_ro_delta_covariance(
        [1.0 2.0; NaN NaN],
        parameter_covariance;
        identity,
        source,
        output_validity=BitVector([true, false]),
        output_gap_reasons=Union{Nothing,Symbol}[
            nothing, :solver_nonconvergence],
    )
    @test partial.status == :unknown_numerical_gap
    @test partial.uncertainty.invalid_gap_reasons == (:solver_nonconvergence,)
    @test all(isnan, partial.output_covariance[2, :])
    @test all(isnan, partial.output_covariance[:, 2])
    @test isnan(partial.output_standard_deviations[2])
    @test partial.identity_payload.output_gap_reasons[2] ==
        :solver_nonconvergence
    @test_throws ArgumentError _ROU.propagate_ro_delta_covariance(
        [1.0 2.0; NaN NaN],
        parameter_covariance;
        identity,
        source,
        output_validity=BitVector([true, false]),
    )

    @test_throws _ROU.ROCovarianceNumericallyAmbiguous begin
        _ROU.propagate_ro_delta_covariance(
            Matrix{Float64}(I, 2, 2),
            [1.0 0.0; 0.0 -1e-16];
            identity,
            source,
        )
    end
    @test_throws ArgumentError _ROU.propagate_ro_delta_covariance(
        Matrix{Float64}(I, 2, 2),
        [1.0 0.0; 0.0 -1e-2];
        identity,
        source,
    )

    near_identity = _uncertainty_identity(["near-singular-output"])
    near_source = _uncertainty_source(["near-singular-output"])
    near_jacobian = reshape(
        [inv(sqrt(2.0)), -inv(sqrt(2.0))], 1, 2)
    delta_scale = 1e-8
    near_covariance = [
        0.5 + delta_scale / 2 0.5 - delta_scale / 2
        0.5 - delta_scale / 2 0.5 + delta_scale / 2
    ]
    near_result = _ROU.propagate_ro_delta_covariance(
        near_jacobian,
        near_covariance;
        identity=near_identity,
        source=near_source,
        covariance_psd_tolerance=0.0,
    )
    @test _ROU.validate_ro_delta_method_covariance(near_result)
    @test near_result.output_covariance ≈
        near_result.output_covariance_factor *
        transpose(near_result.output_covariance_factor)
    @test near_result.output_standard_deviations[1] ≈
        norm(near_result.output_covariance_factor[1, :])
end

@testset "synthetic coverage is computed evidence, never fake calibration" begin
    truth = [0.0 0.0; 1.0 1.0; 2.0 2.0; 3.0 3.0]
    lower = [-0.1 -0.1; 0.5 0.5; 1.5 2.1; 3.1 2.5]
    upper = [0.1 0.1; 1.5 1.5; 2.5 2.5; 3.5 3.5]
    coverage = _ROU.evaluate_ro_synthetic_coverage_fixture(
        truth,
        lower,
        upper;
        fixture_id="linear-recovery-fixture",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=["feature-1", "feature-2"],
        case_ids=["case-$index" for index in 1:4],
    )
    @test coverage.schema_version == _ROU.RO_SYNTHETIC_COVERAGE_VERSION
    @test coverage.status == :complete_synthetic_fixture
    @test coverage.feature_ids == ("feature-1", "feature-2")
    @test coverage.feature_coverage_counts == [3, 3]
    @test coverage.feature_valid_counts == [4, 4]
    @test coverage.feature_coverage == Union{Nothing,Float64}[0.75, 0.75]
    @test coverage.joint_coverage_count == 2
    @test coverage.joint_coverage == 0.5
    @test coverage.calibration_status ==
        :synthetic_coverage_evaluated_not_calibrated
    @test !coverage.experimentally_calibrated
    @test length(coverage.identity_sha256) == 64
    @test _ROU.validate_ro_synthetic_coverage_evidence(coverage)
    coverage_fields = Tuple(
        getfield(coverage, name) for name in fieldnames(typeof(coverage)))
    @test_throws MethodError typeof(coverage)(coverage_fields...)
    tampered_coverage = deepcopy(coverage)
    tampered_coverage.feature_coverage_counts[1] += 1
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(tampered_coverage)
    end
    forged_counts = copy(coverage.feature_coverage_counts)
    forged_counts[1] += 1
    forged_coverage_snapshot = merge(
        coverage.identity_payload.result_snapshot,
        (feature_coverage_counts=Tuple(forged_counts),),
    )
    forged_coverage_payload = merge(
        coverage.identity_payload,
        (result_snapshot=forged_coverage_snapshot,),
    )
    forged_coverage = _forge_rou(
        coverage;
        feature_coverage_counts=forged_counts,
        identity_payload=forged_coverage_payload,
        identity_sha256=_ROU._rou_sha256(forged_coverage_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(forged_coverage)
    end
    forged_experimental = _forge_rou(
        coverage; experimentally_calibrated=true)
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(forged_experimental)
    end
    unknown_coverage_payload = merge(
        coverage.identity_payload,
        (experimentally_proven=true,),
    )
    unknown_coverage = _forge_rou(
        coverage;
        identity_payload=unknown_coverage_payload,
        identity_sha256=_ROU._rou_sha256(unknown_coverage_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(unknown_coverage)
    end
    unknown_coverage_snapshot = merge(
        coverage.identity_payload.result_snapshot,
        (causal_claimed=true,),
    )
    unknown_nested_coverage_payload = merge(
        coverage.identity_payload,
        (result_snapshot=unknown_coverage_snapshot,),
    )
    unknown_nested_coverage = _forge_rou(
        coverage;
        identity_payload=unknown_nested_coverage_payload,
        identity_sha256=_ROU._rou_sha256(
            unknown_nested_coverage_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(
            unknown_nested_coverage)
    end
    negative_zero_truth_rows = collect(coverage.identity_payload.truth_rows)
    negative_zero_truth_rows[1] = (-0.0, 0.0)
    negative_zero_coverage_payload = merge(
        coverage.identity_payload,
        (truth_rows=Tuple(negative_zero_truth_rows),),
    )
    negative_zero_coverage = _forge_rou(
        coverage;
        identity_payload=negative_zero_coverage_payload,
        identity_sha256=_ROU._rou_sha256(negative_zero_coverage_payload),
    )
    @test negative_zero_coverage.identity_sha256 != coverage.identity_sha256
    @test_throws ArgumentError begin
        _ROU.validate_ro_synthetic_coverage_evidence(
            negative_zero_coverage)
    end

    oversized_feature_payload = merge(
        coverage.identity_payload,
        (feature_ids=ntuple(_ -> nothing, 3),),
    )
    oversized_features = _forge_rou(
        coverage; identity_payload=oversized_feature_payload)
    coverage_feature_limit_error = try
        _ROU.validate_ro_synthetic_coverage_evidence(
            oversized_features;
            limits=_ROU.ROUncertaintyLimits(max_outputs=2),
        )
        nothing
    catch error
        error
    end
    @test coverage_feature_limit_error isa _ROU.ROUncertaintyLimitExceeded
    @test coverage_feature_limit_error.phase == :calibration_features

    oversized_case_id_payload = merge(
        coverage.identity_payload,
        (case_ids=ntuple(_ -> nothing, 100),),
    )
    oversized_case_ids = _forge_rou(
        coverage; identity_payload=oversized_case_id_payload)
    @test_throws DimensionMismatch begin
        _ROU.validate_ro_synthetic_coverage_evidence(oversized_case_ids)
    end

    partial_truth = copy(truth)
    partial_truth[3, :] .= NaN
    partial = _ROU.evaluate_ro_synthetic_coverage_fixture(
        partial_truth,
        lower,
        upper;
        fixture_id="partial-fixture",
        source_fixture_sha256=_rou_hash("f"),
        feature_ids=["feature-1", "feature-2"],
        case_ids=["case-$index" for index in 1:4],
        case_validity=BitVector([true, true, false, true]),
        case_gap_reasons=Union{Nothing,Symbol}[
            nothing, nothing, :synthetic_solver_failure, nothing],
    )
    @test partial.status == :unknown_incomplete_synthetic_fixture
    @test partial.invalid_case_ids == ("case-3",)
    @test partial.invalid_gap_reasons == (:synthetic_solver_failure,)
    @test partial.calibration_status ==
        :unknown_incomplete_synthetic_coverage_not_calibrated
    @test !partial.experimentally_calibrated

    @test_throws DimensionMismatch _ROU.evaluate_ro_synthetic_coverage_fixture(
        truth,
        lower,
        upper;
        fixture_id="missing-case",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=["feature-1", "feature-2"],
        case_ids=["case-$index" for index in 1:4],
        expected_case_count=5,
    )
    @test_throws ArgumentError _ROU.evaluate_ro_synthetic_coverage_fixture(
        partial_truth,
        lower,
        upper;
        fixture_id="untyped-gap",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=["feature-1", "feature-2"],
        case_ids=["case-$index" for index in 1:4],
        case_validity=BitVector([true, true, false, true]),
    )
    @test_throws ArgumentError _ROU.evaluate_ro_synthetic_coverage_fixture(
        zeros(Float64, 2, 0),
        zeros(Float64, 2, 0),
        zeros(Float64, 2, 0);
        fixture_id="zero-feature",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=String[],
        case_ids=["case-1", "case-2"],
    )
    @test_throws ArgumentError _ROU.evaluate_ro_synthetic_coverage_fixture(
        truth,
        lower,
        upper;
        fixture_id="bool-count",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=["feature-1", "feature-2"],
        case_ids=["case-$index" for index in 1:4],
        expected_case_count=true,
    )
end

@testset "complete ensemble population keeps gaps and uncertainty classes separate" begin
    outputs = ["threshold", "synergy"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    values = [0.0 0.0; 1.0 2.0; NaN NaN; 3.0 6.0; 4.0 8.0]
    validity = BitVector([true, true, false, true, true])
    gap_reasons = Union{Nothing,Symbol}[
        nothing, nothing, :solver_nonconvergence, nothing, nothing]
    ids = ["replicate-$index" for index in 1:5]
    coverage = _ROU.evaluate_ro_synthetic_coverage_fixture(
        [0.0 0.0; 1.0 1.0],
        [-0.1 -0.1; 0.5 0.5],
        [0.1 0.1; 1.5 1.5];
        fixture_id="two-case-coverage",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=outputs,
        case_ids=["c1", "c2"],
    )
    result = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ensemble_policy(
            5;
            policy_id="lognormal-prior",
            policy_revision="v2",
            distribution_specification=(seed=17,),
        ),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
        quantile_probabilities=(0.0, 0.5, 1.0),
        calibration_evidence=coverage,
    )
    @test result.schema_version == _ROU.RO_UNCERTAINTY_POPULATION_VERSION
    @test result.status == :unknown_numerical_gap
    @test result.expected_population_count == 5
    @test result.population_ids == Tuple(ids)
    @test result.replicate_coordinate_ids == ("draw-1", "draw-2")
    @test result.replicate_coordinates == _rou_replicate_coordinates(5)
    @test result.identity_payload.replicate_coordinates ==
        Tuple(Tuple(row) for row in eachrow(_rou_replicate_coordinates(5)))
    @test result.valid_replicate_count == 4
    @test result.invalid_replicate_count == 1
    @test result.gap_probability == 0.2
    @test result.uncertainty.parametric_uncertainty ==
        :declared_distribution_population
    @test result.uncertainty.experimental_uncertainty == :not_supplied
    @test result.uncertainty.invalid_gap_indices == (3,)
    @test result.uncertainty.invalid_gap_reasons == (:solver_nonconvergence,)
    @test result.quantile_probabilities == (0.0, 0.5, 1.0)
    @test result.feature_quantiles ≈ [0.0 0.0; 2.0 4.0; 4.0 8.0]
    @test result.quantile_scope ==
        :empirical_conditional_on_valid_replicates
    @test result.calibration_evidence !== coverage
    @test result.calibration_evidence.identity_sha256 == coverage.identity_sha256
    @test result.calibration_status ==
        :synthetic_coverage_evaluated_not_calibrated
    @test !result.global_robustness_claimed
    @test !result.causal_claimed
    @test !result.experimentally_validated
    @test result.identity_payload.feature_rows[3] === nothing
    @test result.identity_payload.replicate_gap_reasons[3] ==
        :solver_nonconvergence
    @test _ROU.validate_ro_uncertainty_population_artifact(result)
    tampered_coordinates = deepcopy(result)
    tampered_coordinates.replicate_coordinates[1, 1] += 1.0
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(tampered_coordinates)
    end
    tampered_quantiles = deepcopy(result)
    tampered_quantiles.feature_quantiles[1, 1] += 1.0
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(tampered_quantiles)
    end
    population_fields = Tuple(
        getfield(result, name) for name in fieldnames(typeof(result)))
    @test_throws MethodError typeof(result)(population_fields...)
    forged_quantiles = copy(result.feature_quantiles)
    forged_quantiles[1, 1] += 1.0
    forged_population_snapshot = merge(
        result.identity_payload.result_snapshot,
        (feature_quantiles=_ROU._rou_matrix_payload(forged_quantiles),),
    )
    forged_population_payload = merge(
        result.identity_payload,
        (result_snapshot=forged_population_snapshot,),
    )
    forged_population = _forge_rou(
        result;
        feature_quantiles=forged_quantiles,
        identity_payload=forged_population_payload,
        identity_sha256=_ROU._rou_sha256(forged_population_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(forged_population)
    end
    negative_zero_feature_rows = collect(result.identity_payload.feature_rows)
    negative_zero_feature_rows[1] = (-0.0, 0.0)
    negative_zero_population_payload = merge(
        result.identity_payload,
        (feature_rows=Tuple(negative_zero_feature_rows),),
    )
    negative_zero_population = _forge_rou(
        result;
        identity_payload=negative_zero_population_payload,
        identity_sha256=_ROU._rou_sha256(
            negative_zero_population_payload),
    )
    @test negative_zero_population.identity_sha256 != result.identity_sha256
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(
            negative_zero_population)
    end
    unknown_population_payload = merge(
        result.identity_payload,
        (global_robustness_proven=true,),
    )
    unknown_population = _forge_rou(
        result;
        identity_payload=unknown_population_payload,
        identity_sha256=_ROU._rou_sha256(unknown_population_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(unknown_population)
    end
    unknown_population_policy = merge(
        result.identity_payload.policy,
        (experimentally_proven=true,),
    )
    unknown_nested_population_payload = merge(
        result.identity_payload,
        (policy=unknown_population_policy,),
    )
    unknown_nested_population = _forge_rou(
        result;
        identity_payload=unknown_nested_population_payload,
        identity_sha256=_ROU._rou_sha256(
            unknown_nested_population_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(
            unknown_nested_population)
    end

    oversized_population_policy = merge(
        result.identity_payload.policy,
        (draw_count=100,),
    )
    oversized_population_payload = merge(
        result.identity_payload,
        (
            policy=oversized_population_policy,
            expected_population_count=100,
            population_ids=ntuple(_ -> nothing, 100),
        ),
    )
    oversized_population = _forge_rou(
        result; identity_payload=oversized_population_payload)
    population_limit_error = try
        _ROU.validate_ro_uncertainty_population_artifact(
            oversized_population;
            limits=_ROU.ROUncertaintyLimits(max_replicates=10),
        )
        nothing
    catch error
        error
    end
    @test population_limit_error isa _ROU.ROUncertaintyLimitExceeded
    @test population_limit_error.phase == :replicate_population

    oversized_coordinate_payload = merge(
        result.identity_payload,
        (replicate_coordinate_ids=ntuple(_ -> nothing, 3),),
    )
    oversized_coordinates = _forge_rou(
        result; identity_payload=oversized_coordinate_payload)
    coordinate_limit_error = try
        _ROU.validate_ro_uncertainty_population_artifact(
            oversized_coordinates;
            limits=_ROU.ROUncertaintyLimits(
                max_parameters=2,
                max_replicates=10,
            ),
        )
        nothing
    catch error
        error
    end
    @test coordinate_limit_error isa _ROU.ROUncertaintyLimitExceeded
    @test coordinate_limit_error.phase ==
        :replicate_coordinate_dimensions

    repeated = _ROU.summarize_ro_uncertainty_population(
        copy(values);
        identity,
        source,
        policy=_ensemble_policy(
            5;
            policy_id="lognormal-prior",
            policy_revision="v2",
            distribution_specification=(seed=17,),
        ),
        population_ids=copy(ids),
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=copy(validity),
        replicate_gap_reasons=copy(gap_reasons),
        quantile_probabilities=(0.0, 0.5, 1.0),
        calibration_evidence=coverage,
    )
    @test repeated.identity_sha256 == result.identity_sha256

    experimental = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ensemble_policy(
            5;
            uncertainty_class=:experimental,
            policy_id="measurement-bootstrap",
            distribution_family=:casewise_bootstrap,
            distribution_specification=(resampling=:casewise, seed=17),
        ),
        population_ids=ids,
        replicate_coordinate_ids=["noise-1", "noise-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    @test experimental.uncertainty.parametric_uncertainty == :not_supplied
    @test experimental.uncertainty.experimental_uncertainty ==
        :declared_distribution_population
    @test experimental.identity_sha256 != result.identity_sha256

    integer_policy = _ensemble_policy(
        5; distribution_specification=(seed=17,))
    string_policy = _ensemble_policy(
        5; distribution_specification=(seed="17",))
    integer_identity = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=integer_policy,
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    string_identity = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=string_policy,
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    @test integer_identity.identity_sha256 != string_identity.identity_sha256
    @test _ROU.ro_population_policy_payload(integer_policy) !=
        _ROU.ro_population_policy_payload(string_policy)
    typed_values = Any[
        17,
        "17",
        Symbol("17"),
        true,
        17.0,
        (17,),
        [17],
        (nested=17,),
    ]
    canonical_values = Any[
        _ensemble_policy(
            5; distribution_specification=(value=value,)
        ).distribution_specification
        for value in typed_values
    ]
    @test allunique(canonical_values)
    @test_throws ArgumentError _ensemble_policy(
        5;
        uncertainty_class=:parametric,
        distribution_specification=(uncertainty_class=:experimental,),
    )

    @test_throws DimensionMismatch _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ensemble_policy(6; policy_id="incomplete"),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    @test_throws DimensionMismatch _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ensemble_policy(100; policy_id="declared-one-hundred"),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ensemble_policy(5; policy_id="untyped-gap"),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_ROUExternalPopulationPolicy(),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(5),
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    @test_throws ArgumentError _ROU.ro_population_policy_kind(
        _ROUExternalPopulationPolicy())

    mutable_values = copy(values)
    mutable_coordinates = _rou_replicate_coordinates(5)
    isolated = _ROU.summarize_ro_uncertainty_population(
        mutable_values;
        identity,
        source,
        policy=_ensemble_policy(5; policy_id="input-copy"),
        population_ids=ids,
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=mutable_coordinates,
        replicate_validity=validity,
        replicate_gap_reasons=gap_reasons,
    )
    stored_coordinate = isolated.replicate_coordinates[1, 1]
    stored_quantile = isolated.feature_quantiles[1, 1]
    mutable_coordinates[1, 1] = 999.0
    mutable_values[1, 1] = 999.0
    @test isolated.replicate_coordinates[1, 1] == stored_coordinate
    @test isolated.feature_quantiles[1, 1] == stored_quantile
    @test _ROU.validate_ro_uncertainty_population_artifact(isolated)
end

@testset "interval population binds its certificate without inflating scope" begin
    outputs = ["threshold", "synergy"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    values = [1.0 2.0; 1.5 2.5]
    result = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="instrument-bounds"),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=_rou_replicate_coordinates(2),
        quantile_probabilities=(),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )
    @test result.status == :complete
    @test result.feature_quantiles === nothing
    @test result.certified_bounds == [0.9 1.6; 1.8 2.7]
    @test result.interval_certificate_sha256 == _rou_hash("9")
    @test result.bounds_status ==
        :declared_certificate_bound_to_policy_not_reproved
    @test result.identity_payload.policy.enumeration ==
        "explicit_complete_coordinate_population"
    @test result.identity_payload.policy.coordinate_ids ==
        ("sensor-lower", "sensor-upper")
    @test result.identity_payload.policy.coordinate_units ==
        ("log10 molar", "log10 molar")
    @test result.identity_payload.policy.coordinate_lower == (1.0, 101.0)
    @test result.identity_payload.policy.coordinate_upper == (2.0, 102.0)
    @test result.uncertainty.experimental_uncertainty ==
        :declared_interval_population
    @test result.evidence_scope == :declared_population_only
    @test !result.global_robustness_claimed
    @test !result.experimentally_validated
    @test _ROU.validate_ro_uncertainty_population_artifact(result)
    unknown_interval_policy = merge(
        result.identity_payload.policy,
        (unexpected_interval_owner=true,),
    )
    unknown_interval_payload = merge(
        result.identity_payload,
        (policy=unknown_interval_policy,),
    )
    unknown_interval = _forge_rou(
        result;
        identity_payload=unknown_interval_payload,
        identity_sha256=_ROU._rou_sha256(unknown_interval_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(unknown_interval)
    end
    tampered_interval = deepcopy(result)
    tampered_interval.certified_bounds[1, 1] -= 1.0
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(tampered_interval)
    end

    @test_throws ArgumentError _interval_policy(
        2; interval_certificate_sha256="missing")
    @test_throws ArgumentError _interval_policy(
        2; interval_definition_sha256=_rou_hash("8"))
    @test_throws ArgumentError _interval_policy(
        2; enumeration=:continuous_box_complete)
    @test_throws ArgumentError _interval_policy(
        2; coordinate_lower=(Float32(1), Float32(101)))
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="mixed-summary"),
        population_ids=["a", "b"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=_rou_replicate_coordinates(2),
        quantile_probabilities=(0.5,),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="contradictory-bounds"),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=_rou_replicate_coordinates(2),
        quantile_probabilities=(),
        certified_bounds=[1.1 1.6; 1.8 2.7],
    )
    @test_throws DimensionMismatch _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="coordinate-order"),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-upper", "sensor-lower"],
        replicate_coordinates=_rou_replicate_coordinates(2),
        quantile_probabilities=(),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="coordinate-outside"),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=[100.0 200.0; 101.0 201.0],
        quantile_probabilities=(),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(2; policy_id="duplicate-coordinate"),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=[1.0 101.0; 1.0 101.0],
        quantile_probabilities=(),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )

    forged_policy = merge(
        result.identity_payload.policy,
        (
            coordinate_lower=(100.0, 200.0),
            coordinate_upper=(101.0, 201.0),
        ),
    )
    forged_definition = _ROU._rou_interval_definition_payload(
        forged_policy.population_count,
        Symbol(forged_policy.enumeration),
        Symbol(forged_policy.interval_semantics),
        forged_policy.coordinate_ids,
        forged_policy.coordinate_units,
        forged_policy.coordinate_lower,
        forged_policy.coordinate_upper,
        forged_policy.interval_specification,
    )
    forged_policy = merge(
        forged_policy,
        (interval_definition_sha256=_ROU._rou_sha256(forged_definition),),
    )
    forged_interval_payload = merge(
        result.identity_payload,
        (policy=forged_policy,),
    )
    forged_interval = _forge_rou(
        result;
        identity_payload=forged_interval_payload,
        identity_sha256=_ROU._rou_sha256(forged_interval_payload),
    )
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(forged_interval)
    end

    zero_interval = _ROU.summarize_ro_uncertainty_population(
        values;
        identity,
        source,
        policy=_interval_policy(
            2;
            policy_id="zero-lower-interval",
            coordinate_lower=(0.0, 100.0),
        ),
        population_ids=["lower-corner", "upper-corner"],
        replicate_coordinate_ids=["sensor-lower", "sensor-upper"],
        replicate_coordinates=[0.0 100.0; 2.0 102.0],
        quantile_probabilities=(),
        certified_bounds=[0.9 1.6; 1.8 2.7],
    )
    @test _ROU.validate_ro_uncertainty_population_artifact(zero_interval)
    negative_zero_interval_policy = merge(
        zero_interval.identity_payload.policy,
        (coordinate_lower=(-0.0, 100.0),),
    )
    negative_zero_interval_definition =
        _ROU._rou_interval_definition_payload(
            negative_zero_interval_policy.population_count,
            Symbol(negative_zero_interval_policy.enumeration),
            Symbol(negative_zero_interval_policy.interval_semantics),
            negative_zero_interval_policy.coordinate_ids,
            negative_zero_interval_policy.coordinate_units,
            negative_zero_interval_policy.coordinate_lower,
            negative_zero_interval_policy.coordinate_upper,
            negative_zero_interval_policy.interval_specification,
        )
    negative_zero_interval_policy = merge(
        negative_zero_interval_policy,
        (
            interval_definition_sha256=_ROU._rou_sha256(
                negative_zero_interval_definition),
        ),
    )
    negative_zero_interval_payload = merge(
        zero_interval.identity_payload,
        (policy=negative_zero_interval_policy,),
    )
    negative_zero_interval = _forge_rou(
        zero_interval;
        identity_payload=negative_zero_interval_payload,
        identity_sha256=_ROU._rou_sha256(
            negative_zero_interval_payload),
    )
    @test negative_zero_interval.identity_sha256 !=
        zero_interval.identity_sha256
    @test_throws ArgumentError begin
        _ROU.validate_ro_uncertainty_population_artifact(
            negative_zero_interval)
    end

    oversized_interval_policy = merge(
        result.identity_payload.policy,
        (
            coordinate_ids=ntuple(_ -> nothing, 3),
            coordinate_units=("1", "1", "1"),
            coordinate_lower=(0.0, 0.0, 0.0),
            coordinate_upper=(1.0, 1.0, 1.0),
        ),
    )
    oversized_interval_payload = merge(
        result.identity_payload,
        (policy=oversized_interval_policy,),
    )
    oversized_interval = _forge_rou(
        result; identity_payload=oversized_interval_payload)
    interval_limit_error = try
        _ROU.validate_ro_uncertainty_population_artifact(
            oversized_interval;
            limits=_ROU.ROUncertaintyLimits(max_observations=2),
        )
        nothing
    catch error
        error
    end
    @test interval_limit_error isa _ROU.ROUncertaintyLimitExceeded
    @test interval_limit_error.phase ==
        :replicate_coordinate_dimensions
end

@testset "scientific numeric identity is strict Float64" begin
    outputs = ["y1", "y2"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        Float32[1 0; 0 1];
        identity,
        source,
        observation_weights=ones(2),
    )
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        Matrix{Float64}(I, 2, 2);
        identity,
        source,
        observation_weights=ones(2),
        rank_absolute_high=Float32(1e-12),
    )
    @test_throws ArgumentError _ROU.propagate_ro_delta_covariance(
        Float32[1 0; 0 1],
        Float32[1 0; 0 1];
        identity,
        source,
    )
    @test_throws ArgumentError _ROU.evaluate_ro_synthetic_coverage_fixture(
        Float32[0 0; 1 1],
        Float32[-1 -1; 0 0],
        Float32[1 1; 2 2];
        fixture_id="float32",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=outputs,
        case_ids=["c1", "c2"],
    )
    @test_throws ArgumentError _ROU.summarize_ro_uncertainty_population(
        Float32[0 0; 1 1];
        identity,
        source,
        policy=_ensemble_policy(2; policy_id="float32"),
        population_ids=["r1", "r2"],
        replicate_coordinate_ids=["d1", "d2"],
        replicate_coordinates=_rou_replicate_coordinates(2),
    )
    @test_throws ArgumentError _ensemble_policy(
        2; distribution_specification=(scale=Float32(1),))
    @test_throws ArgumentError _ROU.ROUncertaintyLimits(max_inputs=true)
    limit_fields = ntuple(_ -> 1, length(fieldnames(_ROU.ROUncertaintyLimits)))
    @test_throws MethodError _ROU.ROUncertaintyLimits(limit_fields...)

    setprecision(BigFloat, 256) do
        first_value = BigFloat(1)
        adjacent_value = nextfloat(first_value)
        @test first_value != adjacent_value
        @test Float64(first_value) == Float64(adjacent_value)
        @test_throws ArgumentError _ROU.ROScientificSourceIdentity(
            source_field_sha256=_rou_hash("a"),
            field_request_sha256=_rou_hash("b"),
            network_sha256=_rou_hash("c"),
            local_coordinates=[first_value, adjacent_value],
            nominal_parameters=[0.1, -0.2],
            observation_schedule=outputs,
            solver_revision="solver",
            algorithm_revision="algorithm",
        )
        @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
            BigFloat[first_value 0; 0 adjacent_value];
            identity,
            source,
            observation_weights=ones(2),
        )
    end
end

@testset "factorization, sorting, allocation, and cancellation gates" begin
    sensitivity = Matrix{Float64}(I, 2, 2)
    outputs = ["y1", "y2"]
    identity = _uncertainty_identity(outputs)
    source = _uncertainty_source(outputs)
    identity_constructor_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.ROFieldEvidenceIdentity(
            input_order=_ROUVectorReadProbe{String}(
                3, identity_constructor_reads, "u"),
            input_units=fill("1", 3),
            input_scales=fill("linear", 3),
            parameter_order=["p"],
            parameter_units=["1"],
            parameter_scales=["linear"],
            output_order=["y"],
            output_units=["1"],
            output_scales=["linear"],
            limits=_ROU.ROUncertaintyLimits(max_inputs=2),
        )
    end
    @test identity_constructor_reads[] == 0

    source_constructor_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.ROScientificSourceIdentity(
            source_field_sha256=_rou_hash("a"),
            field_request_sha256=_rou_hash("b"),
            network_sha256=_rou_hash("c"),
            local_coordinates=_ROUVectorReadProbe{Float64}(
                3, source_constructor_reads, 0.0),
            nominal_parameters=[0.0],
            observation_schedule=["y"],
            solver_revision="solver",
            algorithm_revision="algorithm",
            limits=_ROU.ROUncertaintyLimits(max_inputs=2),
        )
    end
    @test source_constructor_reads[] == 0

    unsized_identity_reads = Ref(0)
    @test_throws ArgumentError begin
        _ROU.ROFieldEvidenceIdentity(
            input_order=_ROUUnsizedReadProbe{String}(
                unsized_identity_reads, "u"),
            input_units=["1"],
            input_scales=["linear"],
            parameter_order=["p"],
            parameter_units=["1"],
            parameter_scales=["linear"],
            output_order=["y"],
            output_units=["1"],
            output_scales=["linear"],
        )
    end
    @test unsized_identity_reads[] == 0

    cancelled_constructor_reads = Ref(0)
    @test_throws _ROUTestCancelled begin
        _ROU.ROScientificSourceIdentity(
            source_field_sha256=_rou_hash("a"),
            field_request_sha256=_rou_hash("b"),
            network_sha256=_rou_hash("c"),
            local_coordinates=_ROUVectorReadProbe{Float64}(
                2, cancelled_constructor_reads, 0.0),
            nominal_parameters=[0.0],
            observation_schedule=["y"],
            solver_revision="solver",
            algorithm_revision="algorithm",
            cancel_check=()->throw(_ROUTestCancelled()),
        )
    end
    @test cancelled_constructor_reads[] == 0

    metadata_error = try
        _ROU.ROFieldEvidenceIdentity(
            input_order=[repeat("u", 64)],
            input_units=["1"],
            input_scales=["linear"],
            parameter_order=["p"],
            parameter_units=["1"],
            parameter_scales=["linear"],
            output_order=["y"],
            output_units=["1"],
            output_scales=["linear"],
            limits=_ROU.ROUncertaintyLimits(max_metadata_bytes=32),
        )
        nothing
    catch error
        error
    end
    @test metadata_error isa _ROU.ROUncertaintyLimitExceeded
    @test metadata_error.phase == :identity_metadata_bytes

    cancellation_calls = Ref(0)
    check = () -> (cancellation_calls[] += 1)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.analyze_ro_local_identifiability(
            sensitivity;
            identity,
            source,
            observation_weights=ones(2),
            cancel_check=check,
            limits=_ROU.ROUncertaintyLimits(max_factorization_work=7),
        )
    end
    @test cancellation_calls[] == 1

    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.propagate_ro_delta_covariance(
            sensitivity,
            Matrix{Float64}(I, 2, 2);
            identity,
            source,
            limits=_ROU.ROUncertaintyLimits(max_matrix_elements=19),
        )
    end
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            [0.0 0.0; 1.0 1.0; 2.0 2.0; 3.0 3.0];
            identity,
            source,
            policy=_ensemble_policy(4; policy_id="sort-budget"),
            population_ids=["r1", "r2", "r3", "r4"],
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(4),
            limits=_ROU.ROUncertaintyLimits(max_sort_work=7),
        )
    end
    quantile_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            [0.0 0.0; 1.0 1.0];
            identity,
            source,
            policy=_ensemble_policy(2; policy_id="quantile-count-budget"),
            population_ids=["r1", "r2"],
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(2),
            quantile_probabilities=
                _ROUVectorReadProbe{Float64}(3, quantile_reads, 0.5),
            limits=_ROU.ROUncertaintyLimits(max_quantiles=2),
        )
    end
    @test quantile_reads[] == 0

    ten_outputs = ["q$index" for index in 1:10]
    ten_identity = _uncertainty_identity(ten_outputs)
    ten_source = _uncertainty_source(ten_outputs)
    quantile_element_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            zeros(Float64, 2, 10);
            identity=ten_identity,
            source=ten_source,
            policy=_ensemble_policy(2; policy_id="quantile-element-budget"),
            population_ids=["r1", "r2"],
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(2),
            quantile_probabilities=_ROUVectorReadProbe{Float64}(
                1_000, quantile_element_reads, 0.5),
            limits=_ROU.ROUncertaintyLimits(
                max_quantiles=1_000,
                max_matrix_elements=5_000,
            ),
        )
    end
    @test quantile_element_reads[] == 0

    lookup_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            [0.0 0.0; 1.0 1.0];
            identity,
            source,
            policy=_ensemble_policy(2; policy_id="quantile-lookup-budget"),
            population_ids=["r1", "r2"],
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(2),
            quantile_probabilities=_ROUVectorReadProbe{Float64}(
                1_000, lookup_reads, 0.5),
            limits=_ROU.ROUncertaintyLimits(
                max_quantiles=1_000,
                max_matrix_elements=10_000,
                max_sort_work=100,
            ),
        )
    end
    @test lookup_reads[] == 0

    policy_reads = Ref(0)
    oversized_policy_vector = _ROUVectorReadProbe{Float64}(
        100, policy_reads, 1.0)
    @test_throws _ROU.ROUncertaintyLimitExceeded _ensemble_policy(
        2;
        distribution_specification=(values=oversized_policy_vector,),
        limits=_ROU.ROUncertaintyLimits(max_policy_elements=10),
    )
    @test policy_reads[] == 0

    oversized_population_policy_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded _ensemble_policy(
        2;
        distribution_specification=(values=_ROUVectorReadProbe{Float64}(
            2, oversized_population_policy_reads, 1.0),),
        limits=_ROU.ROUncertaintyLimits(max_replicates=1),
    )
    @test oversized_population_policy_reads[] == 0

    positional_ensemble_reads = Ref(0)
    @test_throws MethodError _ROU.ROEnsemblePopulationPolicy(
        :parametric,
        "positional-ensemble-bypass",
        "v1",
        2,
        :lognormal,
        (values=_ROUVectorReadProbe{Float64}(
            100, positional_ensemble_reads, 1.0),),
        "sampler/v1",
        _ROU.ROUncertaintyLimits(max_policy_elements=10),
        ()->nothing,
    )
    @test positional_ensemble_reads[] == 0

    interval_coordinate_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.ROCertifiedIntervalPopulationPolicy(
            uncertainty_class=:experimental,
            policy_id="interval-preflight",
            policy_revision="v1",
            population_count=2,
            interval_semantics=:bounded_explicit_coordinate_population,
            coordinate_ids=_ROUVectorReadProbe{String}(
                3, interval_coordinate_reads, "sensor"),
            coordinate_units=fill("1", 3),
            coordinate_lower=zeros(Float64, 3),
            coordinate_upper=ones(Float64, 3),
            interval_certificate_sha256=_rou_hash("9"),
            interval_specification=(kind=:fixture,),
            limits=_ROU.ROUncertaintyLimits(max_observations=2),
        )
    end
    @test interval_coordinate_reads[] == 0

    positional_interval_reads = Ref(0)
    @test_throws MethodError _ROU.ROCertifiedIntervalPopulationPolicy(
        :experimental,
        "positional-interval-bypass",
        "v1",
        2,
        :explicit_complete_coordinate_population,
        :bounded_explicit_coordinate_population,
        _ROUVectorReadProbe{String}(
            100, positional_interval_reads, "sensor"),
        ("1", "1"),
        (0.0, 0.0),
        (1.0, 1.0),
        nothing,
        _rou_hash("9"),
        (kind=:fixture,),
        _ROU.ROUncertaintyLimits(max_observations=2),
        ()->nothing,
    )
    @test positional_interval_reads[] == 0

    oversized_policy_metadata = _ensemble_policy(
        2; policy_id=repeat("p", 64))
    object_metadata_error = try
        _ROU.ro_population_policy_payload(
            oversized_policy_metadata;
            limits=_ROU.ROUncertaintyLimits(max_metadata_bytes=32),
        )
        nothing
    catch error
        error
    end
    @test object_metadata_error isa _ROU.ROUncertaintyLimitExceeded
    @test object_metadata_error.phase == :policy_metadata_bytes

    canonical_policy_payload = _ROU.ro_population_policy_payload(
        _ensemble_policy(2; policy_id="payload-metadata"))
    oversized_payload_metadata = merge(
        canonical_policy_payload,
        (policy_id=repeat("p", 64),),
    )
    payload_metadata_error = try
        _ROU._rou_validate_population_policy_payload(
            oversized_payload_metadata;
            limits=_ROU.ROUncertaintyLimits(max_metadata_bytes=32),
        )
        nothing
    catch error
        error
    end
    @test payload_metadata_error isa _ROU.ROUncertaintyLimitExceeded
    @test payload_metadata_error.phase == :policy_metadata_bytes

    symbol_intern_candidate = merge(
        canonical_policy_payload,
        (uncertainty_class=repeat("untrusted-class-", 8),),
    )
    symbol_candidate_error = try
        _ROU._rou_validate_population_policy_payload(
            symbol_intern_candidate;
            limits=_ROU.ROUncertaintyLimits(max_metadata_bytes=40),
        )
        nothing
    catch error
        error
    end
    @test symbol_candidate_error isa _ROU.ROUncertaintyLimitExceeded
    @test symbol_candidate_error.phase == :policy_metadata_bytes

    @test _ROU._rou_validate_population_policy_payload(
        canonical_policy_payload;
        limits=_ROU.ROUncertaintyLimits(max_policy_bytes=24),
    ).policy_id == "payload-metadata"
    integer_boundary_error = _rou_policy_payload_error(
        canonical_policy_payload,
        _ROU.ROUncertaintyLimits(max_policy_bytes=23),
    )
    @test integer_boundary_error isa _ROU.ROUncertaintyLimitExceeded
    @test integer_boundary_error.phase == :policy_bytes

    oversized_noninteger_specification = (
        type="named_tuple",
        entries=((
            name="seed",
            value=(type="integer", value=repeat("x", 250_000)),
        ),),
    )
    oversized_noninteger_payload = merge(
        canonical_policy_payload,
        (distribution_specification=oversized_noninteger_specification,),
    )
    small_policy_limit = _ROU.ROUncertaintyLimits(max_policy_bytes=64)
    oversized_noninteger_error = _rou_policy_payload_error(
        oversized_noninteger_payload,
        small_policy_limit,
    )
    @test oversized_noninteger_error isa _ROU.ROUncertaintyLimitExceeded
    @test oversized_noninteger_error.phase == :policy_bytes

    warm_integer_specification = (
        type="named_tuple",
        entries=((
            name="seed",
            value=(type="integer", value=repeat("9", 128)),
        ),),
    )
    warm_integer_payload = merge(
        canonical_policy_payload,
        (distribution_specification=warm_integer_specification,),
    )
    @test _rou_policy_payload_error(
        warm_integer_payload, small_policy_limit) isa
        _ROU.ROUncertaintyLimitExceeded
    oversized_integer_specification = (
        type="named_tuple",
        entries=((
            name="seed",
            value=(type="integer", value=repeat("9", 250_000)),
        ),),
    )
    oversized_integer_payload = merge(
        canonical_policy_payload,
        (distribution_specification=oversized_integer_specification,),
    )
    GC.gc()
    oversized_integer_error = nothing
    oversized_integer_allocated = @allocated begin
        oversized_integer_error = _rou_policy_payload_error(
            oversized_integer_payload,
            small_policy_limit,
        )
    end
    @test oversized_integer_error isa _ROU.ROUncertaintyLimitExceeded
    @test oversized_integer_error.phase == :policy_bytes
    @test oversized_integer_allocated <= 1_000_000

    symbol_policy_payload = _ROU.ro_population_policy_payload(
        _ensemble_policy(
            2;
            policy_id="symbol-boundary",
            distribution_specification=(kind=:normal,),
        ),
    )
    @test _ROU._rou_validate_population_policy_payload(
        symbol_policy_payload;
        limits=_ROU.ROUncertaintyLimits(max_policy_bytes=27),
    ).policy_id == "symbol-boundary"
    symbol_boundary_error = _rou_policy_payload_error(
        symbol_policy_payload,
        _ROU.ROUncertaintyLimits(max_policy_bytes=26),
    )
    @test symbol_boundary_error isa _ROU.ROUncertaintyLimitExceeded
    @test symbol_boundary_error.phase == :policy_bytes
    bounded_symbol_bytes = Ref(BigInt(0))
    @test _ROU._rou_bounded_symbol_string(
        :lognormal,
        "distribution_family",
        bounded_symbol_bytes,
        _ROU.ROUncertaintyLimits(max_metadata_bytes=9),
        :policy_metadata_bytes,
    ) == "lognormal"
    @test bounded_symbol_bytes[] == 9
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU._rou_bounded_symbol_string(
            :lognormal,
            "distribution_family",
            Ref(BigInt(0)),
            _ROU.ROUncertaintyLimits(max_metadata_bytes=8),
            :policy_metadata_bytes,
        )
    end

    population_id_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            [0.0 0.0; 1.0 1.0];
            identity,
            source,
            policy=_ensemble_policy(2; policy_id="population-id-preflight"),
            population_ids=_ROUVectorReadProbe{String}(
                100, population_id_reads, "replicate"),
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(2),
            limits=_ROU.ROUncertaintyLimits(max_replicates=10),
        )
    end
    @test population_id_reads[] == 0

    coverage_feature_id_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.evaluate_ro_synthetic_coverage_fixture(
            [0.0; 1.0;;],
            [-0.1; 0.9;;],
            [0.1; 1.1;;];
            fixture_id="feature-id-preflight",
            source_fixture_sha256=_rou_hash("e"),
            feature_ids=_ROUVectorReadProbe{String}(
                100, coverage_feature_id_reads, "feature"),
            case_ids=["c1", "c2"],
            limits=_ROU.ROUncertaintyLimits(max_outputs=2),
        )
    end
    @test coverage_feature_id_reads[] == 0

    recursive_policy_value = 1.0
    for _ in 1:10
        recursive_policy_value = (nested=recursive_policy_value,)
    end
    @test_throws _ROU.ROUncertaintyLimitExceeded _ensemble_policy(
        2;
        distribution_specification=(value=recursive_policy_value,),
        limits=_ROU.ROUncertaintyLimits(max_policy_depth=5),
    )
    @test_throws _ROU.ROUncertaintyLimitExceeded _ensemble_policy(
        2;
        distribution_specification=(label=repeat("x", 100),),
        limits=_ROU.ROUncertaintyLimits(max_policy_bytes=20),
    )
    cyclic_policy_value = Any[]
    push!(cyclic_policy_value, cyclic_policy_value)
    @test_throws ArgumentError _ensemble_policy(
        2; distribution_specification=(cycle=cyclic_policy_value,))

    policy_cancel_reads = Ref(0)
    @test_throws _ROUTestCancelled _ensemble_policy(
        2;
        distribution_specification=(values=_ROUVectorReadProbe{Float64}(
            2, policy_cancel_reads, 1.0),),
        cancel_check=()->throw(_ROUTestCancelled()),
    )
    @test policy_cancel_reads[] == 0

    bounded_policy = _ensemble_policy(
        2; distribution_specification=(values=collect(1:20),))
    policy_matrix_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.summarize_ro_uncertainty_population(
            _ROUMatrixReadProbe((2, 2), policy_matrix_reads);
            identity,
            source,
            policy=bounded_policy,
            population_ids=["r1", "r2"],
            replicate_coordinate_ids=["draw-1", "draw-2"],
            replicate_coordinates=_rou_replicate_coordinates(2),
            limits=_ROU.ROUncertaintyLimits(max_policy_elements=5),
        )
    end
    @test policy_matrix_reads[] == 0

    oversized_identity = _forge_rou(
        identity;
        input_order=("u1", "u2", "u3"),
        input_units=("", "", ""),
        input_scales=("linear", "linear", "linear"),
    )
    identity_preflight_reads = Ref(0)
    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.analyze_ro_local_identifiability(
            _ROUMatrixReadProbe((2, 2), identity_preflight_reads);
            identity=oversized_identity,
            source,
            observation_weights=ones(2),
            limits=_ROU.ROUncertaintyLimits(max_inputs=2),
        )
    end
    @test identity_preflight_reads[] == 0

    oversized_source = _uncertainty_source(
        outputs; local_coordinates=[0.0, 0.5, 1.0])
    source_preflight_reads = Ref(0)
    @test_throws DimensionMismatch begin
        _ROU.analyze_ro_local_identifiability(
            _ROUMatrixReadProbe((2, 2), source_preflight_reads);
            identity,
            source=oversized_source,
            observation_weights=ones(2),
        )
    end
    @test source_preflight_reads[] == 0

    baseline_validation = _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(2),
    )
    oversized_identity_result = _forge_rou(
        baseline_validation; identity=oversized_identity)
    identity_validation_error = try
        _ROU.validate_ro_local_identifiability_analysis(
            oversized_identity_result;
            limits=_ROU.ROUncertaintyLimits(max_inputs=2),
        )
        nothing
    catch error
        error
    end
    @test identity_validation_error isa _ROU.ROUncertaintyLimitExceeded
    @test identity_validation_error.phase == :input_dimensions

    oversized_source_result = _forge_rou(
        baseline_validation; source=oversized_source)
    @test_throws DimensionMismatch begin
        _ROU.validate_ro_local_identifiability_analysis(
            oversized_source_result)
    end

    @test_throws _ROU.ROUncertaintyLimitExceeded begin
        _ROU.evaluate_ro_synthetic_coverage_fixture(
            [0.0; 1.0;;],
            [-0.1; 0.9;;],
            [0.1; 1.1;;];
            fixture_id="case-budget",
            source_fixture_sha256=_rou_hash("e"),
            feature_ids=["y1"],
            case_ids=["c1", "c2"],
            limits=_ROU.ROUncertaintyLimits(max_calibration_cases=1),
        )
    end

    cancelled = () -> throw(_ROUTestCancelled())
    @test_throws _ROUTestCancelled _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(2),
        cancel_check=cancelled,
    )
    @test_throws _ROUTestCancelled _ROU.propagate_ro_delta_covariance(
        sensitivity,
        Matrix{Float64}(I, 2, 2);
        identity,
        source,
        cancel_check=cancelled,
    )
    @test_throws _ROUTestCancelled _ROU.summarize_ro_uncertainty_population(
        [0.0 0.0; 1.0 1.0];
        identity,
        source,
        policy=_ensemble_policy(2; policy_id="cancelled"),
        population_ids=["r1", "r2"],
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=_rou_replicate_coordinates(2),
        cancel_check=cancelled,
    )
    @test_throws _ROUTestCancelled _ROU.evaluate_ro_synthetic_coverage_fixture(
        [0.0 0.0; 1.0 1.0],
        [-0.1 -0.1; 0.9 0.9],
        [0.1 0.1; 1.1 1.1];
        fixture_id="cancelled",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=outputs,
        case_ids=["c1", "c2"],
        cancel_check=cancelled,
    )

    matrix_reads = Ref(0)
    validity_reads = Ref(0)
    matrix_probe = _ROUMatrixReadProbe((2, 2), matrix_reads)
    validity_probe = _ROUVectorReadProbe{Bool}(2, validity_reads, true)
    @test_throws _ROUTestCancelled _ROU.analyze_ro_local_identifiability(
        matrix_probe;
        identity,
        source,
        observation_weights=ones(2),
        observation_validity=validity_probe,
        cancel_check=cancelled,
    )
    @test matrix_reads[] == 0
    @test validity_reads[] == 0
    @test_throws _ROUTestCancelled _ROU.propagate_ro_delta_covariance(
        matrix_probe,
        matrix_probe;
        identity,
        source,
        output_validity=validity_probe,
        cancel_check=cancelled,
    )
    @test matrix_reads[] == 0
    @test validity_reads[] == 0
    @test_throws _ROUTestCancelled _ROU.evaluate_ro_synthetic_coverage_fixture(
        matrix_probe,
        matrix_probe,
        matrix_probe;
        fixture_id="read-probe",
        source_fixture_sha256=_rou_hash("e"),
        feature_ids=outputs,
        case_ids=["c1", "c2"],
        case_validity=validity_probe,
        cancel_check=cancelled,
    )
    @test matrix_reads[] == 0
    @test validity_reads[] == 0
    @test_throws _ROUTestCancelled _ROU.summarize_ro_uncertainty_population(
        matrix_probe;
        identity,
        source,
        policy=_ensemble_policy(2; policy_id="read-probe"),
        population_ids=["r1", "r2"],
        replicate_coordinate_ids=["draw-1", "draw-2"],
        replicate_coordinates=matrix_probe,
        replicate_validity=validity_probe,
        cancel_check=cancelled,
    )
    @test matrix_reads[] == 0
    @test validity_reads[] == 0

    wrong_length_reads = Ref(0)
    @test_throws DimensionMismatch _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(2),
        observation_validity=
            _ROUVectorReadProbe{Bool}(1, wrong_length_reads, true),
    )
    @test wrong_length_reads[] == 0
    wrong_type_reads = Ref(0)
    @test_throws ArgumentError _ROU.analyze_ro_local_identifiability(
        sensitivity;
        identity,
        source,
        observation_weights=ones(2),
        observation_validity=
            _ROUVectorReadProbe{Int}(2, wrong_type_reads, 1),
    )
    @test wrong_type_reads[] == 0
end
