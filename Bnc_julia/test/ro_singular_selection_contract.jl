using Test
using BindingAndCatalysis

const _ROSSEL_TEST_MODULE = BindingAndCatalysis

_rossel_test_hash(index::Integer) = lpad(string(index; base=16), 64, '0')

function _rossel_test_policy(;
    protocol_hash=_rossel_test_hash(4),
    enumerator_hash=_rossel_test_hash(5),
    enumeration_scope_hash=_rossel_test_hash(8),
    stability_policy_hash=_rossel_test_hash(6),
    reachability_policy_hash=_rossel_test_hash(7),
    residual_evaluator_hash=_rossel_test_hash(9),
    residual_tolerance=1.0e-8,
    residual_norm=:linf,
    residual_scaling=:absolute,
    residual_unit="scaled equilibrium residual",
    residual_scale=1.0,
    input_order=["u", "v"],
    input_units=nothing,
    output_order=["y1", "y2"],
    output_units=nothing,
    selection_rule=:unique_stable_reachable_complete_candidate,
)
    declared_input_units = input_units === nothing ?
        fill("log10 concentration", length(input_order)) : input_units
    declared_output_units = output_units === nothing ?
        fill("log10 concentration", length(output_order)) : output_units
    return ROSingularSelectionPolicy(
        regular_extension_sha256=_rossel_test_hash(1),
        stratum_sha256=_rossel_test_hash(2),
        model_sha256=_rossel_test_hash(3),
        protocol_sha256=protocol_hash,
        branch_enumerator_sha256=enumerator_hash,
        enumeration_scope_sha256=enumeration_scope_hash,
        stability_policy_sha256=stability_policy_hash,
        reachability_policy_sha256=reachability_policy_hash,
        input_order=input_order,
        input_units=declared_input_units,
        output_order=output_order,
        output_units=declared_output_units,
        residual_norm=residual_norm,
        residual_scaling=residual_scaling,
        residual_unit=residual_unit,
        residual_scale=residual_scale,
        residual_evaluator_sha256=residual_evaluator_hash,
        residual_absolute_tolerance=residual_tolerance,
        selection_rule=selection_rule,
    )
end

function _rossel_test_candidate(
    policy::ROSingularSelectionPolicy,
    index::Int;
    branch_id="branch-$index",
    source_regime_ids=[index, index + 10],
    jacobian=Float64[
        index 2 * index
        3 * index 4 * index
    ],
    output=Float64[0.1 * index, 0.2 * index],
    residual=1.0e-10,
    numeric_gap_reasons=Symbol[],
    stability=:locally_stable,
    stability_analysis_sha256=
        stability == :unknown_gap ? nothing : _rossel_test_hash(200 + index),
    stability_gap_reasons=
        stability == :unknown_gap ? [:stability_solver_failed] : Symbol[],
    reachability=:reached_under_protocol,
    dynamic_trace_sha256=
        reachability == :unknown_gap ? nothing : _rossel_test_hash(300 + index),
    reachability_gap_reasons=
        reachability == :unknown_gap ? [:protocol_trace_failed] : Symbol[],
)
    branch_hash = ro_singular_branch_identity_sha256(
        policy=policy,
        branch_id=branch_id,
        source_regime_ids=source_regime_ids,
    )
    stability_evidence = ROSingularStabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=stability,
        analysis_sha256=stability_analysis_sha256,
        gap_reasons=stability_gap_reasons,
    )
    reachability_evidence = ROSingularReachabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=reachability,
        dynamic_trace_sha256=dynamic_trace_sha256,
        gap_reasons=reachability_gap_reasons,
    )
    return ROSingularBranchCandidate(
        policy=policy,
        branch_id=branch_id,
        source_regime_ids=source_regime_ids,
        jacobian=jacobian,
        output_at_stratum=output,
        residual_upper_bound=residual,
        numeric_gap_reasons=numeric_gap_reasons,
        stability_evidence=stability_evidence,
        reachability_evidence=reachability_evidence,
    )
end

function _rossel_test_receipt(
    candidates,
    policy::ROSingularSelectionPolicy;
    complete=true,
    expected_candidate_count=length(candidates),
    gap_reasons=complete ? Symbol[] : [:enumeration_truncated],
)
    return build_ro_singular_candidate_population_receipt(
        candidates;
        policy=policy,
        expected_candidate_count=expected_candidate_count,
        complete=complete,
        gap_reasons=gap_reasons,
    )
end

function _rossel_test_certify(candidates, policy; receipt=nothing, kwargs...)
    population_receipt = receipt === nothing ?
        _rossel_test_receipt(candidates, policy) : receipt
    return certify_ro_singular_branch_selection(
        candidates;
        policy=policy,
        population_receipt=population_receipt,
        kwargs...,
    )
end

@testset "singular selection requires one complete stable reachable branch" begin
    policy = _rossel_test_policy()
    stable = _rossel_test_candidate(policy, 1)
    unstable = _rossel_test_candidate(policy, 2; stability=:unstable)
    receipt = _rossel_test_receipt([unstable, stable], policy)
    certificate = _rossel_test_certify(
        [unstable, stable], policy; receipt=receipt)

    @test certificate.schema_version == RO_SINGULAR_SELECTION_VERSION
    @test certificate.status ==
        :selected_unique_branch_under_declared_policy
    @test certificate.population_receipt_sha256 == receipt.receipt_sha256
    @test certificate.candidate_population_complete
    @test certificate.candidate_count == 2
    @test certificate.admissible_branch_ids == ["branch-1"]
    @test certificate.selected_branch_id == "branch-1"
    @test certificate.selected_branch_identity_sha256 ==
        stable.branch_identity_sha256
    @test certificate.selected_candidate_payload_sha256 ==
        stable.candidate_payload_sha256
    @test certificate.selected_stability_evidence_sha256 ==
        stable.stability_evidence.evidence_sha256
    @test certificate.selected_stability_analysis_sha256 ==
        stable.stability_evidence.analysis_sha256
    @test certificate.selected_reachability_evidence_sha256 ==
        stable.reachability_evidence.evidence_sha256
    @test certificate.selected_dynamic_trace_sha256 ==
        stable.reachability_evidence.dynamic_trace_sha256
    @test certificate.selected_jacobian == stable.jacobian
    @test certificate.selected_output_at_stratum == stable.output_at_stratum
    @test certificate.selected_jacobian !== stable.jacobian
    @test certificate.selected_output_at_stratum !== stable.output_at_stratum
    @test certificate.includes_singular_branch
    @test !certificate.regular_limit_only
    @test certificate.evidence_scope == RO_SINGULAR_SELECTION_SCOPE
    @test !certificate.universal_selection_claimed
    @test !certificate.causal_claimed
    @test !certificate.experimentally_validated
    @test occursin(r"^[0-9a-f]{64}$", certificate.identity_sha256)

    reordered = _rossel_test_certify(
        [stable, unstable], policy; receipt=receipt)
    @test reordered.identity_sha256 == certificate.identity_sha256
    @test reordered.selected_jacobian == certificate.selected_jacobian

    other_policy = _rossel_test_policy(
        protocol_hash=_rossel_test_hash(999))
    other_stable = _rossel_test_candidate(other_policy, 1)
    other_unstable = _rossel_test_candidate(
        other_policy, 2; stability=:unstable)
    other_protocol = _rossel_test_certify(
        [other_stable, other_unstable], other_policy)
    @test other_protocol.identity_sha256 != certificate.identity_sha256

    other_order_policy = _rossel_test_policy(input_order=["v", "u"])
    other_order_candidates = [
        _rossel_test_candidate(other_order_policy, 1),
        _rossel_test_candidate(other_order_policy, 2; stability=:unstable),
    ]
    other_order = _rossel_test_certify(
        other_order_candidates, other_order_policy)
    @test other_order.identity_sha256 != certificate.identity_sha256
end

@testset "set-valued, incomplete, and gap evidence never choose a branch" begin
    policy = _rossel_test_policy()
    first_branch = _rossel_test_candidate(policy, 1)
    second_branch = _rossel_test_candidate(policy, 2)

    multiple = _rossel_test_certify([first_branch, second_branch], policy)
    @test multiple.status == :set_valued_multiple_admissible_branches
    @test multiple.admissible_branch_ids == ["branch-1", "branch-2"]
    @test multiple.selected_branch_id === nothing
    @test multiple.selected_jacobian === nothing
    @test multiple.includes_singular_branch
    @test !multiple.regular_limit_only

    incomplete_receipt = _rossel_test_receipt(
        [first_branch], policy;
        complete=false,
        expected_candidate_count=2,
        gap_reasons=[:enumeration_cancelled],
    )
    incomplete = _rossel_test_certify(
        [first_branch], policy; receipt=incomplete_receipt)
    @test incomplete.status == :unknown_incomplete_population
    @test incomplete.selected_branch_id === nothing
    @test :candidate_population_incomplete in incomplete.reason_codes
    @test :enumeration_cancelled in incomplete.reason_codes

    gap = _rossel_test_candidate(policy, 3; stability=:unknown_gap)
    unknown = _rossel_test_certify([first_branch, gap], policy)
    @test unknown.status == :unknown_gap
    @test unknown.admissible_branch_ids == String[]
    @test unknown.selected_branch_id === nothing
    @test :candidate_evidence_gap in unknown.reason_codes
    @test :stability_solver_failed in unknown.reason_codes

    rejected = _rossel_test_candidate(
        policy, 4; reachability=:not_reached)
    none = _rossel_test_certify([rejected], policy)
    @test none.status == :no_admissible_candidate_in_declared_population
    @test none.selected_branch_id === nothing
    @test :no_candidate_satisfied_declared_policy in none.reason_codes

    complete_empty = _rossel_test_certify(
        ROSingularBranchCandidate[], policy)
    @test complete_empty.status ==
        :no_admissible_candidate_in_declared_population

    incomplete_empty_receipt = _rossel_test_receipt(
        ROSingularBranchCandidate[], policy;
        complete=false,
        expected_candidate_count=1,
        gap_reasons=[:enumerator_failed_before_first_candidate],
    )
    incomplete_empty = _rossel_test_certify(
        ROSingularBranchCandidate[], policy;
        receipt=incomplete_empty_receipt,
    )
    @test incomplete_empty.status == :unknown_incomplete_population

    obsolete_error = try
        certify_ro_singular_branch_selection(
            [first_branch];
            policy=policy,
            candidate_population_complete=true,
        )
        nothing
    catch error
        error
    end
    @test obsolete_error isa MethodError || obsolete_error isa UndefKeywordError
end

@testset "population receipt binds the complete sorted candidate population" begin
    policy = _rossel_test_policy()
    first_branch = _rossel_test_candidate(policy, 1)
    second_branch = _rossel_test_candidate(policy, 2; stability=:unstable)
    candidates = [first_branch, second_branch]
    receipt = _rossel_test_receipt(candidates, policy)

    @test receipt.complete
    @test receipt.expected_candidate_count == 2
    @test receipt.observed_candidate_count == 2
    @test !receipt.universal_completeness_claimed
    @test length(receipt.candidate_roots) == 2
    @test receipt.receipt_sha256 ==
        _rossel_test_receipt(reverse(candidates), policy).receipt_sha256

    @test_throws ArgumentError _rossel_test_certify(
        [first_branch], policy; receipt=receipt)
    @test_throws ArgumentError _rossel_test_receipt(
        candidates, policy; expected_candidate_count=3)
    @test_throws ArgumentError _rossel_test_receipt(
        candidates, policy; gap_reasons=[:fabricated_gap])
    @test_throws ArgumentError _rossel_test_receipt(
        candidates, policy; complete=false, gap_reasons=Symbol[])
    @test_throws ArgumentError _rossel_test_receipt(
        candidates, policy;
        complete=false,
        expected_candidate_count=1,
        gap_reasons=[:enumeration_truncated],
    )

    truncated_roots = _rossel_test_receipt(candidates, policy)
    pop!(truncated_roots.candidate_roots)
    @test_throws ArgumentError _rossel_test_certify(
        candidates, policy; receipt=truncated_roots)

    forged_complete_gap = _rossel_test_receipt(candidates, policy)
    push!(forged_complete_gap.gap_reasons, :late_gap_injection)
    @test_throws ArgumentError _rossel_test_certify(
        candidates, policy; receipt=forged_complete_gap)

    mutated_candidate = _rossel_test_candidate(policy, 9)
    mutation_receipt = _rossel_test_receipt([mutated_candidate], policy)
    mutated_candidate.jacobian[1, 1] += 1.0
    @test_throws ArgumentError _rossel_test_certify(
        [mutated_candidate], policy; receipt=mutation_receipt)
end

@testset "typed stability and reachability evidence cannot be asserted" begin
    policy = _rossel_test_policy()
    branch_hash = ro_singular_branch_identity_sha256(
        policy=policy,
        branch_id="branch-1",
        source_regime_ids=[1, 11],
    )

    @test_throws ArgumentError ROSingularStabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=:locally_stable,
        analysis_sha256=nothing,
    )
    @test_throws ArgumentError ROSingularReachabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=:reached_under_protocol,
        dynamic_trace_sha256=nothing,
    )
    @test_throws ArgumentError ROSingularStabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=:unknown_gap,
        gap_reasons=Symbol[],
    )

    other_protocol_policy = _rossel_test_policy(
        protocol_hash=_rossel_test_hash(999))
    foreign_reachability = ROSingularReachabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=other_protocol_policy,
        status=:reached_under_protocol,
        dynamic_trace_sha256=_rossel_test_hash(901),
    )
    local_stability = ROSingularStabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=:locally_stable,
        analysis_sha256=_rossel_test_hash(902),
    )
    @test_throws ArgumentError ROSingularBranchCandidate(
        policy=policy,
        branch_id="branch-1",
        source_regime_ids=[1, 11],
        jacobian=ones(2, 2),
        output_at_stratum=zeros(2),
        residual_upper_bound=0.0,
        stability_evidence=local_stability,
        reachability_evidence=foreign_reachability,
    )

    other_stability_policy = _rossel_test_policy(
        stability_policy_hash=_rossel_test_hash(998))
    foreign_stability = ROSingularStabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=other_stability_policy,
        status=:locally_stable,
        analysis_sha256=_rossel_test_hash(903),
    )
    local_reachability = ROSingularReachabilityEvidence(
        branch_identity_sha256=branch_hash,
        policy=policy,
        status=:reached_under_protocol,
        dynamic_trace_sha256=_rossel_test_hash(904),
    )
    @test_throws ArgumentError ROSingularBranchCandidate(
        policy=policy,
        branch_id="branch-1",
        source_regime_ids=[1, 11],
        jacobian=ones(2, 2),
        output_at_stratum=zeros(2),
        residual_upper_bound=0.0,
        stability_evidence=foreign_stability,
        reachability_evidence=local_reachability,
    )

    stability_mutation = _rossel_test_candidate(policy, 2)
    stability_receipt = _rossel_test_receipt([stability_mutation], policy)
    push!(stability_mutation.stability_evidence.gap_reasons,
        :post_hash_stability_gap)
    @test_throws ArgumentError _rossel_test_certify(
        [stability_mutation], policy; receipt=stability_receipt)

    reachability_mutation = _rossel_test_candidate(policy, 3)
    reachability_receipt = _rossel_test_receipt(
        [reachability_mutation], policy)
    push!(reachability_mutation.reachability_evidence.gap_reasons,
        :post_hash_trace_gap)
    @test_throws ArgumentError _rossel_test_certify(
        [reachability_mutation], policy; receipt=reachability_receipt)
end

@testset "inner constructors close the positional bypass and certify revalidates" begin
    policy = _rossel_test_policy()
    candidate = _rossel_test_candidate(policy, 1)
    receipt = _rossel_test_receipt([candidate], policy)

    @test_throws MethodError ROSingularSelectionPolicy(
        policy.schema_version,
        policy.policy_sha256,
        policy.regular_extension_sha256,
        policy.stratum_sha256,
        policy.model_sha256,
        policy.protocol_sha256,
        policy.branch_enumerator_sha256,
        policy.enumeration_scope_sha256,
        policy.stability_policy_sha256,
        policy.reachability_policy_sha256,
        policy.input_order,
        policy.input_units,
        policy.output_order,
        policy.output_units,
        policy.residual_norm,
        policy.residual_scaling,
        policy.residual_unit,
        policy.residual_scale,
        policy.residual_evaluator_sha256,
        policy.residual_absolute_tolerance,
        policy.selection_rule,
    )

    @test_throws MethodError ROSingularBranchCandidate(
        candidate.schema_version,
        candidate.candidate_payload_sha256,
        candidate.selection_policy_sha256,
        candidate.branch_id,
        candidate.branch_identity_sha256,
        candidate.model_sha256,
        candidate.stratum_sha256,
        candidate.source_regime_ids,
        candidate.jacobian,
        candidate.output_at_stratum,
        candidate.residual_upper_bound,
        candidate.numeric_gap_reasons,
        candidate.stability_evidence,
        candidate.reachability_evidence,
    )

    forged_policy = _ROSSEL_TEST_MODULE.ROSingularSelectionPolicy(
        _ROSSEL_TEST_MODULE._ROSSEL_VALIDATED,
        policy.schema_version,
        policy.policy_sha256,
        policy.regular_extension_sha256,
        policy.stratum_sha256,
        policy.model_sha256,
        policy.protocol_sha256,
        policy.branch_enumerator_sha256,
        policy.enumeration_scope_sha256,
        policy.stability_policy_sha256,
        policy.reachability_policy_sha256,
        copy(policy.input_order),
        copy(policy.input_units),
        copy(policy.output_order),
        copy(policy.output_units),
        policy.residual_norm,
        policy.residual_scaling,
        policy.residual_unit,
        policy.residual_scale,
        policy.residual_evaluator_sha256,
        policy.residual_absolute_tolerance,
        :select_first_serialized_branch,
    )
    @test_throws ArgumentError certify_ro_singular_branch_selection(
        [candidate];
        policy=forged_policy,
        population_receipt=receipt,
    )

    forged_candidate = _ROSSEL_TEST_MODULE.ROSingularBranchCandidate(
        _ROSSEL_TEST_MODULE._ROSSEL_VALIDATED,
        candidate.schema_version,
        candidate.candidate_payload_sha256,
        candidate.selection_policy_sha256,
        candidate.branch_id,
        _rossel_test_hash(997),
        candidate.model_sha256,
        candidate.stratum_sha256,
        copy(candidate.source_regime_ids),
        copy(candidate.jacobian),
        copy(candidate.output_at_stratum),
        candidate.residual_upper_bound,
        copy(candidate.numeric_gap_reasons),
        candidate.stability_evidence,
        candidate.reachability_evidence,
    )
    @test_throws ArgumentError certify_ro_singular_branch_selection(
        [forged_candidate];
        policy=policy,
        population_receipt=receipt,
    )

    forged_shape_candidate =
        _ROSSEL_TEST_MODULE.ROSingularBranchCandidate(
            _ROSSEL_TEST_MODULE._ROSSEL_VALIDATED,
            candidate.schema_version,
            candidate.candidate_payload_sha256,
            candidate.selection_policy_sha256,
            candidate.branch_id,
            candidate.branch_identity_sha256,
            candidate.model_sha256,
            candidate.stratum_sha256,
            copy(candidate.source_regime_ids),
            ones(3, 2),
            copy(candidate.output_at_stratum),
            candidate.residual_upper_bound,
            copy(candidate.numeric_gap_reasons),
            candidate.stability_evidence,
            candidate.reachability_evidence,
        )
    @test_throws DimensionMismatch certify_ro_singular_branch_selection(
        [forged_shape_candidate];
        policy=policy,
        population_receipt=receipt,
    )

    forged_stability = _ROSSEL_TEST_MODULE.ROSingularStabilityEvidence(
        _ROSSEL_TEST_MODULE._ROSSEL_VALIDATED,
        candidate.stability_evidence.schema_version,
        candidate.stability_evidence.evidence_sha256,
        candidate.branch_identity_sha256,
        policy.model_sha256,
        policy.stratum_sha256,
        policy.stability_policy_sha256,
        :locally_stable,
        nothing,
        Symbol[],
        candidate.stability_evidence.evidence_scope,
        false,
    )
    forged_evidence_candidate =
        _ROSSEL_TEST_MODULE.ROSingularBranchCandidate(
            _ROSSEL_TEST_MODULE._ROSSEL_VALIDATED,
            candidate.schema_version,
            candidate.candidate_payload_sha256,
            candidate.selection_policy_sha256,
            candidate.branch_id,
            candidate.branch_identity_sha256,
            candidate.model_sha256,
            candidate.stratum_sha256,
            copy(candidate.source_regime_ids),
            copy(candidate.jacobian),
            copy(candidate.output_at_stratum),
            candidate.residual_upper_bound,
            copy(candidate.numeric_gap_reasons),
            forged_stability,
            candidate.reachability_evidence,
        )
    @test_throws ArgumentError certify_ro_singular_branch_selection(
        [forged_evidence_candidate];
        policy=policy,
        population_receipt=receipt,
    )

    mutated_policy = _rossel_test_policy()
    mutation_candidate = _rossel_test_candidate(mutated_policy, 4)
    mutation_receipt = _rossel_test_receipt(
        [mutation_candidate], mutated_policy)
    push!(mutated_policy.input_order, "post-hash-axis")
    @test_throws DimensionMismatch certify_ro_singular_branch_selection(
        [mutation_candidate];
        policy=mutated_policy,
        population_receipt=mutation_receipt,
    )
end

@testset "residual precision and semantics are explicit" begin
    @test_throws ArgumentError _rossel_test_policy(
        residual_tolerance=BigFloat("1e-30"))

    policy = _rossel_test_policy()
    @test_throws ArgumentError _rossel_test_candidate(
        policy, 1; residual=BigFloat("1.0000000000000000001e-8"))

    strict_policy = _rossel_test_policy(residual_tolerance=1.0e-8)
    strict_candidate = _rossel_test_candidate(
        strict_policy, 1; residual=5.0e-7)
    strict = _rossel_test_certify([strict_candidate], strict_policy)

    permissive_policy = _rossel_test_policy(residual_tolerance=1.0e-6)
    permissive_candidate = _rossel_test_candidate(
        permissive_policy, 1;
        jacobian=Float64[1 2; -3 4],
        residual=5.0e-7,
    )
    permissive = _rossel_test_certify(
        [permissive_candidate], permissive_policy)
    @test strict.status == :no_admissible_candidate_in_declared_population
    @test permissive.status ==
        :selected_unique_branch_under_declared_policy
    @test permissive.selected_jacobian == Float64[1 2; -3 4]
    @test permissive.identity_sha256 != strict.identity_sha256

    base_policy = _rossel_test_policy()
    semantic_policies = [
        _rossel_test_policy(residual_norm=:l2),
        _rossel_test_policy(
            residual_scaling=:divide_by_declared_scale,
            residual_scale=2.0,
        ),
        _rossel_test_policy(residual_unit="moles per litre"),
        _rossel_test_policy(
            residual_evaluator_hash=_rossel_test_hash(909)),
    ]
    @test all(other.policy_sha256 != base_policy.policy_sha256
        for other in semantic_policies)

    boundary_policy = _rossel_test_policy(residual_tolerance=1.0e-8)
    boundary_candidate = _rossel_test_candidate(
        boundary_policy, 4; residual=1.0e-8)
    @test _rossel_test_certify(
        [boundary_candidate], boundary_policy).status ==
        :selected_unique_branch_under_declared_policy
    above_candidate = _rossel_test_candidate(
        boundary_policy, 5; residual=nextfloat(1.0e-8))
    @test _rossel_test_certify(
        [above_candidate], boundary_policy).status ==
        :no_admissible_candidate_in_declared_population
end

@testset "singular selection input and work limits fail closed" begin
    @test_throws ArgumentError _rossel_test_policy(
        selection_rule=:first_candidate)
    @test_throws ArgumentError ROSingularSelectionPolicy(
        regular_extension_sha256="not-a-hash",
        stratum_sha256=_rossel_test_hash(2),
        model_sha256=_rossel_test_hash(3),
        protocol_sha256=_rossel_test_hash(4),
        branch_enumerator_sha256=_rossel_test_hash(5),
        enumeration_scope_sha256=_rossel_test_hash(8),
        stability_policy_sha256=_rossel_test_hash(6),
        reachability_policy_sha256=_rossel_test_hash(7),
        input_order=["u"],
        input_units=["unit"],
        output_order=["y"],
        output_units=["unit"],
        residual_unit="unit",
        residual_evaluator_sha256=_rossel_test_hash(9),
        residual_absolute_tolerance=1.0e-8,
    )

    policy = _rossel_test_policy()
    @test_throws ArgumentError _rossel_test_candidate(
        policy, 1; jacobian=Float64[1 NaN; 0 1])
    @test_throws ArgumentError _rossel_test_candidate(
        policy, 1;
        stability=:unknown_gap,
        stability_gap_reasons=Symbol[],
    )
    @test_throws ArgumentError _rossel_test_candidate(
        policy, 1;
        stability=:locally_stable,
        stability_gap_reasons=[:fabricated_gap],
    )
    @test_throws DimensionMismatch _rossel_test_candidate(
        policy, 1; jacobian=ones(3, 2), output=ones(2))

    first_branch = _rossel_test_candidate(policy, 1)
    second_branch = _rossel_test_candidate(policy, 2)
    candidates = [first_branch, second_branch]
    receipt = _rossel_test_receipt(candidates, policy)

    @test_throws ROSingularSelectionLimitExceeded _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_candidates=1),
    )
    @test_throws ROSingularSelectionLimitExceeded _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_matrix_elements=3),
    )
    @test_throws ROSingularSelectionLimitExceeded _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_source_regime_ids=1),
    )
    @test_throws ROSingularSelectionLimitExceeded _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_candidate_payload_bytes=8_192),
    )
    @test_throws ArgumentError _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_inputs=1),
    )
    @test_throws ArgumentError _rossel_test_certify(
        candidates, policy;
        receipt=receipt,
        limits=ROSingularSelectionLimits(max_outputs=1),
    )

    identity_error = try
        _rossel_test_certify(
            candidates,
            policy;
            receipt=receipt,
            limits=ROSingularSelectionLimits(max_identity_bytes=8_192),
        )
        nothing
    catch error
        error
    end
    @test identity_error isa ROSingularSelectionLimitExceeded
    @test identity_error.phase == :policy_identity_reservation

    population_identity_error = try
        _rossel_test_certify(
            candidates,
            policy;
            receipt=receipt,
            limits=ROSingularSelectionLimits(max_identity_bytes=8_500),
        )
        nothing
    catch error
        error
    end
    @test population_identity_error isa ROSingularSelectionLimitExceeded
    @test population_identity_error.phase ==
        :population_receipt_reservation

    two_gap_candidate = _rossel_test_candidate(
        policy, 3;
        stability=:unknown_gap,
        stability_gap_reasons=[:first_gap, :second_gap],
    )
    two_gap_receipt = _rossel_test_receipt([two_gap_candidate], policy)
    @test_throws ROSingularSelectionLimitExceeded _rossel_test_certify(
        [two_gap_candidate], policy;
        receipt=two_gap_receipt,
        limits=ROSingularSelectionLimits(max_gap_reasons=1),
    )

    empty_receipt = _rossel_test_receipt(
        ROSingularBranchCandidate[], policy;
        complete=false,
        expected_candidate_count=1,
        gap_reasons=[:enumeration_cancelled],
    )
    immediate_checks = Ref(0)
    @test_throws ErrorException certify_ro_singular_branch_selection(
        ROSingularBranchCandidate[];
        policy=policy,
        population_receipt=empty_receipt,
        cancel_check=() -> begin
            immediate_checks[] += 1
            error("cancelled")
        end,
    )
    @test immediate_checks[] == 1

    full_checks = Ref(0)
    _rossel_test_certify(
        candidates,
        policy;
        receipt=receipt,
        cancel_check=() -> (full_checks[] += 1),
    )
    @test full_checks[] > 10
    stopped_checks = Ref(0)
    @test_throws ErrorException _rossel_test_certify(
        candidates,
        policy;
        receipt=receipt,
        cancel_check=() -> begin
            stopped_checks[] += 1
            stopped_checks[] == full_checks[] && error("cancelled")
        end,
    )
    @test stopped_checks[] == full_checks[]
end

@testset "intact higher-dimensional MIMO branch matrices are preserved" begin
    policy = _rossel_test_policy(
        input_order=["u", "v", "w"],
        output_order=["y1", "y2"],
    )
    jacobian = Float64[
        1 2 3
        -4 5 -6
    ]
    candidate = _rossel_test_candidate(
        policy, 1; jacobian=jacobian, output=[0.1, 0.2])
    certificate = _rossel_test_certify([candidate], policy)
    @test certificate.selected_jacobian == jacobian
    @test size(certificate.selected_jacobian) == (2, 3)

    @test_throws DimensionMismatch _rossel_test_candidate(
        policy, 2; jacobian=ones(3, 2), output=ones(2))
end
