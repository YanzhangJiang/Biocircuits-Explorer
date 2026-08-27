module ROBranchIndexedFieldContract

using Test
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function translated_cubic_system(; limits=RORegularSheetLimits())
    # f(x,u) = -(x-1)(x-2)(x-3).  The declared control is a neutral
    # continuation coordinate.  Roots 1 and 3 are stable and root 2 is
    # unstable for every control in the admitted boxes.
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["concentration"],
        control_names=["u"],
        control_units=["concentration"],
        equations=[[
            term(-1.0, [3], [0]),
            term(6.0, [2], [0]),
            term(-11.0, [1], [0]),
            term(6.0, [0], [0]),
        ]],
        limits=limits,
    )
end

function cubic_patch(
    system,
    offset;
    control_interval=(1.4375, 1.5625),
    control_reference=1.5,
    remainder=(-0.015625, 0.015625),
)
    stable = offset == 0.0 || offset == 2.0
    return certify_ro_regular_sheet_patch(
        system;
        control_lower=[control_interval[1]],
        control_upper=[control_interval[2]],
        control_reference=[control_reference],
        state_reference=[1.0 + offset],
        predictor_slope=[0.0;;],
        remainder_lower=[remainder[1]],
        remainder_upper=[remainder[2]],
        preconditioner=[stable ? -0.5 : 1.0;;],
    )
end

function cubic_sources()
    system = translated_cubic_system()
    lower_parent = cubic_patch(
        system,
        0.0;
        control_interval=(1.375, 1.5),
        control_reference=1.4375,
    )
    lower_child = cubic_patch(
        system,
        0.0;
        control_interval=(1.4375, 1.5625),
        control_reference=1.5,
    )
    middle = cubic_patch(system, 1.0)
    upper = cubic_patch(system, 2.0)
    lower_bridge = certify_ro_regular_sheet_bridge(
        system,
        lower_parent,
        lower_child;
        overlap_lower=[1.4375],
        overlap_upper=[1.5],
        bridge_control_reference=[1.46875],
        bridge_state_reference=[1.0],
        bridge_predictor_slope=[0.0;;],
        bridge_remainder_lower=[-0.03125],
        bridge_remainder_upper=[0.03125],
        bridge_preconditioner=[-0.5;;],
    )
    return system, lower_parent, lower_child, middle, upper, lower_bridge
end

function dynamics_binding(system; policy=repeat("a", 64))
    return ROPolynomialDynamicsBinding(
        system;
        time_unit="second",
        state_rate_units=["concentration_per_second"],
        dynamics_policy_sha256=policy,
    )
end

function multistable_field()
    system, lower_parent, lower_child, middle, upper, lower_bridge =
        cubic_sources()
    binding = dynamics_binding(system)
    patches = [lower_parent, lower_child, middle, upper]
    bridges = [lower_bridge]
    witnesses = [(
        control_lower=[1.453125],
        control_upper=[1.484375],
        patch_certificate_sha256s=[
            lower_child.certificate_sha256,
            upper.certificate_sha256,
        ],
    )]
    field = certify_ro_branch_indexed_regular_field(
        system,
        binding,
        patches,
        bridges;
        witnesses=witnesses,
    )
    return (
        system=system,
        binding=binding,
        patches=patches,
        bridges=bridges,
        lower_parent=lower_parent,
        lower_child=lower_child,
        middle=middle,
        upper=upper,
        field=field,
    )
end

@testset "P8s0 branch-indexed polynomial regular-field contract" begin
    @testset "dynamics semantics are explicit and content bound" begin
        system = translated_cubic_system()
        binding = dynamics_binding(system)
        @test binding.version == RO_POLYNOMIAL_DYNAMICS_BINDING_VERSION
        @test binding.system_declaration_sha256 == system.declaration_sha256
        @test binding.equations_are_state_time_derivatives
        @test !binding.unit_algebra_verified
        @test binding.evidence_scope ==
            :declared_polynomial_vector_field_semantics_only
        @test validate_ro_polynomial_dynamics_binding(system, binding)
        @test occursin(r"^[0-9a-f]{64}$", binding.declaration_sha256)
        @test dynamics_binding(system; policy=repeat("b", 64)).declaration_sha256 !=
            binding.declaration_sha256
        @test_throws ArgumentError ROPolynomialDynamicsBinding(
            system;
            time_unit="second",
            state_rate_units=["rate"],
            dynamics_policy_sha256="sha256:not-a-policy",
        )
        @test_throws DimensionMismatch ROPolynomialDynamicsBinding(
            system;
            time_unit="second",
            state_rate_units=["rate", "extra"],
            dynamics_policy_sha256=repeat("a", 64),
        )
    end

    @testset "bridges form branch components and stability stays tube local" begin
        fixture = multistable_field()
        field = fixture.field
        @test field.version == RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION
        @test field.evidence_scope ==
            RO_BRANCH_INDEXED_REGULAR_FIELD_SCOPE
        @test field.all_supplied_patches_replayed
        @test field.all_supplied_bridges_replayed
        @test length(field.patch_evidence) == 4
        @test length(field.branches) == 3
        @test field.source_replay_interval_operation_count > 0
        @test field.analysis_interval_operation_count > 0

        evidence = Dict(record.patch_certificate_sha256 => record
            for record in field.patch_evidence)
        @test evidence[fixture.lower_parent.certificate_sha256].stability_status ==
            :certified_uniformly_locally_asymptotically_stable
        @test evidence[fixture.lower_child.certificate_sha256].stability_status ==
            :certified_uniformly_locally_asymptotically_stable
        @test evidence[fixture.upper.certificate_sha256].stability_status ==
            :certified_uniformly_locally_asymptotically_stable
        @test evidence[fixture.middle.certificate_sha256].stability_status ==
            :certified_unstable_by_positive_trace
        @test evidence[fixture.middle.certificate_sha256].trace_lower_bound > 0
        @test !evidence[fixture.middle.certificate_sha256].hopf_excluded_inside_declared_tube
        @test all(record.fold_excluded_inside_declared_tube
            for record in field.patch_evidence)

        lower_branch = only(branch for branch in field.branches if
            fixture.lower_parent.certificate_sha256 in
                branch.patch_certificate_sha256s)
        @test fixture.lower_child.certificate_sha256 in
            lower_branch.patch_certificate_sha256s
        @test length(lower_branch.patch_certificate_sha256s) == 2
        @test lower_branch.bridge_certificate_sha256s ==
            (fixture.bridges[1].certificate_sha256,)
        @test lower_branch.stability_coverage_status ==
            :all_supplied_patches_certified_stable

        lower_response = evidence[
            fixture.lower_child.certificate_sha256].state_log_response_enclosure
        upper_response = evidence[
            fixture.upper.certificate_sha256].state_log_response_enclosure
        @test lower_response[1, 1] == ROExactInterval(0.0, 0.0)
        @test upper_response[1, 1] == ROExactInterval(0.0, 0.0)
        @test_throws BoundsError lower_response[2, 1]
    end

    @testset "branch log-response retains every ordered input axis" begin
        system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["concentration"],
            control_names=["u", "v"],
            control_units=["concentration", "concentration"],
            equations=[[
                term(-1.0, [1], [0, 0]),
                term(1.0, [0], [1, 0]),
                term(2.0, [0], [0, 1]),
            ]],
        )
        patch = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0, 3.0],
            control_upper=[2.0, 4.0],
            control_reference=[1.5, 3.5],
            state_reference=[8.5],
            predictor_slope=[1.0 2.0],
            remainder_lower=[-0.125],
            remainder_upper=[0.125],
            preconditioner=[-1.0;;],
        )
        binding = ROPolynomialDynamicsBinding(
            system;
            time_unit="second",
            state_rate_units=["concentration_per_second"],
            dynamics_policy_sha256=repeat("c", 64),
        )
        field = certify_ro_branch_indexed_regular_field(
            system,
            binding,
            [patch],
            RORegularSheetBridgeCertificate[],
        )
        response = only(field.patch_evidence).state_log_response_enclosure
        @test size(response) == (1, 2)
        @test response[1, 1].lower == 8 // 81
        @test response[1, 1].upper == 16 // 55
        @test response[1, 2].lower == 16 // 27
        @test response[1, 2].upper == 64 // 55
        @test length(field.branches) == 1
        @test field.maximum_witnessed_stable_root_lower_bound == 0
    end

    @testset "stability policy remains conservative when row discs cross zero" begin
        system = ROPolynomialEquilibriumSystem(
            state_names=["x", "y"],
            state_units=["concentration", "concentration"],
            control_names=["u"],
            control_units=["concentration"],
            equations=[
                [
                    term(-1.0, [1, 0], [0]),
                    term(2.0, [0, 1], [0]),
                    term(-1.0, [0, 0], [0]),
                ],
                [
                    term(-2.0, [1, 0], [0]),
                    term(-1.0, [0, 1], [0]),
                    term(3.0, [0, 0], [0]),
                ],
            ],
        )
        patch = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[1.0, 1.0],
            predictor_slope=[0.0; 0.0;;],
            remainder_lower=[-0.015625, -0.015625],
            remainder_upper=[0.015625, 0.015625],
            preconditioner=[-0.25 -0.5; 0.5 -0.25],
        )
        binding = ROPolynomialDynamicsBinding(
            system;
            time_unit="second",
            state_rate_units=[
                "concentration_per_second",
                "concentration_per_second",
            ],
            dynamics_policy_sha256=repeat("d", 64),
        )
        field = certify_ro_branch_indexed_regular_field(
            system,
            binding,
            [patch],
            RORegularSheetBridgeCertificate[],
        )
        evidence = only(field.patch_evidence)
        @test evidence.gershgorin_right_bounds == (1 // 1, 1 // 1)
        @test evidence.trace_lower_bound == -2 // 1
        @test evidence.stability_status == :unknown_stability
        @test evidence.stability_margin === nothing
        @test !evidence.hopf_excluded_inside_declared_tube
    end

    @testset "a witness proves only a stable-root lower bound" begin
        fixture = multistable_field()
        field = fixture.field
        @test length(field.multistability_witnesses) == 1
        witness = only(field.multistability_witnesses)
        @test witness.certified_stable_root_lower_bound == 2
        @test field.maximum_witnessed_stable_root_lower_bound == 2
        @test length(witness.pairwise_separations) == 1
        @test witness.pairwise_separations[1].strict_separation_margin > 0
        @test witness.control_box[1] == ROExactInterval(1.453125, 1.484375)
        @test !witness.stable_root_population_complete
        @test !witness.folds_outside_selected_tubes_excluded
        @test !witness.bifurcation_boundaries_enclosed
        @test field.stable_root_population_status == :witness_lower_bound_only
        @test !field.stable_root_population_complete
        @test !field.fold_boundaries_enclosed
        @test !field.hopf_boundaries_enclosed
        @test !field.global_continuation_certified
        @test !field.true_hysteresis_certified

        @test validate_ro_branch_indexed_regular_field(
            fixture.system,
            reverse(fixture.patches),
            reverse(fixture.bridges),
            field,
        )
        replayed = replay_ro_branch_indexed_regular_field(
            fixture.system,
            fixture.patches,
            fixture.bridges,
            field,
        )
        @test replayed.certificate_sha256 == field.certificate_sha256
        @test occursin(r"^[0-9a-f]{64}$", field.certificate_sha256)
    end

    @testset "witness selection fails closed" begin
        system, lower_parent, lower_child, middle, upper, lower_bridge =
            cubic_sources()
        binding = dynamics_binding(system)
        patches = [lower_parent, lower_child, middle, upper]

        @test_throws ROBranchIndexedFieldCertificationRejected begin
            certify_ro_branch_indexed_regular_field(
                system,
                binding,
                patches,
                [lower_bridge];
                witnesses=[(
                    control_lower=[1.453125],
                    control_upper=[1.484375],
                    patch_certificate_sha256s=[
                        lower_parent.certificate_sha256,
                        lower_child.certificate_sha256,
                    ],
                )],
            )
        end
        unstable_error = try
            certify_ro_branch_indexed_regular_field(
                system,
                binding,
                patches,
                [lower_bridge];
                witnesses=[(
                    control_lower=[1.453125],
                    control_upper=[1.484375],
                    patch_certificate_sha256s=[
                        middle.certificate_sha256,
                        upper.certificate_sha256,
                    ],
                )],
            )
            nothing
        catch caught
            caught
        end
        @test unstable_error isa ROBranchIndexedFieldCertificationRejected
        @test unstable_error.reason == :witness_patch_not_uniformly_stable

        @test_throws ROBranchIndexedFieldCertificationRejected begin
            certify_ro_branch_indexed_regular_field(
                system,
                binding,
                [lower_parent, lower_child],
                RORegularSheetBridgeCertificate[];
                witnesses=[(
                    control_lower=[1.453125],
                    control_upper=[1.484375],
                    patch_certificate_sha256s=[
                        lower_parent.certificate_sha256,
                        lower_child.certificate_sha256,
                    ],
                )],
            )
        end
        @test_throws ROBranchIndexedFieldCertificationRejected begin
            certify_ro_branch_indexed_regular_field(
                system,
                binding,
                [lower_parent, upper],
                RORegularSheetBridgeCertificate[];
                witnesses=[(
                    control_lower=[1.5],
                    control_upper=[1.515625],
                    patch_certificate_sha256s=[
                        lower_parent.certificate_sha256,
                        upper.certificate_sha256,
                    ],
                )],
            )
        end
        @test_throws ArgumentError certify_ro_branch_indexed_regular_field(
            system,
            binding,
            [lower_parent, upper],
            RORegularSheetBridgeCertificate[];
            witnesses=[(
                control_lower=[1],
                control_upper=[1.5],
                patch_certificate_sha256s=[
                    lower_parent.certificate_sha256,
                    upper.certificate_sha256,
                ],
            )],
        )
    end

    @testset "identity, raw forgery, budgets, and cancellation" begin
        fixture = multistable_field()
        field = fixture.field
        raw = Any[getfield(field, name)
            for name in fieldnames(typeof(field))]
        @test_throws MethodError ROBranchIndexedRegularField(raw...)
        completeness_index = findfirst(
            ==(:stable_root_population_complete),
            fieldnames(typeof(field)),
        )
        forged = copy(raw)
        forged[completeness_index] = true
        @test_throws ArgumentError BindingAndCatalysis.ROBranchIndexedRegularField(
            BindingAndCatalysis._ROBS_VALIDATED_TOKEN,
            forged[1:end-1]...,
        )
        @test_throws ArgumentError ROBranchIndexedFieldLimits(
            max_branches=typemax(Int),
            max_witness_branches=typemax(Int),
            max_pairwise_separations=typemax(Int),
        )

        source_limit = field.source_replay_interval_operation_count - 1
        @test_throws ROBranchIndexedFieldLimitExceeded begin
            certify_ro_branch_indexed_regular_field(
                fixture.system,
                fixture.binding,
                fixture.patches,
                fixture.bridges;
                limits=ROBranchIndexedFieldLimits(
                    max_source_replay_interval_operations=source_limit,
                ),
            )
        end
        @test_throws ROBranchIndexedFieldLimitExceeded begin
            certify_ro_branch_indexed_regular_field(
                fixture.system,
                fixture.binding,
                fixture.patches,
                fixture.bridges;
                witnesses=[(
                    control_lower=[1.453125],
                    control_upper=[1.484375],
                    patch_certificate_sha256s=[
                        fixture.lower_child.certificate_sha256,
                        fixture.upper.certificate_sha256,
                    ],
                )],
                limits=ROBranchIndexedFieldLimits(
                    max_witness_branches=1,
                    max_pairwise_separations=1,
                ),
            )
        end
        @test_throws ROBranchIndexedFieldLimitExceeded begin
            certify_ro_branch_indexed_regular_field(
                fixture.system,
                fixture.binding,
                fixture.patches,
                fixture.bridges;
                limits=ROBranchIndexedFieldLimits(
                    max_analysis_interval_operations=1,
                ),
            )
        end

        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 4 && throw(InterruptException())
        end
        @test_throws InterruptException certify_ro_branch_indexed_regular_field(
            fixture.system,
            fixture.binding,
            fixture.patches,
            fixture.bridges;
            cancel_check=cancel,
        )
        @test calls[] == 4
    end
end

end # module
