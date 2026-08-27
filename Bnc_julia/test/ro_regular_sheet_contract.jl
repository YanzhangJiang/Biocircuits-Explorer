module RORegularSheetContract

using Test
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function affine_system(; limits=RORegularSheetLimits(), state_name="x")
    return ROPolynomialEquilibriumSystem(
        state_names=[state_name],
        state_units=["concentration"],
        control_names=["u"],
        control_units=["concentration"],
        equations=[[
            term(1.0, [1], [0]),
            term(-1.0, [0], [1]),
        ]],
        limits=limits,
    )
end

function affine_patch(
    system;
    control_interval=(1.0, 2.0),
    control_reference=1.5,
    state_reference=control_reference,
    remainder=(-0.125, 0.125),
    cancel_check=() -> nothing,
)
    return certify_ro_regular_sheet_patch(
        system;
        control_lower=[control_interval[1]],
        control_upper=[control_interval[2]],
        control_reference=[control_reference],
        state_reference=[state_reference],
        predictor_slope=[1.0;;],
        remainder_lower=[remainder[1]],
        remainder_upper=[remainder[2]],
        preconditioner=[1.0;;],
        cancel_check=cancel_check,
    )
end

@testset "P5r0 exact polynomial regular-sheet contract" begin
    @testset "declarative system identity and canonical monomials" begin
        system = affine_system()
        @test RO_REGULAR_SHEET_PATCH_VERSION ==
            "bne-ro-exact-regular-sheet-patch/v1.1.0"
        @test RO_REGULAR_SHEET_BRIDGE_VERSION ==
            "bne-ro-exact-regular-sheet-bridge/v1.1.0"
        @test validate_ro_polynomial_equilibrium_system(system)
        @test system.state_names == ("x",)
        @test system.control_names == ("u",)
        @test length(system.equations) == 1
        @test system.equations[1][1].state_exponents == (0,)
        @test system.equations[1][2].state_exponents == (1,)
        @test occursin(r"^[0-9a-f]{64}$", system.declaration_sha256)

        reordered = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["concentration"],
            control_names=["u"],
            control_units=["concentration"],
            equations=[[
                term(-1.0, [0], [1]),
                term(1.0, [1], [0]),
            ]],
        )
        @test reordered.declaration_sha256 == system.declaration_sha256
        @test affine_system(state_name="other").declaration_sha256 !=
            system.declaration_sha256

        @test_throws ArgumentError ROPolynomialTerm(
            0.0, [1], [0])
        @test_throws ArgumentError ROPolynomialTerm(
            Inf, [1], [0])
        @test_throws ArgumentError ROPolynomialTerm(
            1.0, [-1], [0])
        @test_throws ArgumentError ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(1.0, [1], [0]),
                term(2.0, [1], [0]),
            ]],
        )
        @test_throws DimensionMismatch ROPolynomialEquilibriumSystem(
            state_names=["x", "y"],
            state_units=["1", "1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[term(1.0, [1, 0], [0])]],
        )
        @test_throws RORegularSheetLimitExceeded ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[term(1.0, [17], [0])]],
        )
        @test_throws MethodError ROPolynomialTerm(
            1, [1], [0])
    end

    @testset "exact affine Krawczyk patch and replay" begin
        system = affine_system()
        certificate = affine_patch(system)
        @test certificate.version == RO_REGULAR_SHEET_PATCH_VERSION
        @test certificate.evidence_scope == RO_REGULAR_SHEET_PATCH_SCOPE
        @test certificate.root_exists_for_every_control
        @test certificate.unique_inside_declared_tube
        @test !certificate.roots_outside_declared_tube_excluded
        @test certificate.state_jacobian_nonsingular_on_tube
        @test certificate.implicit_derivative_enclosed
        @test certificate.contraction_beta == 0
        @test certificate.krawczyk_image[1] == ROExactInterval(0.0, 0.0)
        @test certificate.strict_lower_margins[1] == 1 // 8
        @test certificate.strict_upper_margins[1] == 1 // 8
        @test certificate.implicit_derivative_enclosure[1, 1] ==
            ROExactInterval(1.0, 1.0)
        @test certificate.predictor_total_derivative_enclosure[1, 1] ==
            ROExactInterval(0.0, 0.0)
        @test_throws BoundsError certificate.predictor_slope[2, 1]
        @test_throws BoundsError certificate.predictor_slope[1, 2]
        @test_throws BoundsError certificate.implicit_derivative_enclosure[2, 1]
        @test certificate.control_box[1].lower == 1
        @test certificate.control_box[1].upper == 2
        @test certificate.exact_operation_count > 0
        @test occursin(r"^[0-9a-f]{64}$", certificate.branch_identity_sha256)
        @test occursin(r"^[0-9a-f]{64}$", certificate.certificate_sha256)
        @test validate_ro_regular_sheet_patch(system, certificate)
        replayed = replay_ro_regular_sheet_patch(system, certificate)
        @test replayed.certificate_sha256 == certificate.certificate_sha256

        raw_arguments = Any[getfield(certificate, field) for
            field in fieldnames(typeof(certificate))]
        @test_throws MethodError RORegularSheetPatchCertificate(
            raw_arguments...)
        outside_index = findfirst(==(:roots_outside_declared_tube_excluded),
            fieldnames(typeof(certificate)))
        forged = copy(raw_arguments)
        forged[outside_index] = true
        pop!(forged)
        @test_throws ArgumentError RORegularSheetPatchCertificate(
            forged..., Val(:validated))
        alternate_proof = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[1.5],
            predictor_slope=[1.0;;],
            remainder_lower=[-0.125],
            remainder_upper=[0.125],
            preconditioner=[0.5;;],
        )
        @test alternate_proof.branch_identity_sha256 ==
            certificate.branch_identity_sha256
        @test alternate_proof.certificate_sha256 !=
            certificate.certificate_sha256
    end

    @testset "two-state two-control exact derivative enclosure" begin
        system = ROPolynomialEquilibriumSystem(
            state_names=["x", "y"],
            state_units=["1", "1"],
            control_names=["u", "v"],
            control_units=["1", "1"],
            equations=[
                [
                    term(1.0, [1, 0], [0, 0]),
                    term(-1.0, [0, 0], [1, 0]),
                    term(-1.0, [0, 0], [0, 1]),
                ],
                [
                    term(1.0, [0, 1], [0, 0]),
                    term(-2.0, [0, 0], [0, 0]),
                    term(-1.0, [0, 0], [1, 0]),
                    term(1.0, [0, 0], [0, 1]),
                ],
            ],
        )
        certificate = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0, 1.0],
            control_upper=[1.125, 1.125],
            control_reference=[1.0, 1.0],
            state_reference=[2.0, 2.0],
            predictor_slope=[1.0 1.0; 1.0 -1.0],
            remainder_lower=[-0.125, -0.125],
            remainder_upper=[0.125, 0.125],
            preconditioner=[1.0 0.0; 0.0 1.0],
        )
        @test certificate.contraction_beta == 0
        expected = [1.0 1.0; 1.0 -1.0]
        for control in 1:2, state in 1:2
            @test certificate.implicit_derivative_enclosure[state, control] ==
                ROExactInterval(expected[state, control],
                    expected[state, control])
        end
        @test all(interval -> interval.lower > 0,
            certificate.tube_state_enclosure)
    end

    @testset "nonlinear polynomial, local uniqueness, and singular rejection" begin
        square_root_system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(1.0, [2], [0]),
                term(-1.0, [0], [1]),
            ]],
        )
        certificate = certify_ro_regular_sheet_patch(
            square_root_system;
            control_lower=[1.0],
            control_upper=[1.0009765625],
            control_reference=[1.0],
            state_reference=[1.0],
            predictor_slope=[0.5;;],
            remainder_lower=[-0.001953125],
            remainder_upper=[0.001953125],
            preconditioner=[0.5;;],
        )
        @test 0 < certificate.contraction_beta < 1
        @test certificate.unique_inside_declared_tube
        @test !certificate.roots_outside_declared_tube_excluded
        @test certificate.implicit_derivative_enclosure[1, 1].lower > 0
        @test certificate.implicit_derivative_enclosure[1, 1].upper < 1

        singular_system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(1.0, [2], [0]),
                term(-2.0, [1], [0]),
                term(1.0, [0], [0]),
            ]],
        )
        error = try
            certify_ro_regular_sheet_patch(
                singular_system;
                control_lower=[1.0],
                control_upper=[2.0],
                control_reference=[1.5],
                state_reference=[1.0],
                predictor_slope=[0.0;;],
                remainder_lower=[-0.125],
                remainder_upper=[0.125],
                preconditioner=[1.0;;],
            )
            nothing
        catch caught
            caught
        end
        @test error isa RORegularSheetCertificationRejected
        @test error.reason == :contraction_not_proven
    end

    @testset "correlated nonlinear moving branches" begin
        translated_cubic = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(-1.0, [3], [0]),
                term(3.0, [2], [1]),
                term(-3.0, [1], [2]),
                term(1.0, [0], [3]),
                term(3.0, [2], [0]),
                term(-6.0, [1], [1]),
                term(3.0, [0], [2]),
                term(-2.0, [1], [0]),
                term(2.0, [0], [1]),
            ]],
        )
        lower_branch = certify_ro_regular_sheet_patch(
            translated_cubic;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[1.5],
            predictor_slope=[1.0;;],
            remainder_lower=[-0.0625],
            remainder_upper=[0.0625],
            preconditioner=[-0.5;;],
        )
        @test lower_branch.contraction_beta < 1
        @test lower_branch.predictor_residual_enclosure[1] ==
            ROExactInterval(0.0, 0.0)
        @test lower_branch.predictor_total_derivative_enclosure[1, 1] ==
            ROExactInterval(0.0, 0.0)
        @test lower_branch.implicit_derivative_enclosure[1, 1] ==
            ROExactInterval(1.0, 1.0)
        @test lower_branch.state_jacobian_enclosure[1, 1].upper < 0
        @test validate_ro_regular_sheet_patch(
            translated_cubic, lower_branch)

        upper_branch = certify_ro_regular_sheet_patch(
            translated_cubic;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[3.5],
            predictor_slope=[1.0;;],
            remainder_lower=[-0.0625],
            remainder_upper=[0.0625],
            preconditioner=[-0.5;;],
        )
        @test upper_branch.predictor_total_derivative_enclosure[1, 1] ==
            ROExactInterval(0.0, 0.0)
        @test upper_branch.implicit_derivative_enclosure[1, 1] ==
            ROExactInterval(1.0, 1.0)
        @test upper_branch.branch_identity_sha256 !=
            lower_branch.branch_identity_sha256

        translated_two_input = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u", "v"],
            control_units=["1", "1"],
            equations=[[
                term(-1.0, [3], [0, 0]),
                term(3.0, [2], [1, 0]),
                term(6.0, [2], [0, 1]),
                term(-3.0, [1], [2, 0]),
                term(-12.0, [1], [1, 1]),
                term(-12.0, [1], [0, 2]),
                term(1.0, [0], [3, 0]),
                term(6.0, [0], [2, 1]),
                term(12.0, [0], [1, 2]),
                term(8.0, [0], [0, 3]),
                term(-1.0, [1], [0, 0]),
                term(1.0, [0], [1, 0]),
                term(2.0, [0], [0, 1]),
            ]],
        )
        surface = certify_ro_regular_sheet_patch(
            translated_two_input;
            control_lower=[1.0, 1.0],
            control_upper=[1.25, 1.125],
            control_reference=[1.125, 1.0625],
            state_reference=[3.25],
            predictor_slope=[1.0 2.0],
            remainder_lower=[-0.0625],
            remainder_upper=[0.0625],
            preconditioner=[-1.0;;],
        )
        @test surface.predictor_residual_enclosure[1] ==
            ROExactInterval(0.0, 0.0)
        @test surface.predictor_total_derivative_enclosure[1, 1] ==
            ROExactInterval(0.0, 0.0)
        @test surface.predictor_total_derivative_enclosure[1, 2] ==
            ROExactInterval(0.0, 0.0)
        @test surface.implicit_derivative_enclosure[1, 1] ==
            ROExactInterval(1.0, 1.0)
        @test surface.implicit_derivative_enclosure[1, 2] ==
            ROExactInterval(2.0, 2.0)
        @test surface.control_jacobian_enclosure[1, 1].lower == 1
        @test surface.control_jacobian_enclosure[1, 1].upper == 259 // 256
        @test surface.control_jacobian_enclosure[1, 2].lower == 2
        @test surface.control_jacobian_enclosure[1, 2].upper == 259 // 128
        @test replay_ro_regular_sheet_patch(
            translated_two_input, surface).certificate_sha256 ==
            surface.certificate_sha256
    end

    @testset "strict domains, Float64 boundary, resources, and cancellation" begin
        system = affine_system()
        @test_throws ArgumentError certify_ro_regular_sheet_patch(
            system;
            control_lower=[1],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[1.5],
            predictor_slope=[1.0;;],
            remainder_lower=[-0.125],
            remainder_upper=[0.125],
            preconditioner=[1.0;;],
        )
        @test_throws RORegularSheetCertificationRejected affine_patch(
            system; control_interval=(-1.0, 1.0), control_reference=0.5,
            state_reference=0.5)
        @test_throws RORegularSheetCertificationRejected affine_patch(
            system; state_reference=0.01, remainder=(-0.125, 0.125))
        @test_throws RORegularSheetCertificationRejected affine_patch(
            system; remainder=(0.0, 0.125))
        @test_throws RORegularSheetCertificationRejected begin
            certify_ro_regular_sheet_patch(
                system;
                control_lower=[1.0],
                control_upper=[2.0],
                control_reference=[1.5],
                state_reference=[1.5],
                predictor_slope=[1.0;;],
                remainder_lower=[-0.125],
                remainder_upper=[0.125],
                preconditioner=[0.0;;],
            )
        end

        limited_system = affine_system(limits=RORegularSheetLimits(
            max_interval_operations=10))
        @test_throws RORegularSheetLimitExceeded affine_patch(limited_system)

        expansion_limits = RORegularSheetLimits(
            max_terms_per_equation=4,
            max_total_terms=4,
            max_expanded_terms=4,
            max_total_degree=4,
        )
        expansion_system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(1.0, [4], [0]),
                term(-1.0, [0], [1]),
            ]],
            limits=expansion_limits,
        )
        @test_throws RORegularSheetLimitExceeded begin
            certify_ro_regular_sheet_patch(
                expansion_system;
                control_lower=[1.0],
                control_upper=[1.015625],
                control_reference=[1.0],
                state_reference=[2.0],
                predictor_slope=[1.0;;],
                remainder_lower=[-0.125],
                remainder_upper=[0.125],
                preconditioner=[1.0;;],
            )
        end

        bit_limits = RORegularSheetLimits(max_exact_operand_bits=1076)
        bit_system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(floatmax(Float64), [1], [0]),
                term(-floatmax(Float64), [0], [1]),
            ]],
            limits=bit_limits,
        )
        bit_error = try
            certify_ro_regular_sheet_patch(
                bit_system;
                control_lower=[1.0],
                control_upper=[1.125],
                control_reference=[1.0],
                state_reference=[1.0],
                predictor_slope=[1.0;;],
                remainder_lower=[-0.125],
                remainder_upper=[0.125],
                preconditioner=[floatmax(Float64);;],
            )
            nothing
        catch caught
            caught
        end
        @test bit_error isa RORegularSheetLimitExceeded
        @test bit_error.phase == :exact_intermediate_operand_bits

        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 2 && throw(InterruptException())
        end
        @test_throws InterruptException affine_patch(
            system; cancel_check=cancel)
        @test calls[] == 2
    end

    @testset "overlap bridge proves shared branch identity" begin
        system = affine_system()
        parent = affine_patch(
            system;
            control_interval=(1.0, 1.5),
            control_reference=1.25,
        )
        child = affine_patch(
            system;
            control_interval=(1.25, 2.0),
            control_reference=1.625,
        )
        bridge = certify_ro_regular_sheet_bridge(
            system,
            parent,
            child;
            overlap_lower=[1.25],
            overlap_upper=[1.5],
            bridge_control_reference=[1.375],
            bridge_state_reference=[1.375],
            bridge_predictor_slope=[1.0;;],
            bridge_remainder_lower=[-0.25],
            bridge_remainder_upper=[0.25],
            bridge_preconditioner=[1.0;;],
        )
        @test bridge.version == RO_REGULAR_SHEET_BRIDGE_VERSION
        @test bridge.evidence_scope == RO_REGULAR_SHEET_BRIDGE_SCOPE
        @test bridge.parent_tube_contained
        @test bridge.child_tube_contained
        @test bridge.same_root_on_overlap
        @test bridge.inherited_branch_identity_sha256 ==
            parent.branch_identity_sha256
        @test bridge.bridge_patch.unique_inside_declared_tube
        @test validate_ro_regular_sheet_bridge(
            system, parent, child, bridge)
        @test replay_ro_regular_sheet_bridge(
            system, parent, child, bridge).certificate_sha256 ==
            bridge.certificate_sha256
        bridge_arguments = Any[getfield(bridge, field) for
            field in fieldnames(typeof(bridge))]
        @test_throws MethodError RORegularSheetBridgeCertificate(
            bridge_arguments...)
        shared_root_index = findfirst(==(:same_root_on_overlap),
            fieldnames(typeof(bridge)))
        forged_bridge = copy(bridge_arguments)
        forged_bridge[shared_root_index] = false
        pop!(forged_bridge)
        @test_throws ArgumentError RORegularSheetBridgeCertificate(
            forged_bridge..., Val(:validated))

        @test_throws RORegularSheetCertificationRejected begin
            certify_ro_regular_sheet_bridge(
                system,
                parent,
                child;
                overlap_lower=[1.0],
                overlap_upper=[1.125],
                bridge_control_reference=[1.0625],
                bridge_state_reference=[1.0625],
                bridge_predictor_slope=[1.0;;],
                bridge_remainder_lower=[-0.25],
                bridge_remainder_upper=[0.25],
                bridge_preconditioner=[1.0;;],
            )
        end
        containment_error = try
            certify_ro_regular_sheet_bridge(
                system,
                parent,
                child;
                overlap_lower=[1.25],
                overlap_upper=[1.5],
                bridge_control_reference=[1.375],
                bridge_state_reference=[1.375],
                bridge_predictor_slope=[1.0;;],
                bridge_remainder_lower=[-0.0625],
                bridge_remainder_upper=[0.0625],
                bridge_preconditioner=[1.0;;],
            )
            nothing
        catch caught
            caught
        end
        @test containment_error isa RORegularSheetCertificationRejected
        @test containment_error.reason ==
            :parent_tube_not_contained_in_bridge

        limited_budget = 2 * max(
            parent.exact_operation_count,
            child.exact_operation_count,
        )
        limited_system = affine_system(limits=RORegularSheetLimits(
            max_interval_operations=limited_budget))
        limited_parent = affine_patch(
            limited_system;
            control_interval=(1.0, 1.5),
            control_reference=1.25,
        )
        limited_child = affine_patch(
            limited_system;
            control_interval=(1.25, 2.0),
            control_reference=1.625,
        )
        @test_throws RORegularSheetLimitExceeded begin
            certify_ro_regular_sheet_bridge(
                limited_system,
                limited_parent,
                limited_child;
                overlap_lower=[1.25],
                overlap_upper=[1.5],
                bridge_control_reference=[1.375],
                bridge_state_reference=[1.375],
                bridge_predictor_slope=[1.0;;],
                bridge_remainder_lower=[-0.25],
                bridge_remainder_upper=[0.25],
                bridge_preconditioner=[1.0;;],
            )
        end

        bridge_calls = Ref(0)
        bridge_cancel = () -> begin
            bridge_calls[] += 1
            bridge_calls[] >= 2 && throw(InterruptException())
        end
        @test_throws InterruptException certify_ro_regular_sheet_bridge(
            system,
            parent,
            child;
            overlap_lower=[1.25],
            overlap_upper=[1.5],
            bridge_control_reference=[1.375],
            bridge_state_reference=[1.375],
            bridge_predictor_slope=[1.0;;],
            bridge_remainder_lower=[-0.25],
            bridge_remainder_upper=[0.25],
            bridge_preconditioner=[1.0;;],
            cancel_check=bridge_cancel,
        )
        @test bridge_calls[] == 2
    end
end

end # module
