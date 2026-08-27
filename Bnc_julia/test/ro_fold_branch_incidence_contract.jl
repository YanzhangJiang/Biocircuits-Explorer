module ROFoldBranchIncidenceContract

using Test
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function fold_system(; limits=RORegularSheetLimits())
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[[
            term(1.0, [2], [0]),
            term(-2.0, [1], [0]),
            term(2.0, [0], [0]),
            term(-1.0, [0], [1]),
        ]],
        limits=limits,
    )
end

function fold_census(system)
    return certify_ro_complete_simple_fold_event_census(
        system;
        event_axis_breaks=[
            [0.98, 0.99, 1.01, 1.02],
            [0.94, 0.99, 1.01, 1.06],
        ],
        event_grid_indices=[(2, 2)],
        event_preconditioners=[[0.0 0.5; -1.0 0.0]],
    )
end

function local_patch(
    branch_system;
    control_lower,
    control_upper,
    control_reference,
    state_reference,
    predictor_slope,
    remainder,
)
    return certify_ro_regular_sheet_patch(
        branch_system;
        control_lower=[control_lower],
        control_upper=[control_upper],
        control_reference=[control_reference],
        state_reference=[state_reference],
        predictor_slope=[predictor_slope;;],
        remainder_lower=[-remainder],
        remainder_upper=[remainder],
        preconditioner=[-1.0;;],
    )
end

function branch_fixture(; limits=RORegularSheetLimits())
    system = fold_system(limits=limits)
    census = fold_census(system)
    branch_system = ro_fold_branch_coordinate_system(system, 1)
    central = local_patch(
        branch_system;
        control_lower=0.99,
        control_upper=1.01,
        control_reference=1.0,
        state_reference=1.0,
        predictor_slope=0.0,
        remainder=0.05,
    )
    local_halves = RORegularSheetPatchCertificate[]
    bridges = RORegularSheetBridgeCertificate[]
    regular_patches = RORegularSheetPatchCertificate[]
    for sign in (-1.0, 1.0)
        q = sign * 2.0^-7
        state_reference = 1.0 + q
        control_reference = 1.0 + q^2
        half_width = 2.0^-40
        half = local_patch(
            branch_system;
            control_lower=state_reference - half_width,
            control_upper=state_reference + half_width,
            control_reference=state_reference,
            state_reference=control_reference,
            predictor_slope=2q,
            remainder=2.0^-37,
        )
        bridge = certify_ro_regular_sheet_bridge(
            branch_system,
            central,
            half;
            overlap_lower=[state_reference - half_width],
            overlap_upper=[state_reference + half_width],
            bridge_control_reference=[state_reference],
            bridge_state_reference=[1.0],
            bridge_predictor_slope=[0.0;;],
            bridge_remainder_lower=[-0.05],
            bridge_remainder_upper=[0.05],
            bridge_preconditioner=[-1.0;;],
        )
        original_slope = 1.0 / (2q)
        regular = certify_ro_regular_sheet_patch(
            system;
            control_lower=[control_reference - 2.0^-32],
            control_upper=[control_reference + 2.0^-32],
            control_reference=[control_reference],
            state_reference=[state_reference],
            predictor_slope=[original_slope;;],
            remainder_lower=[-2.0^-11],
            remainder_upper=[2.0^-11],
            preconditioner=[original_slope;;],
        )
        push!(local_halves, half)
        push!(bridges, bridge)
        push!(regular_patches, regular)
    end
    certificate = certify_ro_simple_fold_branch_incidence(
        system,
        census,
        only(census.events),
        central,
        local_halves[1],
        local_halves[2],
        bridges[1],
        bridges[2],
        regular_patches[1],
        regular_patches[2];
        chart_state_index=1,
    )
    return (
        system=system,
        census=census,
        branch_system=branch_system,
        central=central,
        lower_local=local_halves[1],
        upper_local=local_halves[2],
        lower_bridge=bridges[1],
        upper_bridge=bridges[2],
        lower_regular=regular_patches[1],
        upper_regular=regular_patches[2],
        certificate=certificate,
    )
end

function three_fold_corridor_fixture()
    # F=((x-a)(x-b))^2 + 1 - lambda has simple folds at a, (a+b)/2,
    # and b. The fine x grid makes the event population complete while keeping
    # the three event boxes disjoint.
    a = 1.0
    b = 1.0 + 2.0^-6
    middle = (a + b) / 2
    middle_lambda = 1.0 + 2.0^-28
    coefficients = (
        a^2 * b^2 + 1.0,
        -2a * b * (a + b),
        a^2 + 4a * b + b^2,
        -2(a + b),
        1.0,
    )
    system = ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["1"],
        control_names=["lambda"],
        control_units=["1"],
        equations=[[
            term(coefficients[5], [4], [0]),
            term(coefficients[4], [3], [0]),
            term(coefficients[3], [2], [0]),
            term(coefficients[2], [1], [0]),
            term(coefficients[1], [0], [0]),
            term(-1.0, [0], [1]),
        ]],
    )
    step = 2.0^-14
    padding_cells = 164
    state_axis = [
        a - (padding_cells + 0.5) * step + index * step
        for index in 0:(2padding_cells + 257)
    ]
    lambda_radius = 2.0^-34
    control_axis = [
        0.4,
        1.0 - lambda_radius,
        1.0 + lambda_radius,
        middle_lambda - lambda_radius,
        middle_lambda + lambda_radius,
        1.6,
    ]
    census = certify_ro_complete_simple_fold_event_census(
        system;
        event_axis_breaks=[state_axis, control_axis],
        event_grid_indices=[
            (padding_cells + 1, 2),
            (padding_cells + 129, 4),
            (padding_cells + 257, 2),
        ],
        event_preconditioners=[
            [0.0 2048.0; -1.0 0.0],
            [0.0 -4096.0; -1.0 0.0],
            [0.0 2048.0; -1.0 0.0],
        ],
        limits=ROFoldEventCensusLimits(
            max_axis_breakpoints_per_variable=600,
            max_cells=4000,
        ),
    )
    branch_system = ro_fold_branch_coordinate_system(system, 1)
    central = local_patch(
        branch_system;
        control_lower=0.999,
        control_upper=1.017,
        control_reference=1.008,
        state_reference=1.0,
        predictor_slope=0.0,
        remainder=0.5,
    )
    selected_event = only(event for event in census.events
        if event.center[1] == Rational{BigInt}(b))
    return (
        system=system,
        census=census,
        selected_event=selected_event,
        central=central,
        middle=middle,
    )
end

function raw_incidence_with_flag(certificate, field::Symbol, value)
    fields = fieldnames(typeof(certificate))
    raw = Any[getfield(certificate, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROSimpleFoldBranchIncidenceCertificate(
        BindingAndCatalysis._ROFI_VALIDATED_TOKEN,
        raw...,
    )
end

function raw_half_with_flag(incidence, field::Symbol, value)
    fields = fieldnames(typeof(incidence))
    raw = Any[getfield(incidence, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROFoldHalfBranchIncidence(
        BindingAndCatalysis._ROFI_VALIDATED_TOKEN,
        raw...,
    )
end

@testset "P8s1b1 local two-half-branch incidence contract" begin
    fixture = branch_fixture()

    @testset "coordinate permutation remains declarative and replayable" begin
        system = fixture.branch_system
        @test system.state_names == ("lambda",)
        @test system.state_units == ("concentration",)
        @test system.control_names == ("x",)
        @test system.control_units == ("concentration",)
        @test validate_ro_polynomial_equilibrium_system(system)
        @test system.declaration_sha256 ==
            ro_fold_branch_coordinate_system(fixture.system, 1).
                declaration_sha256
        @test_throws BoundsError ro_fold_branch_coordinate_system(
            fixture.system, 2)

        two_state = ROPolynomialEquilibriumSystem(
            state_names=["x", "y"],
            state_units=["1", "1"],
            control_names=["lambda"],
            control_units=["1"],
            equations=[
                [
                    term(1.0, [2, 0], [0]),
                    term(-2.0, [1, 0], [0]),
                    term(2.0, [0, 0], [0]),
                    term(-1.0, [0, 0], [1]),
                ],
                [
                    term(1.0, [0, 1], [0]),
                    term(-2.0, [0, 0], [0]),
                ],
            ],
        )
        two_state_chart = ro_fold_branch_coordinate_system(two_state, 1)
        @test two_state_chart.state_names == ("y", "lambda")
        @test two_state_chart.control_names == ("x",)
        @test length(two_state_chart.equations) == 2
        @test validate_ro_polynomial_equilibrium_system(two_state_chart)
    end

    @testset "one local fold chart attaches both certified half branches" begin
        certificate = fixture.certificate
        @test certificate.version == RO_SIMPLE_FOLD_BRANCH_INCIDENCE_VERSION
        @test certificate.version ==
            "bne-ro-simple-fold-branch-incidence/v1.0.0"
        @test certificate.evidence_scope ==
            RO_SIMPLE_FOLD_BRANCH_INCIDENCE_SCOPE
        @test certificate.system_declaration_sha256 ==
            fixture.system.declaration_sha256
        @test certificate.fold_event_census_sha256 ==
            fixture.census.certificate_sha256
        @test certificate.fold_event_certificate_sha256 ==
            only(fixture.census.events).certificate_sha256
        @test certificate.branch_coordinate_system_sha256 ==
            fixture.branch_system.declaration_sha256
        @test certificate.chart_state_index == 1
        @test certificate.event_on_local_branch_certified
        @test certificate.two_local_half_branches_certified
        @test certificate.adjacent_regular_sheet_incidence_certified
        @test !certificate.original_control_two_root_side_certified
        @test !certificate.remote_component_identity_certified
        @test !certificate.root_population_complete_inside_declared_domain
        @test !certificate.stable_root_population_complete
        @test !certificate.hopf_event_set_complete
        @test !certificate.global_continuation_certified
        @test !certificate.true_hysteresis_certified
        @test occursin(r"^[0-9a-f]{64}$", certificate.certificate_sha256)

        event_chart = certificate.event_augmented_root_enclosure[1]
        @test fixture.lower_local.control_box[1].upper < event_chart.lower
        @test event_chart.upper < fixture.upper_local.control_box[1].lower
        @test certificate.lower_incidence.side == :lower_chart_side
        @test certificate.upper_incidence.side == :upper_chart_side
        for incidence in (
            certificate.lower_incidence,
            certificate.upper_incidence,
        )
            @test incidence.local_half_strictly_separated_from_event
            @test incidence.local_tube_strictly_inside_regular_patch
            @test incidence.same_equilibrium_branch_on_overlap
            @test incidence.intervening_fold_event_excluded
            @test occursin(r"^[0-9a-f]{64}$", incidence.evidence_sha256)
            @test all(interval -> interval.lower < interval.upper,
                incidence.regular_remainder_enclosure)
            @test length(incidence.corridor_augmented_enclosure) == 2
        end
        @test certificate.lower_incidence.regular_patch_sha256 ==
            fixture.lower_regular.certificate_sha256
        @test certificate.upper_incidence.regular_patch_sha256 ==
            fixture.upper_regular.certificate_sha256

        @test validate_ro_simple_fold_branch_incidence(
            fixture.system,
            fixture.census,
            only(fixture.census.events),
            fixture.central,
            fixture.lower_local,
            fixture.upper_local,
            fixture.lower_bridge,
            fixture.upper_bridge,
            fixture.lower_regular,
            fixture.upper_regular,
            certificate,
        )
        replayed = replay_ro_simple_fold_branch_incidence(
            fixture.system,
            fixture.census,
            only(fixture.census.events),
            fixture.central,
            fixture.lower_local,
            fixture.upper_local,
            fixture.lower_bridge,
            fixture.upper_bridge,
            fixture.lower_regular,
            fixture.upper_regular,
            certificate,
        )
        @test replayed.certificate_sha256 == certificate.certificate_sha256
    end

    @testset "wrong side, wrong branch, identity, and budgets fail closed" begin
        event = only(fixture.census.events)
        @test_throws ROFoldBranchIncidenceRejected begin
            certify_ro_simple_fold_branch_incidence(
                fixture.system,
                fixture.census,
                event,
                fixture.central,
                fixture.upper_local,
                fixture.lower_local,
                fixture.upper_bridge,
                fixture.lower_bridge,
                fixture.upper_regular,
                fixture.lower_regular;
                chart_state_index=1,
            )
        end
        wrong_regular = try
            certify_ro_simple_fold_branch_incidence(
                fixture.system,
                fixture.census,
                event,
                fixture.central,
                fixture.lower_local,
                fixture.upper_local,
                fixture.lower_bridge,
                fixture.upper_bridge,
                fixture.upper_regular,
                fixture.lower_regular;
                chart_state_index=1,
            )
            nothing
        catch caught
            caught
        end
        @test wrong_regular isa ROFoldBranchIncidenceRejected
        @test wrong_regular.reason == :local_tube_not_inside_regular_patch

        fields = fieldnames(typeof(fixture.certificate))
        raw = Any[getfield(fixture.certificate, name) for name in fields]
        @test_throws MethodError ROSimpleFoldBranchIncidenceCertificate(raw...)
        @test_throws ArgumentError raw_incidence_with_flag(
            fixture.certificate,
            :remote_component_identity_certified,
            true,
        )
        @test_throws ArgumentError raw_incidence_with_flag(
            fixture.certificate,
            :original_control_two_root_side_certified,
            true,
        )
        @test_throws ArgumentError raw_half_with_flag(
            fixture.certificate.lower_incidence,
            :same_equilibrium_branch_on_overlap,
            false,
        )
        @test_throws ArgumentError raw_half_with_flag(
            fixture.certificate.lower_incidence,
            :intervening_fold_event_excluded,
            false,
        )

        foreign_system = fold_system(limits=RORegularSheetLimits(
            max_interval_operations=1_999_999))
        @test_throws ArgumentError replay_ro_simple_fold_branch_incidence(
            foreign_system,
            fixture.census,
            event,
            fixture.central,
            fixture.lower_local,
            fixture.upper_local,
            fixture.lower_bridge,
            fixture.upper_bridge,
            fixture.lower_regular,
            fixture.upper_regular,
            fixture.certificate,
        )
        foreign_census = certify_ro_complete_simple_fold_event_census(
            fixture.system;
            event_axis_breaks=[
                [0.97, 0.99, 1.01, 1.03],
                [0.97, 0.99, 1.01, 1.03],
            ],
            event_grid_indices=[(2, 2)],
            event_preconditioners=[[0.0 0.5; -1.0 0.0]],
        )
        @test_throws ArgumentError replay_ro_simple_fold_branch_incidence(
            fixture.system,
            foreign_census,
            only(foreign_census.events),
            fixture.central,
            fixture.lower_local,
            fixture.upper_local,
            fixture.lower_bridge,
            fixture.upper_bridge,
            fixture.lower_regular,
            fixture.upper_regular,
            fixture.certificate,
        )

        @test_throws ROFoldBranchIncidenceLimitExceeded begin
            certify_ro_simple_fold_branch_incidence(
                fixture.system,
                fixture.census,
                event,
                fixture.central,
                fixture.lower_local,
                fixture.upper_local,
                fixture.lower_bridge,
                fixture.upper_bridge,
                fixture.lower_regular,
                fixture.upper_regular;
                chart_state_index=1,
                limits=ROFoldBranchIncidenceLimits(
                    max_source_replay_interval_operations=0),
            )
        end
        @test_throws ROFoldBranchIncidenceLimitExceeded begin
            certify_ro_simple_fold_branch_incidence(
                fixture.system,
                fixture.census,
                event,
                fixture.central,
                fixture.lower_local,
                fixture.upper_local,
                fixture.lower_bridge,
                fixture.upper_bridge,
                fixture.lower_regular,
                fixture.upper_regular;
                chart_state_index=1,
                limits=ROFoldBranchIncidenceLimits(
                    max_analysis_interval_operations=1),
            )
        end

        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 2 && throw(InterruptException())
        end
        @test_throws InterruptException begin
            certify_ro_simple_fold_branch_incidence(
                fixture.system,
                fixture.census,
                event,
                fixture.central,
                fixture.lower_local,
                fixture.upper_local,
                fixture.lower_bridge,
                fixture.upper_bridge,
                fixture.lower_regular,
                fixture.upper_regular;
                chart_state_index=1,
                cancel_check=cancel,
            )
        end
        @test calls[] == 2
    end

    @testset "a second fold in the chart corridor blocks adjacency" begin
        three_fold = three_fold_corridor_fixture()
        context = BindingAndCatalysis._RORSContext(
            three_fold.system.limits, () -> nothing)
        selected_root = BindingAndCatalysis._rofi_event_root_enclosure(
            three_fold.selected_event, context)
        blocked = try
            BindingAndCatalysis._rofi_fold_free_corridor(
                :lower_chart_side,
                three_fold.census,
                three_fold.selected_event,
                selected_root[1],
                three_fold.central,
                ROExactInterval(1.004, 1.005),
                1,
                context,
            )
            nothing
        catch caught
            caught
        end
        @test three_fold.census.fold_event_count == 3
        @test 1.004 < three_fold.middle <
            three_fold.selected_event.center[1]
        @test blocked isa ROFoldBranchIncidenceRejected
        @test blocked.reason == :intervening_fold_event_not_excluded
    end
end

end # module
