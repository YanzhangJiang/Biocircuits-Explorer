module ROSpectralHopfEventCensusContract

using Test
using LinearAlgebra
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function crossing_system(; limits=RORegularSheetLimits())
    # X=x-1, Y=y-1, mu=lambda-1:
    #   X' = mu*X - Y
    #   Y' = X + mu*Y.
    # The eigenvalues are mu +/- i. The system is deliberately linear, so its
    # first Lyapunov coefficient vanishes even though the spectral crossing is
    # simple and transverse.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [1, 0], [1]),
                term(-1.0, [0, 0], [1]),
                term(-1.0, [1, 0], [0]),
                term(-1.0, [0, 1], [0]),
                term(2.0, [0, 0], [0]),
            ],
            [
                term(1.0, [1, 0], [0]),
                term(1.0, [0, 1], [1]),
                term(-1.0, [0, 0], [1]),
                term(-1.0, [0, 1], [0]),
            ],
        ],
        limits=limits,
    )
end

function dynamics_binding(system; policy=repeat("c", 64))
    return ROPolynomialDynamicsBinding(
        system;
        time_unit="second",
        state_rate_units=fill("concentration_per_second",
            length(system.state_names)),
        dynamics_policy_sha256=policy,
    )
end

const CROSSING_AXES = [
    [0.875, 1.125],
    [0.875, 1.125],
    [0.875, 0.984375, 1.015625, 1.125],
    [0.0, 0.875, 0.984375, 1.015625, 1.125, 1.5],
]

const CROSSING_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -1.0 0.0 0.0 0.0
    0.0 0.0 0.0 -0.5
    0.0 0.0 -1.0 0.0
]

function crossing_census(
    system=crossing_system(),
    binding=dynamics_binding(system);
    kwargs...,
)
    return certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        binding;
        event_axis_breaks=CROSSING_AXES,
        event_grid_indices=[(1, 1, 2, 3)],
        event_preconditioners=[CROSSING_PRECONDITIONER],
        kwargs...,
    )
end

function stable_system(; limits=RORegularSheetLimits())
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [term(-1.0, [1, 0], [0]), term(1.0, [0, 0], [0])],
            [term(-2.0, [0, 1], [0]), term(2.0, [0, 0], [0])],
        ],
        limits=limits,
    )
end

function tangential_system(; limits=RORegularSheetLimits())
    # Replace mu by mu^2. The imaginary pair touches the axis and does not
    # cross it, so the augmented Jacobian is singular at lambda=1.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [1, 0], [2]),
                term(-1.0, [0, 0], [2]),
                term(-2.0, [1, 0], [1]),
                term(2.0, [0, 0], [1]),
                term(1.0, [1, 0], [0]),
                term(-1.0, [0, 1], [0]),
            ],
            [
                term(1.0, [1, 0], [0]),
                term(1.0, [0, 1], [2]),
                term(-1.0, [0, 0], [2]),
                term(-2.0, [0, 1], [1]),
                term(2.0, [0, 0], [1]),
                term(1.0, [0, 1], [0]),
                term(-2.0, [0, 0], [0]),
            ],
        ],
        limits=limits,
    )
end

function unstable_spectrum_system(; limits=RORegularSheetLimits())
    base = crossing_system(; limits=limits)
    padded = [
        [term(item.coefficient, [item.state_exponents..., 0],
            item.control_exponents) for item in equation]
        for equation in base.equations
    ]
    push!(padded, [
        term(2.0, [0, 0, 1], [0]),
        term(-2.0, [0, 0, 0], [0]),
    ])
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "w"],
        state_units=fill("concentration", 3),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=padded,
        limits=limits,
    )
end

function zero_hopf_system(; limits=RORegularSheetLimits())
    base = crossing_system(; limits=limits)
    padded = [
        [term(item.coefficient, [item.state_exponents..., 0],
            item.control_exponents) for item in equation]
        for equation in base.equations
    ]
    push!(padded, [
        term(1.0, [0, 0, 1], [1]),
        term(-1.0, [0, 0, 0], [1]),
        term(-1.0, [0, 0, 1], [0]),
        term(1.0, [0, 0, 0], [0]),
    ])
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "w"],
        state_units=fill("concentration", 3),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=padded,
        limits=limits,
    )
end

function double_hopf_system(; limits=RORegularSheetLimits())
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "u", "v"],
        state_units=fill("concentration", 4),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [1, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [1, 0, 0, 0], [0]),
                term(-1.0, [0, 1, 0, 0], [0]),
                term(2.0, [0, 0, 0, 0], [0]),
            ],
            [
                term(1.0, [1, 0, 0, 0], [0]),
                term(1.0, [0, 1, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 1, 0, 0], [0]),
            ],
            [
                term(1.0, [0, 0, 1, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 1, 0], [0]),
                term(-2.0, [0, 0, 0, 1], [0]),
                term(3.0, [0, 0, 0, 0], [0]),
            ],
            [
                term(2.0, [0, 0, 1, 0], [0]),
                term(1.0, [0, 0, 0, 1], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 1], [0]),
                term(-1.0, [0, 0, 0, 0], [0]),
            ],
        ],
        limits=limits,
    )
end

function small_frequency_system(; limits=RORegularSheetLimits())
    epsilon = 2.0^-10
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [1, 0], [1]),
                term(-1.0, [0, 0], [1]),
                term(-1.0, [1, 0], [0]),
                term(-epsilon, [0, 1], [0]),
                term(1.0 + epsilon, [0, 0], [0]),
            ],
            [
                term(epsilon, [1, 0], [0]),
                term(1.0, [0, 1], [1]),
                term(-1.0, [0, 0], [1]),
                term(-1.0, [0, 1], [0]),
                term(1.0 - epsilon, [0, 0], [0]),
            ],
        ],
        limits=limits,
    )
end

function repeated_pair_system(; limits=RORegularSheetLimits())
    # Two identical rotation blocks produce one repeated conjugate pair at
    # z=1. The (E,O) root is algebraically multiple and DH is singular.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "u", "v"],
        state_units=fill("concentration", 4),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [1, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [1, 0, 0, 0], [0]),
                term(-1.0, [0, 1, 0, 0], [0]),
                term(2.0, [0, 0, 0, 0], [0]),
            ],
            [
                term(1.0, [1, 0, 0, 0], [0]),
                term(1.0, [0, 1, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 1, 0, 0], [0]),
            ],
            [
                term(1.0, [0, 0, 1, 0], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 1, 0], [0]),
                term(-1.0, [0, 0, 0, 1], [0]),
                term(2.0, [0, 0, 0, 0], [0]),
            ],
            [
                term(1.0, [0, 0, 1, 0], [0]),
                term(1.0, [0, 0, 0, 1], [1]),
                term(-1.0, [0, 0, 0, 0], [1]),
                term(-1.0, [0, 0, 0, 1], [0]),
            ],
        ],
        limits=limits,
    )
end

function two_control_crossing_system(; limits=RORegularSheetLimits())
    base = crossing_system(; limits=limits)
    equations = [
        [term(item.coefficient, item.state_exponents,
            [item.control_exponents[1], 0]) for item in equation]
        for equation in base.equations
    ]
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda", "eta"],
        control_units=["concentration", "concentration"],
        equations=equations,
        limits=limits,
    )
end

function diagonal_system(state_count::Int; limits=RORegularSheetLimits(
    max_states=state_count,
))
    equations = Vector{Vector{ROPolynomialTerm}}(undef, state_count)
    for state in 1:state_count
        exponent = zeros(Int, state_count)
        exponent[state] = 1
        equations[state] = [
            term(1.0, exponent, [0]),
            term(-1.0, zeros(Int, state_count), [0]),
        ]
    end
    return ROPolynomialEquilibriumSystem(
        state_names=["x$state" for state in 1:state_count],
        state_units=fill("concentration", state_count),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=equations,
        limits=limits,
    )
end

function raw_census_with_flag(census, field::Symbol, value)
    fields = fieldnames(typeof(census))
    raw = Any[getfield(census, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROCompleteSimpleSpectralHopfEventCensus(
        BindingAndCatalysis._ROHSC_VALIDATED_TOKEN,
        raw...,
    )
end

function raw_event_with_field(event, field::Symbol, value)
    fields = fieldnames(typeof(event))
    raw = Any[getfield(event, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROSimpleSpectralHopfEvent(
        BindingAndCatalysis._ROHSC_VALIDATED_TOKEN,
        raw...,
    )
end

@testset "P8s1c0 complete simple spectral-Hopf event census contract" begin
    @testset "a complete phase-free census proves only a spectral crossing" begin
        system = crossing_system()
        binding = dynamics_binding(system)
        census = crossing_census(system, binding)
        @test census.version == RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_VERSION
        @test census.version ==
            "bne-ro-simple-spectral-hopf-event-census/v1.0.0"
        @test census.evidence_scope ==
            RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_SCOPE
        @test census.system_declaration_sha256 == system.declaration_sha256
        @test census.dynamics_binding == binding
        @test census.augmented_variable_names ==
            ("x", "y", "lambda", "frequency_squared")
        @test census.augmented_variable_units == (
            "concentration", "concentration", "concentration",
            "(second)^-2")
        @test census.partition_cell_count == 15
        @test census.spectral_hopf_event_count == 1
        @test census.spectral_hopf_free_cell_count == 14
        @test census.frequency_domain_complete_for_declared_state_control_box
        @test census.spectral_hopf_event_set_complete_inside_declared_domain
        @test census.all_events_equilibrium_regular
        @test census.all_events_have_one_simple_conjugate_pair
        @test census.all_events_real_part_transverse
        @test census.event_state_control_projections_pairwise_disjoint
        @test !census.first_lyapunov_coefficients_nonzero_certified
        @test !census.nonlinear_hopf_bifurcations_certified
        @test !census.periodic_orbit_incidence_certified
        @test !census.stable_root_population_complete
        @test !census.global_continuation_certified
        @test !census.native_residuals_certified
        @test !census.true_hysteresis_certified
        @test first(census.event_axis_breaks[end]) == 0
        @test last(census.event_axis_breaks[end]) >
            census.frequency_squared_coverage_upper_bound
        @test census.frequency_squared_coverage_upper_bound ==
            census.uniform_spectral_radius_upper_bound^2
        @test occursin(r"^[0-9a-f]{64}$", census.certificate_sha256)

        event = only(census.events)
        @test event.grid_index == (1, 1, 2, 3)
        @test event.center == (1, 1, 1, 1)
        @test event.frequency_squared_root_enclosure.lower <= 1 <=
            event.frequency_squared_root_enclosure.upper
        @test event.frequency_squared_root_enclosure.lower > 0
        @test event.state_jacobian_determinant_enclosure.lower > 0
        @test event.contraction_beta < 1
        @test event.unique_augmented_root_inside_event_box
        @test event.equilibrium_regular_certified
        @test event.nonzero_frequency_certified
        @test event.simple_conjugate_pair_certified
        @test event.real_part_transversality_certified
        @test !event.no_other_imaginary_pair_at_same_equilibrium_certified
        @test !event.first_lyapunov_coefficient_nonzero_certified
        @test !event.nonlinear_hopf_bifurcation_certified
        @test !event.periodic_orbit_incidence_certified
        @test count(cell -> cell.classification ==
            :unique_simple_spectral_hopf_event, census.cells) == 1
        @test all(cell -> cell.classification in (
            :unique_simple_spectral_hopf_event,
            :spectral_hopf_free_by_augmented_residual_exclusion,
        ), census.cells)

        @test validate_ro_complete_simple_spectral_hopf_event_census(
            system, census)
        replayed = replay_ro_complete_simple_spectral_hopf_event_census(
            system, census)
        @test replayed.certificate_sha256 == census.certificate_sha256
        @test replayed.events[1].certificate_sha256 ==
            event.certificate_sha256
    end

    @testset "an exhaustive domain can contain no imaginary-axis event" begin
        system = stable_system()
        census = certify_ro_complete_simple_spectral_hopf_event_census(
            system,
            dynamics_binding(system);
            event_axis_breaks=[
                [0.9, 1.1], [0.9, 1.1], [0.9, 1.1], [0.0, 5.0],
            ],
            event_grid_indices=Tuple[],
            event_preconditioners=Matrix{Float64}[],
        )
        @test census.partition_cell_count == 1
        @test census.spectral_hopf_event_count == 0
        @test census.spectral_hopf_free_cell_count == 1
        @test only(census.cells).excluding_augmented_equation_index == 4
        @test validate_ro_complete_simple_spectral_hopf_event_census(
            system, census)
    end

    @testset "the frequency domain has no positive lower cutoff" begin
        system = small_frequency_system()
        epsilon_squared = 2.0^-20
        census = certify_ro_complete_simple_spectral_hopf_event_census(
            system,
            dynamics_binding(system);
            event_axis_breaks=[
                [1.0 - 2.0^-20, 1.0 + 2.0^-20],
                [1.0 - 2.0^-20, 1.0 + 2.0^-20],
                [1.0 - 2.0^-12, 1.0 + 2.0^-12],
                [0.0, 3.0 * 2.0^-22, 5.0 * 2.0^-22, 2.0^-18],
            ],
            event_grid_indices=[(1, 1, 1, 2)],
            event_preconditioners=[
                [
                    0.0 1024.0 0.0 0.0
                    -1024.0 0.0 0.0 0.0
                    0.0 0.0 0.0 -0.5
                    0.0 0.0 -1.0 0.0
                ],
            ],
        )
        event = only(census.events)
        @test event.frequency_squared_root_enclosure.lower <=
            epsilon_squared <= event.frequency_squared_root_enclosure.upper
        @test event.frequency_squared_root_enclosure.upper < 2.0^-18
        @test census.frequency_domain_complete_for_declared_state_control_box
    end

    @testset "other unstable spectrum is retained rather than hidden" begin
        system = unstable_spectrum_system()
        preconditioner = [
            0.0 1.0 0.0 0.0 0.0
            -1.0 0.0 0.0 0.0 0.0
            0.0 0.0 0.5 0.0 0.0
            0.0 0.0 0.0 0.1 0.2
            0.0 0.0 0.0 0.4 -0.2
        ]
        census = certify_ro_complete_simple_spectral_hopf_event_census(
            system,
            dynamics_binding(system);
            event_axis_breaks=[
                [0.875, 1.125], [0.875, 1.125], [0.875, 1.125],
                [0.96875, 0.99609375, 1.00390625, 1.03125],
                [0.0, 0.875, 0.984375, 0.9921875, 1.0078125,
                    1.015625, 1.125, 3.875, 4.125],
            ],
            event_grid_indices=[(1, 1, 1, 2, 4)],
            event_preconditioners=[preconditioner],
        )
        @test census.spectral_hopf_event_count == 1
        @test census.all_events_real_part_transverse
        @test !census.stable_root_population_complete
    end

    @testset "degenerate or ambiguous spectral events fail closed" begin
        tangency = tangential_system()
        err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                tangency,
                dynamics_binding(tangency);
                event_axis_breaks=[
                    [0.875, 1.125],
                    [0.875, 1.125],
                    [0.99609375, 1.00390625],
                    [0.0, 0.875, 0.984375, 1.015625, 1.125],
                ],
                event_grid_indices=[(1, 1, 1, 3)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test err isa ROSpectralHopfEventCensusRejected
        @test err.reason == :augmented_contraction_not_proven

        zero_hopf = zero_hopf_system()
        zero_err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                zero_hopf,
                dynamics_binding(zero_hopf);
                event_axis_breaks=[
                    [0.875, 1.125], [0.875, 1.125], [0.875, 1.125],
                    [0.96875, 0.9990234375, 1.0009765625, 1.03125],
                    [0.0, 0.875, 0.984375, 0.99609375, 1.00390625,
                        1.015625, 1.125],
                ],
                event_grid_indices=[(1, 1, 1, 2, 4)],
                event_preconditioners=[
                    Matrix{Float64}(I, 5, 5),
                ],
            )
            nothing
        catch caught
            caught
        end
        @test zero_err isa ROSpectralHopfEventCensusRejected
        @test zero_err.reason == :equilibrium_regularity_not_proven

        repeated = repeated_pair_system()
        repeated_err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                repeated,
                dynamics_binding(repeated);
                event_axis_breaks=[
                    [0.875, 1.125], [0.875, 1.125],
                    [0.875, 1.125], [0.875, 1.125],
                    [0.984375, 1.015625], [0.0, 1.5],
                ],
                event_grid_indices=[(1, 1, 1, 1, 1, 1)],
                event_preconditioners=[Matrix{Float64}(I, 6, 6)],
            )
            nothing
        catch caught
            caught
        end
        @test repeated_err isa ROSpectralHopfEventCensusRejected
        @test repeated_err.reason == :augmented_contraction_not_proven

        double = double_hopf_system()
        axes = [
            [0.9, 1.1], [0.9, 1.1], [0.9, 1.1], [0.9, 1.1],
            [0.9, 0.99, 1.01, 1.1],
            [0.0, 0.9, 0.99, 1.01, 1.1, 3.9, 3.99, 4.01, 4.1, 4.6],
        ]
        first_preconditioner = [
            0.0 1.0 0.0 0.0 0.0 0.0
            -1.0 0.0 0.0 0.0 0.0 0.0
            0.0 0.0 0.0 0.5 0.0 0.0
            0.0 0.0 -0.5 0.0 0.0 0.0
            0.0 0.0 0.0 0.0 0.0 -1.0 / 6.0
            0.0 0.0 0.0 0.0 -1.0 / 3.0 0.0
        ]
        second_preconditioner = [
            0.0 1.0 0.0 0.0 0.0 0.0
            -1.0 0.0 0.0 0.0 0.0 0.0
            0.0 0.0 0.0 0.5 0.0 0.0
            0.0 0.0 -0.5 0.0 0.0 0.0
            0.0 0.0 0.0 0.0 0.0 1.0 / 6.0
            0.0 0.0 0.0 0.0 1.0 / 3.0 0.0
        ]
        double_err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                double,
                dynamics_binding(double);
                event_axis_breaks=axes,
                event_grid_indices=[
                    (1, 1, 1, 1, 2, 3),
                    (1, 1, 1, 1, 2, 7),
                ],
                event_preconditioners=[
                    first_preconditioner, second_preconditioner],
            )
            nothing
        catch caught
            caught
        end
        @test double_err isa ROSpectralHopfEventCensusRejected
        @test double_err.reason ==
            :multiple_imaginary_pairs_same_equilibrium_not_excluded
        @test_throws ROSpectralHopfEventCensusLimitExceeded begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                double,
                dynamics_binding(double);
                event_axis_breaks=axes,
                event_grid_indices=[
                    (1, 1, 1, 1, 2, 3),
                    (1, 1, 1, 1, 2, 7),
                ],
                event_preconditioners=[
                    first_preconditioner, second_preconditioner],
                limits=ROSpectralHopfEventCensusLimits(
                    max_projection_pairs=0),
            )
        end
    end

    @testset "coverage, boundary, source, and resource gates reject" begin
        system = crossing_system()
        binding = dynamics_binding(system)
        short_axes = copy(CROSSING_AXES)
        short_axes[4] = [0.0, 0.875, 0.984375, 1.015625, 1.125]
        err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=short_axes,
                event_grid_indices=[(1, 1, 2, 3)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test err isa ROSpectralHopfEventCensusRejected
        @test err.reason == :frequency_domain_not_complete

        boundary_axes = copy(CROSSING_AXES)
        boundary_axes[3] = [0.875, 1.0, 1.125]
        boundary_err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=boundary_axes,
                event_grid_indices=[(1, 1, 1, 3)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test boundary_err isa ROSpectralHopfEventCensusRejected
        @test boundary_err.reason ==
            :augmented_krawczyk_inclusion_not_proven

        z_boundary_axes = copy(CROSSING_AXES)
        z_boundary_axes[4] = [0.0, 0.875, 1.0, 1.125, 1.5]
        z_boundary_err = try
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=z_boundary_axes,
                event_grid_indices=[(1, 1, 2, 2)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test z_boundary_err isa ROSpectralHopfEventCensusRejected
        @test z_boundary_err.reason ==
            :augmented_krawczyk_inclusion_not_proven

        @test_throws ArgumentError begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=(
                    Float32[0.9, 1.1],
                    CROSSING_AXES[2], CROSSING_AXES[3], CROSSING_AXES[4],
                ),
                event_grid_indices=[(1, 1, 2, 3)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
        end
        @test_throws ArgumentError begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=[
                    CROSSING_AXES[1], CROSSING_AXES[2], CROSSING_AXES[3],
                    [0.1, 0.9, 1.3],
                ],
                event_grid_indices=[(1, 1, 2, 2)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
        end
        @test_throws ROSpectralHopfEventCensusLimitExceeded begin
            crossing_census(
                system,
                binding;
                limits=ROSpectralHopfEventCensusLimits(max_cells=14),
            )
        end
        @test_throws ROSpectralHopfEventCensusLimitExceeded begin
            crossing_census(
                system,
                binding;
                limits=ROSpectralHopfEventCensusLimits(max_events=0),
            )
        end
        @test_throws ROSpectralHopfEventCensusLimitExceeded begin
            crossing_census(
                system,
                binding;
                limits=ROSpectralHopfEventCensusLimits(
                    max_interval_operations=1),
            )
        end
        @test_throws InterruptException crossing_census(
            system,
            binding;
            cancel_check=() -> throw(InterruptException()),
        )
        late_cancel_count = Ref(0)
        @test_throws InterruptException crossing_census(
            system,
            binding;
            cancel_check=() -> begin
                late_cancel_count[] += 1
                late_cancel_count[] > 10 && throw(InterruptException())
            end,
        )
        @test late_cancel_count[] > 10

        nine_state = diagonal_system(9)
        nine_axes = [fill([0.875, 1.125], 10)..., [0.0, 2.0]]
        @test_throws ROSpectralHopfEventCensusLimitExceeded begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                nine_state,
                dynamics_binding(nine_state);
                event_axis_breaks=nine_axes,
                event_grid_indices=Tuple[],
                event_preconditioners=Matrix{Float64}[],
                limits=ROSpectralHopfEventCensusLimits(
                    max_interval_operations=100_000),
            )
        end

        one_state = diagonal_system(1)
        @test_throws ArgumentError begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                one_state,
                dynamics_binding(one_state);
                event_axis_breaks=[[0.875, 1.125], [0.875, 1.125],
                    [0.0, 2.0]],
                event_grid_indices=Tuple[],
                event_preconditioners=Matrix{Float64}[],
            )
        end
        two_controls = two_control_crossing_system()
        @test_throws ArgumentError begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                two_controls,
                dynamics_binding(two_controls);
                event_axis_breaks=CROSSING_AXES,
                event_grid_indices=[(1, 1, 2, 3)],
                event_preconditioners=[CROSSING_PRECONDITIONER],
            )
        end
        @test_throws ArgumentError begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=CROSSING_AXES,
                event_grid_indices=[(1, 1, 2, 3), (1, 1, 2, 3)],
                event_preconditioners=[
                    CROSSING_PRECONDITIONER, CROSSING_PRECONDITIONER],
            )
        end
        @test_throws DimensionMismatch begin
            certify_ro_complete_simple_spectral_hopf_event_census(
                system,
                binding;
                event_axis_breaks=CROSSING_AXES,
                event_grid_indices=[(1, 1, 2, 3)],
                event_preconditioners=[ones(3, 3)],
            )
        end

        foreign = stable_system()
        @test_throws ArgumentError begin
            replay_ro_complete_simple_spectral_hopf_event_census(
                foreign, crossing_census(system, binding))
        end
        @test_throws ArgumentError crossing_census(
            system, dynamics_binding(stable_system()))
    end

    @testset "stronger nonlinear and global flags cannot be forged" begin
        census = crossing_census()
        event = only(census.events)
        @test_throws ArgumentError raw_event_with_field(
            event,
            :no_other_imaginary_pair_at_same_equilibrium_certified,
            true,
        )
        @test_throws ArgumentError raw_event_with_field(
            event,
            :first_lyapunov_coefficient_nonzero_certified,
            true,
        )
        @test_throws ArgumentError raw_event_with_field(
            event, :certificate_sha256, repeat("0", 64))
        for field in (
            :first_lyapunov_coefficients_nonzero_certified,
            :nonlinear_hopf_bifurcations_certified,
            :periodic_orbit_incidence_certified,
            :stable_root_population_complete,
            :global_continuation_certified,
            :native_residuals_certified,
            :true_hysteresis_certified,
        )
            @test_throws ArgumentError raw_census_with_flag(
                census, field, true)
        end
        for field in (
            :frequency_domain_complete_for_declared_state_control_box,
            :spectral_hopf_event_set_complete_inside_declared_domain,
            :all_events_equilibrium_regular,
            :all_events_have_one_simple_conjugate_pair,
            :all_events_real_part_transverse,
            :event_state_control_projections_pairwise_disjoint,
        )
            @test_throws ArgumentError raw_census_with_flag(
                census, field, false)
        end
    end
end

end # module
