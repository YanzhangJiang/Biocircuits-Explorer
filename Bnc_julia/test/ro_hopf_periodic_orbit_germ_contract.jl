module ROHopfPeriodicOrbitGermContract

using Test
using LinearAlgebra
using BindingAndCatalysis

struct CancelProbe <: Exception end

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function add_centered_monomial!(
    polynomial::Dict{NTuple{3,Int},Float64},
    coefficient::Float64,
    exponents::NTuple{3,Int},
)
    for x_power in 0:exponents[1],
            y_power in 0:exponents[2],
            control_power in 0:exponents[3]
        expanded_coefficient = coefficient *
            binomial(exponents[1], x_power) *
            binomial(exponents[2], y_power) *
            binomial(exponents[3], control_power) *
            (-1.0)^(
                sum(exponents) - x_power - y_power - control_power)
        key = (x_power, y_power, control_power)
        polynomial[key] = get(polynomial, key, 0.0) + expanded_coefficient
        polynomial[key] == 0.0 && delete!(polynomial, key)
    end
    return polynomial
end

function centered_equation(terms)
    polynomial = Dict{NTuple{3,Int},Float64}()
    for (coefficient, exponents) in terms
        coefficient == 0.0 && continue
        add_centered_monomial!(polynomial, coefficient, exponents)
    end
    return [
        term(polynomial[key], [key[1], key[2]], [key[3]])
        for key in sort!(collect(keys(polynomial)))
    ]
end

function radial_normal_form_system(;
    crossing_speed::Float64=1.0,
    radial::Float64=-1.0,
    limits::RORegularSheetLimits=RORegularSheetLimits(),
)
    # X=x-1, Y=y-1, mu=lambda-1:
    #   X' = alpha*mu*X - Y + c*X*(X^2+Y^2)
    #   Y' = X + alpha*mu*Y + c*Y*(X^2+Y^2).
    # The Hopf crossing speed is alpha, and the P8s1c1 convention gives
    # l1=2c.  Thus the germ lies above/below the event according to
    # -sign(alpha*l1), while c<0/c>0 gives attracting/repelling radial flow.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            centered_equation([
                (crossing_speed, (1, 0, 1)),
                (-1.0, (0, 1, 0)),
                (radial, (3, 0, 0)),
                (radial, (1, 2, 0)),
            ]),
            centered_equation([
                (1.0, (1, 0, 0)),
                (crossing_speed, (0, 1, 1)),
                (radial, (2, 1, 0)),
                (radial, (0, 3, 0)),
            ]),
        ],
        limits=limits,
    )
end

function two_event_normal_form_system(;
    radial::Float64=-1.0,
    limits::RORegularSheetLimits=RORegularSheetLimits(),
)
    # With mu=lambda-1, a(lambda)=mu*(mu-1) has transverse zeros at
    # lambda=1 and lambda=2.  Their crossing speeds are respectively -1 and
    # +1, while both events have omega=1 and l1=2c.  This catches any germ
    # attribution based only on frequency or Lyapunov sign.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            centered_equation([
                (1.0, (1, 0, 2)),
                (-1.0, (1, 0, 1)),
                (-1.0, (0, 1, 0)),
                (radial, (3, 0, 0)),
                (radial, (1, 2, 0)),
            ]),
            centered_equation([
                (1.0, (1, 0, 0)),
                (1.0, (0, 1, 2)),
                (-1.0, (0, 1, 1)),
                (radial, (2, 1, 0)),
                (radial, (0, 3, 0)),
            ]),
        ],
        limits=limits,
    )
end

function unstable_transverse_normal_form_system()
    base = radial_normal_form_system(radial=-1.0)
    equations = [
        [
            term(item.coefficient, [item.state_exponents..., 0],
                item.control_exponents)
            for item in equation
        ]
        for equation in base.equations
    ]
    # W=w-1 and W'=2W.  The Hopf germ remains center-manifold attracting,
    # but its full-state periodic orbit would be transversely unstable.
    push!(equations, [
        term(2.0, [0, 0, 1], [0]),
        term(-2.0, [0, 0, 0], [0]),
    ])
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "w"],
        state_units=fill("concentration", 3),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=equations,
    )
end

function stable_system()
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [term(-1.0, [1, 0], [0]), term(1.0, [0, 0], [0])],
            [term(-2.0, [0, 1], [0]), term(2.0, [0, 0], [0])],
        ],
    )
end

function dynamics_binding(system; policy=repeat("8", 64))
    return ROPolynomialDynamicsBinding(
        system;
        time_unit="second",
        state_rate_units=fill(
            "concentration_per_second", length(system.state_names)),
        dynamics_policy_sha256=policy,
    )
end

const SINGLE_EVENT_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-8, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 1.0 + 2.0^-8],
    [0.0, 0.5, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 2.0, 16.0],
]

function single_parent_preconditioner(crossing_speed::Float64=1.0)
    return [
        0.0 1.0 0.0 0.0
        -1.0 0.0 0.0 0.0
        0.0 0.0 0.0 -1.0 / (2.0 * crossing_speed)
        0.0 0.0 -1.0 0.0
    ]
end

const TWO_EVENT_LAMBDA_AXIS = vcat(
    collect(0.875:0.03125:0.96875),
    [1.0 - 2.0^-10, 1.0 + 2.0^-10],
    collect(1.03125:0.03125:1.96875),
    [2.0 - 2.0^-10, 2.0 + 2.0^-10],
    collect(2.03125:0.03125:2.125),
)
const FIRST_TWO_EVENT_LAMBDA_CELL =
    findfirst(==(1.0 - 2.0^-10), TWO_EVENT_LAMBDA_AXIS)
const SECOND_TWO_EVENT_LAMBDA_CELL =
    findfirst(==(2.0 - 2.0^-10), TWO_EVENT_LAMBDA_AXIS)
const TWO_EVENT_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    TWO_EVENT_LAMBDA_AXIS,
    [0.0, 0.5, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 2.0, 64.0],
]
const FIRST_TWO_EVENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -1.0 0.0 0.0 0.0
    0.0 0.0 0.0 0.5
    0.0 0.0 -1.0 0.0
]
const SECOND_TWO_EVENT_PRECONDITIONER =
    single_parent_preconditioner()

const UNSTABLE_EVENT_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [0.96875, 0.99609375, 1.00390625, 1.03125],
    [0.0, 0.875, 0.984375, 0.9921875, 1.0078125,
        1.015625, 1.125, 3.875, 4.125],
]
const UNSTABLE_PARENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0 0.0
    -1.0 0.0 0.0 0.0 0.0
    0.0 0.0 0.5 0.0 0.0
    0.0 0.0 0.0 0.1 0.2
    0.0 0.0 0.0 0.4 -0.2
]

function single_parent(system; crossing_speed::Float64=1.0, kwargs...)
    return certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system);
        event_axis_breaks=SINGLE_EVENT_AXES,
        event_grid_indices=[(1, 1, 2, 3)],
        event_preconditioners=[
            single_parent_preconditioner(crossing_speed)],
        kwargs...,
    )
end

function unstable_parent(system; kwargs...)
    return certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("9", 64));
        event_axis_breaks=UNSTABLE_EVENT_AXES,
        event_grid_indices=[(1, 1, 1, 2, 4)],
        event_preconditioners=[UNSTABLE_PARENT_PRECONDITIONER],
        kwargs...,
    )
end

function two_event_parent(system; kwargs...)
    return certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("a", 64));
        event_axis_breaks=TWO_EVENT_AXES,
        event_grid_indices=[
            (1, 1, FIRST_TWO_EVENT_LAMBDA_CELL, 3),
            (1, 1, SECOND_TWO_EVENT_LAMBDA_CELL, 3),
        ],
        event_preconditioners=[
            FIRST_TWO_EVENT_PRECONDITIONER,
            SECOND_TWO_EVENT_PRECONDITIONER,
        ],
        kwargs...,
    )
end

function realify(matrix::Matrix{ComplexF64})
    return [real.(matrix) -imag.(matrix);
        imag.(matrix) real.(matrix)]
end

function bordered_matrix(
    matrix::Matrix{ComplexF64},
    top_border::Vector{ComplexF64},
    bottom_border::Vector{ComplexF64},
)
    state_count = size(matrix, 1)
    result = zeros(ComplexF64, state_count + 1, state_count + 1)
    result[1:state_count, 1:state_count] = matrix
    result[1:state_count, end] = top_border
    result[end, 1:state_count] = bottom_border
    return result
end

function lyapunov_seed(system, parent; event_index::Int=1)
    state_count = length(system.state_names)
    state_jacobian = zeros(ComplexF64, state_count, state_count)
    state_jacobian[1:2, 1:2] = ComplexF64[0 -1; 1 0]
    state_count == 3 && (state_jacobian[3, 3] = 2)
    q = zeros(ComplexF64, state_count)
    q[1:2] = ComplexF64[1, -im]
    p = q ./ 2.0
    unit = zeros(ComplexF64, state_count)
    unit[1] = 1
    right_bordered = bordered_matrix(
        state_jacobian - im * I, unit, unit)
    adjoint_bordered = bordered_matrix(
        transpose(state_jacobian) + im * I, unit, conj.(q))
    second_harmonic_matrix = 2im * I - state_jacobian
    radius = 2.0^-6
    return ROHopfLyapunovSeed(
        system,
        parent.events[event_index];
        anchor_state_index=1,
        right_eigenvector_center=q,
        right_bordered_remainder_radii=radius,
        right_bordered_preconditioner=realify(inv(right_bordered)),
        adjoint_eigenvector_center=p,
        adjoint_bordered_remainder_radii=radius,
        adjoint_bordered_preconditioner=realify(inv(adjoint_bordered)),
        zero_resolvent_center=zeros(Float64, state_count),
        zero_resolvent_remainder_radii=radius,
        zero_resolvent_preconditioner=real.(inv(state_jacobian)),
        second_harmonic_center=zeros(ComplexF64, state_count),
        second_harmonic_remainder_radii=radius,
        second_harmonic_preconditioner=
            realify(inv(second_harmonic_matrix)),
    )
end

function single_hopf_certificate(;
    radial::Float64=-1.0,
    crossing_speed::Float64=1.0,
)
    system = radial_normal_form_system(
        radial=radial, crossing_speed=crossing_speed)
    parent = single_parent(system; crossing_speed=crossing_speed)
    seed = lyapunov_seed(system, parent)
    hopf = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [seed])
    return system, parent, seed, hopf
end

function unstable_hopf_certificate()
    system = unstable_transverse_normal_form_system()
    parent = unstable_parent(system)
    seed = lyapunov_seed(system, parent)
    hopf = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [seed])
    return system, parent, seed, hopf
end

function two_event_hopf_certificate()
    system = two_event_normal_form_system()
    parent = two_event_parent(system)
    first_seed = lyapunov_seed(system, parent; event_index=1)
    second_seed = lyapunov_seed(system, parent; event_index=2)
    # Deliberately reverse caller order.  P8s1c1 and P8s1c2a must both
    # canonicalize by the complete parent event population.
    hopf = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [second_seed, first_seed])
    return system, parent, first_seed, second_seed, hopf
end

function empty_hopf_certificate()
    system = stable_system()
    parent = certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("b", 64));
        event_axis_breaks=[
            [0.875, 1.125],
            [0.875, 1.125],
            [0.875, 1.125],
            [0.0, 5.0],
        ],
        event_grid_indices=Tuple[],
        event_preconditioners=Matrix{Float64}[],
    )
    hopf = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, ROHopfLyapunovSeed[])
    return system, parent, hopf
end

function raw_germ_event_with_field(event, field::Symbol, value)
    fields = fieldnames(typeof(event))
    raw = Any[getfield(event, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROHopfPeriodicOrbitGermEvent(
        BindingAndCatalysis._ROHPG_VALIDATED_TOKEN,
        raw...,
    )
end

function raw_germ_census_with_field(census, field::Symbol, value)
    fields = fieldnames(typeof(census))
    raw = Any[getfield(census, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROCompleteHopfPeriodicOrbitGermCensus(
        BindingAndCatalysis._ROHPG_VALIDATED_TOKEN,
        raw...,
    )
end

function overwrite_exact_bigint!(target::Rational{BigInt}, value)
    exact_value = Rational{BigInt}(value)
    Base.GMP.MPZ.set!(
        numerator(target), deepcopy(numerator(exact_value)))
    Base.GMP.MPZ.set!(
        denominator(target), deepcopy(denominator(exact_value)))
    return target
end

function assert_no_explicit_or_global_claim(event)
    @test !event.explicit_periodic_orbit_enclosure_certified
    @test !event.validated_periodic_orbit_branch_certified
    @test !event.quantitative_amplitude_radius_certified
    @test !event.floquet_spectrum_certified
    @test !event.full_state_periodic_orbit_stability_certified
    @test !event.global_continuation_certified
    @test !event.true_hysteresis_certified
end

function assert_no_explicit_or_population_claim(census)
    @test !census.explicit_periodic_orbit_enclosures_certified
    @test !census.validated_periodic_orbit_branches_certified
    @test !census.quantitative_amplitude_radii_certified
    @test !census.floquet_spectra_certified
    @test !census.full_state_periodic_orbit_stabilities_certified
    @test !census.periodic_orbit_population_complete
    @test !census.stable_periodic_orbit_population_complete
    @test !census.global_continuation_certified
    @test !census.true_hysteresis_certified
end

@testset "P8s1c2a theorem-level Hopf periodic-orbit germ contract" begin
    @testset "negative l1 and positive crossing give an attracting upper-side germ" begin
        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=-1.0)
        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)

        @test census.version ==
            RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_VERSION
        @test census.theorem_version ==
            RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION
        @test census.crossing_orientation_formula_version ==
            RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION
        @test census.evidence_scope ==
            RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_SCOPE
        @test census.system_declaration_sha256 == system.declaration_sha256
        @test census.spectral_parent_census_sha256 ==
            spectral_parent.certificate_sha256
        @test census.hopf_parent_census_sha256 ==
            hopf_parent.certificate_sha256
        @test census.parent_event_count == 1
        @test census.local_periodic_orbit_germ_count == 1
        @test census.complete_parent_chain_replayed
        @test census.every_parent_event_lifted_exactly_once
        @test census.all_local_periodic_orbit_germs_exist
        @test census.all_theorem_level_hopf_event_incidences_certified
        @test census.all_original_control_sides_certified
        @test census.
            all_center_manifold_radial_stabilities_at_onset_certified
        assert_no_explicit_or_population_claim(census)

        event = only(census.events)
        @test event.version == RO_HOPF_PERIODIC_ORBIT_GERM_EVENT_VERSION
        @test event.parent_spectral_event_sha256 ==
            only(spectral_parent.events).certificate_sha256
        @test event.parent_hopf_event_sha256 ==
            only(hopf_parent.events).certificate_sha256
        @test event.original_control_name == "lambda"
        @test event.original_control_unit == "concentration"
        @test event.preconditioner_determinant_sign == -1
        @test event.state_jacobian_determinant_sign == 1
        @test event.real_part_crossing_speed_sign == 1
        @test event.first_lyapunov_coefficient_sign == -1
        @test event.original_control_side == :control_above_hopf_event
        @test event.center_manifold_radial_stability_at_onset ==
            :radially_attracting_on_center_manifold
        @test event.complete_parent_chain_required_for_authority
        @test event.nondegenerate_hopf_hypotheses_replayed
        @test event.real_part_crossing_orientation_certified
        @test event.local_periodic_orbit_germ_exists
        @test event.theorem_level_hopf_event_incidence_certified
        @test event.original_control_side_certified
        @test event.center_manifold_radial_stability_at_onset_certified
        assert_no_explicit_or_global_claim(event)

        @test validate_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent, census)
        replayed = replay_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent, census)
        @test replayed.certificate_sha256 == census.certificate_sha256
        @test replayed.events[1].certificate_sha256 ==
            event.certificate_sha256
    end

    @testset "positive l1 and positive crossing give a repelling lower-side germ" begin
        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=1.0)
        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        event = only(census.events)

        @test event.real_part_crossing_speed_sign == 1
        @test event.first_lyapunov_coefficient_sign == 1
        @test event.original_control_side == :control_below_hopf_event
        @test event.center_manifold_radial_stability_at_onset ==
            :radially_repelling_on_center_manifold
        assert_no_explicit_or_global_claim(event)
        assert_no_explicit_or_population_claim(census)

        reverse_system, reverse_spectral, _, reverse_hopf =
            single_hopf_certificate(radial=1.0, crossing_speed=-1.0)
        reverse_census =
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                reverse_system, reverse_spectral, reverse_hopf)
        reverse_event = only(reverse_census.events)
        @test reverse_event.real_part_crossing_speed_sign == -1
        @test reverse_event.first_lyapunov_coefficient_sign == 1
        @test reverse_event.original_control_side ==
            :control_above_hopf_event
        @test reverse_event.center_manifold_radial_stability_at_onset ==
            :radially_repelling_on_center_manifold
    end

    @testset "two equal-l1 events retain opposite crossing sides in parent order" begin
        system, spectral_parent, first_seed, second_seed, hopf_parent =
            two_event_hopf_certificate()
        @test hopf_parent.seeds[1].seed_sha256 == first_seed.seed_sha256
        @test hopf_parent.seeds[2].seed_sha256 == second_seed.seed_sha256

        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        @test census.parent_event_count == 2
        @test census.local_periodic_orbit_germ_count == 2
        @test Tuple(event.parent_spectral_event_sha256
            for event in census.events) ==
            Tuple(event.certificate_sha256
                for event in spectral_parent.events)
        @test Tuple(event.parent_hopf_event_sha256
            for event in census.events) ==
            Tuple(event.certificate_sha256 for event in hopf_parent.events)
        @test Tuple(event.first_lyapunov_coefficient_sign
            for event in census.events) == (-1, -1)
        @test Tuple(event.real_part_crossing_speed_sign
            for event in census.events) == (-1, 1)
        @test Tuple(event.original_control_side
            for event in census.events) == (
                :control_below_hopf_event,
                :control_above_hopf_event,
            )
        @test all(event ->
            event.center_manifold_radial_stability_at_onset ==
                :radially_attracting_on_center_manifold,
            census.events)
        @test validate_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent, census)
    end

    @testset "crossing analysis consumes only replayed spectral events" begin
        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=-1.0)
        baseline = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        baseline_event = only(baseline.events)
        determinant = only(spectral_parent.events).
            state_jacobian_determinant_enclosure
        original_lower = deepcopy(determinant.lower)
        original_upper = deepcopy(determinant.upper)

        # Rational{BigInt} is shallowly immutable: mutating its nested GMP
        # integers bypasses the outer spectral-event constructor and its
        # stored hash.  Parent replay must reconstruct this derived interval,
        # and the germ lift must not read the corrupted original afterwards.
        overwrite_exact_bigint!(determinant.lower, -original_upper)
        overwrite_exact_bigint!(determinant.upper, -original_lower)
        @test determinant.upper < 0
        @test validate_ro_complete_simple_spectral_hopf_event_census(
            system, spectral_parent)

        rebuilt = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        @test only(rebuilt.events).real_part_crossing_speed_sign ==
            baseline_event.real_part_crossing_speed_sign == 1
        @test only(rebuilt.events).original_control_side ==
            baseline_event.original_control_side ==
            :control_above_hopf_event
        @test rebuilt.certificate_sha256 == baseline.certificate_sha256
        @test validate_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent, baseline)
    end

    @testset "an extra unstable transverse mode never becomes a full-state claim" begin
        system, spectral_parent, _, hopf_parent =
            unstable_hopf_certificate()
        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        event = only(census.events)

        @test event.first_lyapunov_coefficient_sign == -1
        @test event.center_manifold_radial_stability_at_onset ==
            :radially_attracting_on_center_manifold
        @test !event.floquet_spectrum_certified
        @test !event.full_state_periodic_orbit_stability_certified
        @test !census.full_state_periodic_orbit_stabilities_certified
        @test !census.stable_periodic_orbit_population_complete
    end

    @testset "an empty complete parent has an empty complete germ lift" begin
        system, spectral_parent, hopf_parent = empty_hopf_certificate()
        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)

        @test census.parent_event_count == 0
        @test census.local_periodic_orbit_germ_count == 0
        @test isempty(census.events)
        @test census.complete_parent_chain_replayed
        @test census.every_parent_event_lifted_exactly_once
        @test census.all_local_periodic_orbit_germs_exist
        @test census.all_theorem_level_hopf_event_incidences_certified
        @test census.all_original_control_sides_certified
        @test census.
            all_center_manifold_radial_stabilities_at_onset_certified
        assert_no_explicit_or_population_claim(census)
        @test validate_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent, census)
        empty_tight_dimension =
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent;
                limits=ROHopfPeriodicOrbitGermLimits(
                    max_events=0,
                    max_determinant_dimension=1),
            )
        @test isempty(empty_tight_dimension.events)
    end

    @testset "foreign authority, tampering, and strong-claim forgery fail closed" begin
        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=-1.0)
        census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        event = only(census.events)

        foreign_system, foreign_spectral, _, foreign_hopf =
            single_hopf_certificate(radial=1.0)
        @test foreign_system.declaration_sha256 != system.declaration_sha256
        @test_throws ArgumentError begin
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system, spectral_parent, foreign_hopf)
        end
        @test_throws ArgumentError begin
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system, foreign_spectral, hopf_parent)
        end
        foreign_census = certify_ro_complete_hopf_periodic_orbit_germ_census(
            foreign_system, foreign_spectral, foreign_hopf)
        @test_throws ArgumentError begin
            replay_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent,
                foreign_census,
            )
        end

        @test_throws ArgumentError raw_germ_event_with_field(
            event, :parent_spectral_event_sha256, repeat("0", 64))
        @test_throws ArgumentError raw_germ_event_with_field(
            event, :real_part_crossing_speed_sign, -1)
        @test_throws ArgumentError raw_germ_event_with_field(
            event, :original_control_side, :control_below_hopf_event)
        for field in (
            :explicit_periodic_orbit_enclosure_certified,
            :validated_periodic_orbit_branch_certified,
            :quantitative_amplitude_radius_certified,
            :floquet_spectrum_certified,
            :full_state_periodic_orbit_stability_certified,
            :global_continuation_certified,
            :true_hysteresis_certified,
        )
            @test_throws ArgumentError raw_germ_event_with_field(
                event, field, true)
        end
        for field in (
            :explicit_periodic_orbit_enclosures_certified,
            :validated_periodic_orbit_branches_certified,
            :quantitative_amplitude_radii_certified,
            :floquet_spectra_certified,
            :full_state_periodic_orbit_stabilities_certified,
            :periodic_orbit_population_complete,
            :stable_periodic_orbit_population_complete,
            :global_continuation_certified,
            :true_hysteresis_certified,
        )
            @test_throws ArgumentError raw_germ_census_with_field(
                census, field, true)
        end
        @test_throws ArgumentError raw_germ_census_with_field(
            census, :analysis_interval_operation_count,
            census.analysis_interval_operation_count - 1)

        # A caller with access to internal constructors can recompute a
        # self-consistent local hash, but that object still has no authority:
        # source-bound replay must rebuild the canonical parent attachment.
        fake_parent_hash = repeat("f", 64)
        fake_event_hash = BindingAndCatalysis._rohpg_event_sha256(
            fake_parent_hash,
            event.parent_hopf_event_sha256,
            event.original_control_name,
            event.original_control_unit,
            event.preconditioner_determinant_sign,
            event.state_jacobian_determinant_sign,
            event.real_part_crossing_speed_sign,
            event.first_lyapunov_coefficient_sign,
            event.original_control_side,
            event.center_manifold_radial_stability_at_onset,
        )
        event_fields = fieldnames(typeof(event))
        fake_event_raw = Any[getfield(event, name) for name in event_fields]
        fake_event_raw[findfirst(==(:parent_spectral_event_sha256),
            event_fields)] = fake_parent_hash
        fake_event_raw[findfirst(==(:certificate_sha256), event_fields)] =
            fake_event_hash
        fake_event = BindingAndCatalysis.ROHopfPeriodicOrbitGermEvent(
            BindingAndCatalysis._ROHPG_VALIDATED_TOKEN,
            fake_event_raw...,
        )
        fake_events = (fake_event,)
        fake_census_hash = BindingAndCatalysis._rohpg_census_sha256(
            census.system_declaration_sha256,
            census.dynamics_binding_declaration_sha256,
            census.spectral_parent_census_sha256,
            census.hopf_parent_census_sha256,
            census.limits,
            fake_events,
            census.analysis_interval_operation_count,
        )
        census_fields = fieldnames(typeof(census))
        fake_census_raw = Any[
            getfield(census, name) for name in census_fields]
        fake_census_raw[findfirst(==(:events), census_fields)] = fake_events
        fake_census_raw[findfirst(==(:certificate_sha256), census_fields)] =
            fake_census_hash
        fake_census = BindingAndCatalysis.
            ROCompleteHopfPeriodicOrbitGermCensus(
                BindingAndCatalysis._ROHPG_VALIDATED_TOKEN,
                fake_census_raw...,
            )
        @test_throws ArgumentError begin
            replay_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent,
                fake_census,
            )
        end
    end

    @testset "determinant, matrix, parent, and total work are cumulative" begin
        dense = Rational{BigInt}[
            1 2 3
            4 5 6
            7 8 10
        ]
        dense_context = BindingAndCatalysis._RORSContext(
            RORegularSheetLimits(), () -> nothing)
        dense_wrapper = BindingAndCatalysis._rors_exact_matrix_wrapper(dense)
        @test BindingAndCatalysis._rohpg_exact_determinant_sign(
            dense_wrapper, dense_context) == -1
        swapped = copy(dense)
        swapped[1, :], swapped[2, :] =
            copy(swapped[2, :]), copy(swapped[1, :])
        swapped_context = BindingAndCatalysis._RORSContext(
            RORegularSheetLimits(), () -> nothing)
        @test BindingAndCatalysis._rohpg_exact_determinant_sign(
            BindingAndCatalysis._rors_exact_matrix_wrapper(swapped),
            swapped_context,
        ) == 1

        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=-1.0)
        baseline = certify_ro_complete_hopf_periodic_orbit_germ_census(
            system, spectral_parent, hopf_parent)
        parent_replay_operations =
            spectral_parent.analysis_interval_operation_count +
            hopf_parent.analysis_interval_operation_count
        @test baseline.analysis_interval_operation_count >
            parent_replay_operations

        static_limits = (
            ROHopfPeriodicOrbitGermLimits(max_events=0),
            ROHopfPeriodicOrbitGermLimits(max_determinant_dimension=3),
            ROHopfPeriodicOrbitGermLimits(max_preconditioner_entries=15),
            ROHopfPeriodicOrbitGermLimits(
                max_parent_replay_interval_operations=
                    parent_replay_operations - 1),
        )
        for limits in static_limits
            cancel_checks = Ref(0)
            @test_throws ROHopfPeriodicOrbitGermLimitExceeded begin
                certify_ro_complete_hopf_periodic_orbit_germ_census(
                    system,
                    spectral_parent,
                    hopf_parent;
                    limits=limits,
                    cancel_check=() -> (cancel_checks[] += 1),
                )
            end
            @test cancel_checks[] == 0
        end

        @test_throws ROHopfPeriodicOrbitGermLimitExceeded begin
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent;
                limits=ROHopfPeriodicOrbitGermLimits(
                    max_parent_replay_interval_operations=
                        parent_replay_operations,
                    max_analysis_interval_operations=
                        baseline.analysis_interval_operation_count - 1,
                ),
            )
        end

        @test_throws ArgumentError ROHopfPeriodicOrbitGermLimits(
            max_determinant_dimension=big(typemax(Int)) + 1)
        @test_throws ArgumentError ROHopfPeriodicOrbitGermLimits(
            max_parent_replay_interval_operations=2,
            max_analysis_interval_operations=1,
        )
    end

    @testset "cancellation reaches the child determinant analysis" begin
        system, spectral_parent, _, hopf_parent =
            single_hopf_certificate(radial=-1.0)

        @test_throws CancelProbe begin
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent;
                cancel_check=() -> throw(CancelProbe()),
            )
        end

        parent_checks = Ref(0)
        replay_ro_complete_nondegenerate_hopf_census(
            system,
            spectral_parent,
            hopf_parent;
            cancel_check=() -> (parent_checks[] += 1),
        )
        child_checks = Ref(0)
        # After the parent replay, one callback constructs the local context,
        # one enters the event, and the third is inside exact determinant
        # elimination after the matrix has been copied.
        child_cancel_threshold = parent_checks[] + 3
        @test_throws CancelProbe begin
            certify_ro_complete_hopf_periodic_orbit_germ_census(
                system,
                spectral_parent,
                hopf_parent;
                cancel_check=() -> begin
                    child_checks[] += 1
                    child_checks[] == child_cancel_threshold &&
                        throw(CancelProbe())
                end,
            )
        end
        @test child_checks[] == child_cancel_threshold
    end
end

end # module
