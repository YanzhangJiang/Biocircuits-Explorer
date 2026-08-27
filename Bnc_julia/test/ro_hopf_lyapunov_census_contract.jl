module ROHopfLyapunovCensusContract

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
            (-1.0)^(sum(exponents) - x_power - y_power - control_power)
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

function normal_form_system(;
    radial::Float64=0.0,
    first_quadratic::Float64=0.0,
    second_quadratic::Float64=0.0,
    quartic::Float64=0.0,
    limits::RORegularSheetLimits=RORegularSheetLimits(),
)
    # X=x-1, Y=y-1, mu=lambda-1:
    #   X' = mu*X - Y + a*X^2 + c*X*(X^2+Y^2)
    #   Y' = X + mu*Y + b*X^2 + c*Y*(X^2+Y^2).
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            centered_equation([
                (1.0, (1, 0, 1)),
                (-1.0, (0, 1, 0)),
                (first_quadratic, (2, 0, 0)),
                (radial, (3, 0, 0)),
                (radial, (1, 2, 0)),
                (quartic, (4, 0, 0)),
            ]),
            centered_equation([
                (1.0, (1, 0, 0)),
                (1.0, (0, 1, 1)),
                (second_quadratic, (2, 0, 0)),
                (radial, (2, 1, 0)),
                (radial, (0, 3, 0)),
            ]),
        ],
        limits=limits,
    )
end

function nonnormal_normal_form_system(;
    radial::Float64=-1.0,
    limits::RORegularSheetLimits=RORegularSheetLimits(),
)
    # The linear change X=2u, Y=v applied to the omega=2 radial normal form:
    #   X' = mu*X - 4Y + c*X*(X^2/4 + Y^2)
    #   Y' = X + mu*Y + c*Y*(X^2/4 + Y^2).
    # At the event A=[0 -4; 1 0], q=[1,-i/2], while the Hermitian adjoint
    # p=[1/2,-i] is not proportional to q.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            centered_equation([
                (1.0, (1, 0, 1)),
                (-4.0, (0, 1, 0)),
                (radial / 4.0, (3, 0, 0)),
                (radial, (1, 2, 0)),
            ]),
            centered_equation([
                (1.0, (1, 0, 0)),
                (1.0, (0, 1, 1)),
                (radial / 4.0, (2, 1, 0)),
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
    # lambda=1 and lambda=2. The positive equilibrium X=Y=0 is fixed and both
    # events have omega=1 and the same nonzero radial first Lyapunov sign.
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

function dynamics_binding(system; policy=repeat("d", 64))
    return ROPolynomialDynamicsBinding(
        system;
        time_unit="second",
        state_rate_units=fill("concentration_per_second",
            length(system.state_names)),
        dynamics_policy_sha256=policy,
    )
end

const HOPF_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-8, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 1.0 + 2.0^-8],
    [0.0, 0.5, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 2.0, 16.0],
]

const RADIAL_PARENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -1.0 0.0 0.0 0.0
    0.0 0.0 0.0 -0.5
    0.0 0.0 -1.0 0.0
]

const QUADRATIC_PARENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -1.0 0.0 0.0 0.0
    0.0 -1.0 0.0 -0.5
    0.0 2.0 -1.0 0.0
]

const NONNORMAL_PARENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -0.25 0.0 0.0 0.0
    0.0 0.0 0.0 -0.5
    0.0 0.0 -1.0 0.0
]

const NONNORMAL_HOPF_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-8, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 1.0 + 2.0^-8],
    [0.0, 2.0, 4.0 - 2.0^-8,
        4.0 + 2.0^-8, 8.0, 64.0],
]

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

const TWO_EVENT_HOPF_AXES = [
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    [1.0 - 2.0^-14, 1.0 + 2.0^-14],
    TWO_EVENT_LAMBDA_AXIS,
    [0.0, 0.5, 1.0 - 2.0^-10,
        1.0 + 2.0^-10, 2.0, 64.0],
]

const FIRST_TWO_EVENT_PARENT_PRECONDITIONER = [
    0.0 1.0 0.0 0.0
    -1.0 0.0 0.0 0.0
    0.0 0.0 0.0 0.5
    0.0 0.0 -1.0 0.0
]

const SECOND_TWO_EVENT_PARENT_PRECONDITIONER =
    RADIAL_PARENT_PRECONDITIONER

function parent_census(system; quadratic::Bool=false, kwargs...)
    return certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system);
        event_axis_breaks=HOPF_AXES,
        event_grid_indices=[(1, 1, 2, 3)],
        event_preconditioners=[quadratic ?
            QUADRATIC_PARENT_PRECONDITIONER :
            RADIAL_PARENT_PRECONDITIONER],
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

function lyapunov_seed(
    system,
    parent;
    quadratic::Bool=false,
    anchor::Int=1,
    radius::Float64=2.0^-6,
    preconditioner_override=nothing,
    event_index::Int=1,
)
    event = parent.events[event_index]
    state_count = 2
    state_jacobian = ComplexF64[0 -1; 1 0]
    q = anchor == 1 ? ComplexF64[1, -im] : ComplexF64[im, 1]
    p = q ./ 2.0
    unit = zeros(ComplexF64, state_count)
    unit[anchor] = 1.0
    right_bordered = bordered_matrix(
        state_jacobian - im * I,
        unit,
        unit,
    )
    adjoint_bordered = bordered_matrix(
        transpose(state_jacobian) + im * I,
        unit,
        conj.(q),
    )
    second_harmonic_matrix = 2im * I - state_jacobian
    h11 = quadratic ? Float64[2.0, -2.0] : zeros(Float64, 2)
    h20 = quadratic ?
        ComplexF64[(2.0 - 4im) / 3.0, (-2.0 - 4im) / 3.0] :
        zeros(ComplexF64, 2)
    right_preconditioner = preconditioner_override === nothing ?
        realify(inv(right_bordered)) : preconditioner_override
    return ROHopfLyapunovSeed(
        system,
        event;
        anchor_state_index=anchor,
        right_eigenvector_center=q,
        right_bordered_remainder_radii=radius,
        right_bordered_preconditioner=right_preconditioner,
        adjoint_eigenvector_center=p,
        adjoint_bordered_remainder_radii=radius,
        adjoint_bordered_preconditioner=realify(inv(adjoint_bordered)),
        zero_resolvent_center=h11,
        zero_resolvent_remainder_radii=radius,
        zero_resolvent_preconditioner=Float64[0 1; -1 0],
        second_harmonic_center=h20,
        second_harmonic_remainder_radii=radius,
        second_harmonic_preconditioner=
            realify(inv(second_harmonic_matrix)),
    )
end

function nonnormal_certificate()
    system = nonnormal_normal_form_system()
    parent = certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("f", 64));
        event_axis_breaks=NONNORMAL_HOPF_AXES,
        event_grid_indices=[(1, 1, 2, 3)],
        event_preconditioners=[NONNORMAL_PARENT_PRECONDITIONER],
    )
    state_jacobian = ComplexF64[0 -4; 1 0]
    q = ComplexF64[1, -0.5im]
    p = ComplexF64[0.5, -im]
    unit = ComplexF64[1, 0]
    right_bordered = bordered_matrix(
        state_jacobian - 2im * I, unit, unit)
    adjoint_bordered = bordered_matrix(
        transpose(state_jacobian) + 2im * I, unit, conj.(q))
    second_harmonic_matrix = 4im * I - state_jacobian
    seed = ROHopfLyapunovSeed(
        system,
        only(parent.events);
        anchor_state_index=1,
        right_eigenvector_center=q,
        right_bordered_remainder_radii=2.0^-6,
        right_bordered_preconditioner=realify(inv(right_bordered)),
        adjoint_eigenvector_center=p,
        adjoint_bordered_remainder_radii=2.0^-6,
        adjoint_bordered_preconditioner=realify(inv(adjoint_bordered)),
        zero_resolvent_center=zeros(Float64, 2),
        zero_resolvent_remainder_radii=2.0^-6,
        zero_resolvent_preconditioner=Float64[0 1; -0.25 0],
        second_harmonic_center=zeros(ComplexF64, 2),
        second_harmonic_remainder_radii=2.0^-6,
        second_harmonic_preconditioner=
            realify(inv(second_harmonic_matrix)),
    )
    certificate = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [seed])
    return system, parent, seed, certificate
end

function two_event_certificate()
    system = two_event_normal_form_system()
    parent = certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("7", 64));
        event_axis_breaks=TWO_EVENT_HOPF_AXES,
        event_grid_indices=[
            (1, 1, FIRST_TWO_EVENT_LAMBDA_CELL, 3),
            (1, 1, SECOND_TWO_EVENT_LAMBDA_CELL, 3),
        ],
        event_preconditioners=[
            FIRST_TWO_EVENT_PARENT_PRECONDITIONER,
            SECOND_TWO_EVENT_PARENT_PRECONDITIONER,
        ],
    )
    first_seed = lyapunov_seed(system, parent; event_index=1)
    second_seed = lyapunov_seed(system, parent; event_index=2)
    certificate = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [second_seed, first_seed])
    return system, parent, first_seed, second_seed, certificate
end

function certify_normal_form(;
    radial::Float64,
    first_quadratic::Float64=0.0,
    second_quadratic::Float64=0.0,
    quartic::Float64=0.0,
    kwargs...,
)
    system = normal_form_system(
        radial=radial,
        first_quadratic=first_quadratic,
        second_quadratic=second_quadratic,
        quartic=quartic,
    )
    quadratic = first_quadratic != 0.0 || second_quadratic != 0.0
    parent = parent_census(system; quadratic=quadratic)
    seed = lyapunov_seed(system, parent; quadratic=quadratic)
    certificate = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [seed]; kwargs...)
    return system, parent, seed, certificate
end

@testset "P8s1c1 certifies radial normal-form sign and authority boundary" begin
    system, parent, seed, certificate = certify_normal_form(radial=-1.0)
    @test certificate.version ==
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_VERSION
    @test certificate.formula_version == RO_FIRST_LYAPUNOV_FORMULA_VERSION
    @test certificate.normalization_version ==
        RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION
    @test certificate.parent_census_sha256 == parent.certificate_sha256
    @test certificate.parent_event_count == 1
    @test certificate.nondegenerate_hopf_event_count == 1
    @test certificate.parent_census_replayed
    @test certificate.every_parent_event_lifted_exactly_once
    @test certificate.all_first_lyapunov_coefficients_nonzero_certified
    @test certificate.nondegenerate_local_hopf_event_set_complete_for_parent
    @test !certificate.original_control_sides_certified
    @test !certificate.periodic_orbit_incidence_certified
    @test !certificate.full_state_periodic_orbit_stability_certified
    @test !certificate.stable_root_population_complete
    @test !certificate.global_continuation_certified
    @test !certificate.true_hysteresis_certified
    @test certificate.evidence_scope ==
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_SCOPE

    event = only(certificate.events)
    @test event.parent_event_certificate_sha256 ==
        only(parent.events).certificate_sha256
    @test event.seed_sha256 == seed.seed_sha256
    @test event.center_manifold_criticality == :supercritical
    @test -2 in event.first_lyapunov_coefficient_enclosure
    @test event.first_lyapunov_coefficient_enclosure.upper < 0
    @test event.frequency_enclosure.lower > 0
    @test 1 in event.adjoint_pairing_real_enclosure
    @test 0 in event.adjoint_pairing_imaginary_enclosure
    @test event.q_norm_squared_enclosure.lower > 0
    @test event.parent_complete_census_required_for_authority
    @test event.right_eigenpair_bordered_system_certified
    @test event.hermitian_adjoint_normalization_certified
    @test event.zero_resolvent_certified
    @test event.second_harmonic_resolvent_certified
    @test event.first_lyapunov_coefficient_nonzero_certified
    @test event.nondegenerate_local_hopf_certified
    @test !event.original_control_side_certified
    @test !event.periodic_orbit_incidence_certified
    @test !event.full_state_periodic_orbit_stability_certified
    @test validate_ro_complete_nondegenerate_hopf_census(
        system, parent, certificate)
    @test replay_ro_complete_nondegenerate_hopf_census(
        system, parent, certificate).certificate_sha256 ==
        certificate.certificate_sha256

    _, _, _, subcritical = certify_normal_form(radial=1.0)
    subcritical_event = only(subcritical.events)
    @test subcritical_event.center_manifold_criticality == :subcritical
    @test 2 in subcritical_event.first_lyapunov_coefficient_enclosure
    @test subcritical_event.first_lyapunov_coefficient_enclosure.lower > 0

    anchor_two_seed = lyapunov_seed(system, parent; anchor=2)
    anchor_two = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [anchor_two_seed])
    @test -2 in only(anchor_two.events).
        first_lyapunov_coefficient_enclosure
    @test only(anchor_two.events).center_manifold_criticality ==
        :supercritical
    @test anchor_two.certificate_sha256 != certificate.certificate_sha256

    # A fourth-order term changes neither B nor C at the event. This catches
    # evaluating derivatives at the coordinate origin instead of the enclosed
    # positive equilibrium x=y=lambda=1.
    _, _, _, quartic = certify_normal_form(radial=-1.0, quartic=1.0)
    @test -2 in only(quartic.events).first_lyapunov_coefficient_enclosure
end

@testset "P8s1c1 includes both quadratic resolvent terms" begin
    _, _, _, quadratic = certify_normal_form(
        radial=0.0,
        first_quadratic=1.0,
        second_quadratic=1.0,
    )
    event = only(quadratic.events)
    @test -0.5 in event.first_lyapunov_coefficient_enclosure
    @test event.center_manifold_criticality == :supercritical

    # The quadratic contribution is -1/2. Adding radial c contributes 2c.
    # These cases catch a second 2!/3! division and the sign on -2B(q,h11).
    _, _, _, positive_mixed = certify_normal_form(
        radial=5.0 / 16.0,
        first_quadratic=1.0,
        second_quadratic=1.0,
    )
    @test 0.125 in only(positive_mixed.events).
        first_lyapunov_coefficient_enclosure
    @test only(positive_mixed.events).center_manifold_criticality ==
        :subcritical

    _, _, _, negative_mixed = certify_normal_form(
        radial=3.0 / 16.0,
        first_quadratic=1.0,
        second_quadratic=1.0,
    )
    @test -0.125 in only(negative_mixed.events).
        first_lyapunov_coefficient_enclosure
    @test only(negative_mixed.events).center_manifold_criticality ==
        :supercritical
end

@testset "P8s1c1 handles nonnormal adjoints and omega not equal to one" begin
    system, parent, _, certificate = nonnormal_certificate()
    event = only(certificate.events)
    @test 2 in event.frequency_enclosure
    @test -2 // 5 in event.first_lyapunov_coefficient_enclosure
    @test event.center_manifold_criticality == :supercritical
    @test 1 in event.adjoint_pairing_real_enclosure
    @test 0 in event.adjoint_pairing_imaginary_enclosure
    @test validate_ro_complete_nondegenerate_hopf_census(
        system, parent, certificate)
end

@testset "P8s1c1 canonically lifts every event in a two-event parent" begin
    system, parent, first_seed, second_seed, certificate =
        two_event_certificate()
    @test certificate.parent_event_count == 2
    @test certificate.nondegenerate_hopf_event_count == 2
    @test certificate.seeds[1].seed_sha256 == first_seed.seed_sha256
    @test certificate.seeds[2].seed_sha256 == second_seed.seed_sha256
    @test Tuple(event.parent_event_certificate_sha256
        for event in certificate.events) ==
        Tuple(event.certificate_sha256 for event in parent.events)
    @test all(event -> -2 in event.first_lyapunov_coefficient_enclosure,
        certificate.events)
    @test all(event -> event.center_manifold_criticality == :supercritical,
        certificate.events)
    @test validate_ro_complete_nondegenerate_hopf_census(
        system, parent, certificate)
end

@testset "P8s1c1 rejects Bautin, missing authority, and invalid seeds" begin
    linear_system = normal_form_system()
    linear_parent = parent_census(linear_system)
    linear_seed = lyapunov_seed(linear_system, linear_parent)
    @test_throws ROHopfLyapunovRejected begin
        certify_ro_complete_nondegenerate_hopf_census(
            linear_system, linear_parent, [linear_seed])
    end

    bautin_system = normal_form_system(
        radial=0.25,
        first_quadratic=1.0,
        second_quadratic=1.0,
    )
    bautin_parent = parent_census(bautin_system; quadratic=true)
    bautin_seed = lyapunov_seed(
        bautin_system, bautin_parent; quadratic=true)
    @test_throws ROHopfLyapunovRejected begin
        certify_ro_complete_nondegenerate_hopf_census(
            bautin_system, bautin_parent, [bautin_seed])
    end

    system = normal_form_system(radial=-1.0)
    parent = parent_census(system)
    seed = lyapunov_seed(system, parent)
    @test_throws ArgumentError certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [])
    @test_throws ArgumentError begin
        certify_ro_complete_nondegenerate_hopf_census(
            system, parent, [seed, seed])
    end

    foreign_system = normal_form_system(radial=1.0)
    foreign_parent = parent_census(foreign_system)
    foreign_seed = lyapunov_seed(foreign_system, foreign_parent)
    @test_throws ArgumentError certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [foreign_seed])

    bad_seed = lyapunov_seed(
        system,
        parent;
        preconditioner_override=Matrix{Float64}(I, 6, 6),
    )
    @test_throws ROHopfLyapunovRejected begin
        certify_ro_complete_nondegenerate_hopf_census(
            system, parent, [bad_seed])
    end

    @test_throws ArgumentError ROHopfLyapunovSeed(
        system,
        only(parent.events);
        anchor_state_index=1,
        right_eigenvector_center=ComplexF32[1, -im],
        right_bordered_remainder_radii=2.0^-6,
        right_bordered_preconditioner=Matrix{Float64}(I, 6, 6),
        adjoint_eigenvector_center=ComplexF64[0.5, -0.5im],
        adjoint_bordered_remainder_radii=2.0^-6,
        adjoint_bordered_preconditioner=Matrix{Float64}(I, 6, 6),
        zero_resolvent_center=zeros(Float64, 2),
        zero_resolvent_remainder_radii=2.0^-6,
        zero_resolvent_preconditioner=Matrix{Float64}(I, 2, 2),
        second_harmonic_center=zeros(ComplexF64, 2),
        second_harmonic_remainder_radii=2.0^-6,
        second_harmonic_preconditioner=Matrix{Float64}(I, 4, 4),
    )

    valid = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, [seed])
    valid_event = only(valid.events)
    @test_throws DimensionMismatch BindingAndCatalysis.ROFirstLyapunovHopfEvent(
        BindingAndCatalysis._ROHL_VALIDATED_TOKEN,
        valid_event.version,
        valid_event.formula_version,
        valid_event.normalization_version,
        valid_event.parent_event_certificate_sha256,
        valid_event.seed_sha256,
        valid_event.root_enclosure,
        valid_event.frequency_enclosure,
        (),
        valid_event.adjoint_solution_enclosure,
        valid_event.zero_resolvent_solution_enclosure,
        valid_event.second_harmonic_solution_enclosure,
        valid_event.contraction_betas,
        valid_event.q_norm_squared_enclosure,
        valid_event.adjoint_pairing_real_enclosure,
        valid_event.adjoint_pairing_imaginary_enclosure,
        valid_event.g21_real_enclosure,
        valid_event.g21_imaginary_enclosure,
        valid_event.first_lyapunov_coefficient_enclosure,
        valid_event.center_manifold_criticality,
        true, true, true, true, true, true, true,
        false, false, false,
    )
    beta_error = try
        BindingAndCatalysis.ROFirstLyapunovHopfEvent(
            BindingAndCatalysis._ROHL_VALIDATED_TOKEN,
            valid_event.version,
            valid_event.formula_version,
            valid_event.normalization_version,
            valid_event.parent_event_certificate_sha256,
            valid_event.seed_sha256,
            valid_event.root_enclosure,
            valid_event.frequency_enclosure,
            valid_event.right_solution_enclosure,
            valid_event.adjoint_solution_enclosure,
            valid_event.zero_resolvent_solution_enclosure,
            valid_event.second_harmonic_solution_enclosure,
            (),
            valid_event.q_norm_squared_enclosure,
            valid_event.adjoint_pairing_real_enclosure,
            valid_event.adjoint_pairing_imaginary_enclosure,
            valid_event.g21_real_enclosure,
            valid_event.g21_imaginary_enclosure,
            valid_event.first_lyapunov_coefficient_enclosure,
            valid_event.center_manifold_criticality,
            true, true, true, true, true, true, true,
            false, false, false,
        )
        nothing
    catch err
        err
    end
    @test beta_error isa ArgumentError
    @test occursin("four contraction betas", sprint(showerror, beta_error))
end

@testset "P8s1c1 supports an empty complete parent lift" begin
    system = ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [term(-1.0, [1, 0], [0]), term(1.0, [0, 0], [0])],
            [term(-2.0, [0, 1], [0]), term(2.0, [0, 0], [0])],
        ],
    )
    parent = certify_ro_complete_simple_spectral_hopf_event_census(
        system,
        dynamics_binding(system; policy=repeat("e", 64));
        event_axis_breaks=[
            [0.875, 1.125],
            [0.875, 1.125],
            [0.875, 1.125],
            [0.0, 5.0],
        ],
        event_grid_indices=Tuple[],
        event_preconditioners=Matrix{Float64}[],
    )
    certificate = certify_ro_complete_nondegenerate_hopf_census(
        system, parent, ROHopfLyapunovSeed[])
    @test certificate.parent_event_count == 0
    @test isempty(certificate.events)
    @test isempty(certificate.seeds)
    @test certificate.every_parent_event_lifted_exactly_once
    @test validate_ro_complete_nondegenerate_hopf_census(
        system, parent, certificate)
end

@testset "P8s1c1 enforces cumulative work and cancellation" begin
    system, parent, seed, certificate = certify_normal_form(radial=-1.0)
    @test_throws ROHopfLyapunovLimitExceeded begin
        certify_ro_complete_nondegenerate_hopf_census(
            system,
            parent,
            [seed];
            limits=ROHopfLyapunovLimits(
                max_parent_replay_interval_operations=
                    parent.analysis_interval_operation_count - 1,
            ),
        )
    end
    @test_throws ROHopfLyapunovLimitExceeded begin
        certify_ro_complete_nondegenerate_hopf_census(
            system,
            parent,
            [seed];
            limits=ROHopfLyapunovLimits(
                max_parent_replay_interval_operations=
                    parent.analysis_interval_operation_count,
                max_analysis_interval_operations=
                    certificate.analysis_interval_operation_count - 1,
            ),
        )
    end

    for limits in (
        ROHopfLyapunovLimits(max_events=0),
        ROHopfLyapunovLimits(max_realified_linear_dimension=5),
        ROHopfLyapunovLimits(max_derivative_tensor_entries=27),
        ROHopfLyapunovLimits(max_preconditioner_entries=91),
        ROHopfLyapunovLimits(max_sqrt_bisection_steps=127),
    )
        preflight_checks = Ref(0)
        @test_throws ROHopfLyapunovLimitExceeded begin
            certify_ro_complete_nondegenerate_hopf_census(
                system,
                parent,
                [seed];
                limits=limits,
                cancel_check=() -> (preflight_checks[] += 1),
            )
        end
        @test preflight_checks[] == 0
    end

    @test_throws CancelProbe begin
        certify_ro_complete_nondegenerate_hopf_census(
            system, parent, [seed]; cancel_check=() -> throw(CancelProbe()))
    end

    parent_checks = Ref(0)
    replay_ro_complete_simple_spectral_hopf_event_census(
        system,
        parent;
        cancel_check=() -> (parent_checks[] += 1),
    )
    child_checks = Ref(0)
    child_cancel_threshold = 1 + parent_checks[] + 5
    @test_throws CancelProbe begin
        certify_ro_complete_nondegenerate_hopf_census(
            system,
            parent,
            [seed];
            cancel_check=() -> begin
                child_checks[] += 1
                child_checks[] == child_cancel_threshold &&
                    throw(CancelProbe())
            end,
        )
    end
    @test child_checks[] == child_cancel_threshold
end

@testset "P8s1c1 exact frequency enclosure handles Float64 scale extremes" begin
    context = BindingAndCatalysis._RORSContext(
        RORegularSheetLimits(max_interval_operations=100_000),
        () -> nothing,
    )
    limits = ROHopfLyapunovLimits()
    for value in (nextfloat(0.0), floatmax(Float64))
        squared_frequency = ROExactInterval(value, value)
        frequency = BindingAndCatalysis._rohl_frequency_enclosure(
            squared_frequency, limits, context)
        @test frequency.lower > 0
        @test frequency.lower^2 <= squared_frequency.lower
        @test squared_frequency.upper <= frequency.upper^2
    end
end

end # module
