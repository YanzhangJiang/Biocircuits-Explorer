module ROPeriodicFourierIdentityContract

using Test
using BindingAndCatalysis

const Q = Rational{BigInt}

struct FourierCancelProbe <: Exception end

struct NoReadFourierVector{T} <: AbstractVector{T}
    declared_length::Int
    touched::Base.RefValue{Bool}
end

Base.size(vector::NoReadFourierVector) = (vector.declared_length,)
Base.length(vector::NoReadFourierVector) = vector.declared_length
function Base.getindex(vector::NoReadFourierVector, index::Int)
    vector.touched[] = true
    throw(ErrorException("oversized vector must be rejected before reading"))
end

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

q(n::Integer, d::Integer=1) = BigInt(n) // BigInt(d)

function centered_three_state_equation(terms)
    # Expand monomials in X=x-1, Y=y-1, Z=z-1, mu=lambda-1 into
    # the canonical polynomial declaration owned by P5r0.  All fixtures use
    # integer coefficients, so this expansion is exact before Float64
    # admission.
    polynomial = Dict{NTuple{4,Int},Float64}()
    for (coefficient, exponents) in terms
        coefficient isa Float64 || throw(ArgumentError(
            "centered fixture coefficients must be Float64"))
        exponents isa NTuple{4,Int} || throw(ArgumentError(
            "centered fixture exponents must have four entries"))
        for x_power in 0:exponents[1],
                y_power in 0:exponents[2],
                z_power in 0:exponents[3],
                control_power in 0:exponents[4]
            raw_exponents = (x_power, y_power, z_power, control_power)
            selected_degree = sum(raw_exponents)
            expanded = coefficient *
                binomial(exponents[1], x_power) *
                binomial(exponents[2], y_power) *
                binomial(exponents[3], z_power) *
                binomial(exponents[4], control_power) *
                (-1.0)^(sum(exponents) - selected_degree)
            polynomial[raw_exponents] =
                get(polynomial, raw_exponents, 0.0) + expanded
            polynomial[raw_exponents] == 0.0 &&
                delete!(polynomial, raw_exponents)
        end
    end
    return [
        term(
            polynomial[key],
            [key[1], key[2], key[3]],
            [key[4]],
        )
        for key in sort!(collect(keys(polynomial)); by=key ->
            ((key[1], key[2], key[3]), (key[4],)))
    ]
end

function forced_second_harmonic_hopf_system(;
    limits=RORegularSheetLimits(),
    state_units=fill("concentration", 3),
)
    # X=x-1, Y=y-1, Z=z-1, mu=lambda-1:
    #
    #   X' = mu X - Y - X(X^2+Y^2)
    #   Y' = X + mu Y - Y(X^2+Y^2)
    #   Z' = -2Z + X^2.
    #
    # At a=1/16, lambda=1+a^2 and omega=1 this system has the exact
    # nonconstant finite-support orbit used below.  This is an identity
    # witness only; no Hopf parent or branch certificate is fabricated here.
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "z"],
        state_units=state_units,
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            centered_three_state_equation([
                (1.0, (1, 0, 0, 1)),
                (-1.0, (0, 1, 0, 0)),
                (-1.0, (3, 0, 0, 0)),
                (-1.0, (1, 2, 0, 0)),
            ]),
            centered_three_state_equation([
                (1.0, (1, 0, 0, 0)),
                (1.0, (0, 1, 0, 1)),
                (-1.0, (2, 1, 0, 0)),
                (-1.0, (0, 3, 0, 0)),
            ]),
            centered_three_state_equation([
                (-2.0, (0, 0, 1, 0)),
                (1.0, (2, 0, 0, 0)),
            ]),
        ],
        limits=limits,
    )
end

function unstable_transverse_forced_hopf_system()
    base = forced_second_harmonic_hopf_system()
    equations = [
        [
            term(item.coefficient, (item.state_exponents..., 0),
                item.control_exponents)
            for item in equation
        ]
        for equation in base.equations
    ]
    # W=w-1 and W'=2W.  The exact orbit remains in W=0, but it has an
    # unstable transverse direction.  A residual identity must never upgrade
    # this fact into a full-state stability claim.
    push!(equations, [
        term(-2.0, [0, 0, 0, 0], [0]),
        term(2.0, [0, 0, 0, 1], [0]),
    ])
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y", "z", "w"],
        state_units=fill("concentration", 4),
        control_names=["lambda"],
        control_units=["concentration"],
        equations=equations,
    )
end

function polynomial_oracle_system(power::Int)
    power > 0 || throw(ArgumentError("power must be positive"))
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["state"],
        control_names=["u"],
        control_units=["control"],
        equations=[[
            term(1.0, [power], [0]),
        ]],
    )
end

function product_oracle_system()
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["state", "state"],
        control_names=["u"],
        control_units=["control"],
        equations=[
            [term(1.0, [1, 1], [0])],
            [term(1.0, [1, 0], [0])],
        ],
    )
end

function neutral_constant_system()
    # f(x,u)=u admits a zero vector field at the declared control u=0 while
    # retaining one canonical nonzero source monomial.
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["state"],
        control_names=["u"],
        control_units=["control"],
        equations=[[
            term(1.0, [0], [1]),
        ]],
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

fourier(real_parts, imag_parts;
    limits=ROPeriodicFourierLimits()) =
    ROExactRealFourierSeries(real_parts, imag_parts; limits=limits)

function exact_hopf_series(; include_z_second_harmonic::Bool=true)
    a = q(1, 16)
    x = fourier([q(1), a / 2], [q(0), q(0)])
    y = fourier([q(1), q(0)], [q(0), -a / 2])
    if include_z_second_harmonic
        z = fourier(
            [q(1) + a^2 / 4, q(0), a^2 / 16],
            [q(0), q(0), -a^2 / 16],
        )
    else
        z = fourier([q(1) + a^2 / 4], [q(0)])
    end
    return [x, y, z]
end

function full_mode(series, mode::Integer)
    mode >= 0 || throw(ArgumentError(
        "test helper reads the stored nonnegative half-spectrum"))
    mode <= series.half_bandwidth || return (q(0), q(0))
    return (series.real_parts[mode + 1], series.imag_parts[mode + 1])
end

function audit_forced_hopf(;
    system=forced_second_harmonic_hopf_system(),
    dynamics=dynamics_binding(system),
    state_series=exact_hopf_series(),
    controls=[q(257, 256)],
    omega=q(1),
    galerkin_half_bandwidth=2,
    weight_nu=q(2),
    limits=ROPeriodicFourierLimits(),
    cancel_check=() -> nothing,
)
    return audit_ro_exact_polynomial_periodic_fourier_residual(
        system,
        dynamics,
        state_series,
        controls,
        omega;
        galerkin_half_bandwidth=galerkin_half_bandwidth,
        weight_nu=weight_nu,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function audit_oracle(system, state_series;
    controls=[q(0)],
    omega=q(1),
    galerkin_half_bandwidth=maximum(
        series.half_bandwidth for series in state_series),
    weight_nu=q(2),
    limits=ROPeriodicFourierLimits(),
    cancel_check=() -> nothing,
)
    dynamics = dynamics_binding(system)
    return audit_ro_exact_polynomial_periodic_fourier_residual(
        system,
        dynamics,
        state_series,
        controls,
        omega;
        galerkin_half_bandwidth=galerkin_half_bandwidth,
        weight_nu=weight_nu,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function fourier_limits_with(
    base::ROPeriodicFourierLimits=ROPeriodicFourierLimits();
    replacements...,
)
    names = fieldnames(ROPeriodicFourierLimits)
    values = NamedTuple{names}(Tuple(getfield(base, name) for name in names))
    return ROPeriodicFourierLimits(; merge(values, (; replacements...))...)
end

function tight_hopf_limits(; replacements...)
    base = ROPeriodicFourierLimits(
        max_states=3,
        max_controls=1,
        max_input_half_bandwidth=2,
        max_output_half_bandwidth=3,
        max_series_entries=11,
        max_source_terms=24,
        max_total_degree=3,
        max_convolution_pairs=218,
        max_workspace_entries=7,
        max_canonical_payload_bytes=1_639,
        max_exact_operand_bits=1_076,
        max_exact_operations=2_274,
    )
    return fourier_limits_with(base; replacements...)
end


const AUDIT_FLAG_FIELDS = (
    :real_fourier_symmetry_by_construction,
    :positive_angular_frequency,
    :nonconstant_parameterization,
    :galerkin_head_residual_exactly_zero,
    :omitted_residual_tail_exactly_zero,
    :full_residual_exactly_zero,
    :single_exact_ode_periodic_parameterization_certified,
    :explicit_periodic_orbit_enclosure_certified,
    :state_positivity_certified,
    :minimal_period_certified,
    :infinite_fourier_tail_radii_certified,
    :local_uniqueness_inside_declared_fourier_tube,
    :validated_periodic_orbit_branch_certified,
    :quantitative_amplitude_coverage_certified,
    :constructive_hopf_event_incidence_certified,
    :floquet_spectrum_certified,
    :full_state_periodic_orbit_stability_certified,
    :periodic_orbit_population_complete,
    :stable_periodic_orbit_population_complete,
    :multi_control_hopf_sheet_certified,
    :native_residuals_certified,
    :global_continuation_certified,
    :true_hysteresis_certified,
)

function raw_audit_with_fields(
    audit::ROExactPolynomialPeriodicFourierAudit,
    replacements::Pair...;
    rehash::Bool=false,
)
    names = fieldnames(typeof(audit))
    raw = Any[getfield(audit, name) for name in names]
    for (field, value) in replacements
        index = findfirst(==(field), names)
        index === nothing && throw(ArgumentError("unknown audit field $field"))
        raw[index] = value
    end
    if rehash
        flag_values = Tuple(
            raw[findfirst(==(name), names)] for name in AUDIT_FLAG_FIELDS)
        raw[findfirst(==(:certificate_sha256), names)] =
            BindingAndCatalysis._ropf_audit_sha256(
                raw[findfirst(==(:system_declaration_sha256), names)],
                raw[findfirst(==(:dynamics_binding_declaration_sha256), names)],
                raw[findfirst(==(:state_series), names)],
                raw[findfirst(==(:controls), names)],
                raw[findfirst(==(:omega), names)],
                raw[findfirst(==(:galerkin_half_bandwidth), names)],
                raw[findfirst(==(:weight_nu), names)],
                raw[findfirst(==(:residual_series), names)],
                raw[findfirst(==(:galerkin_head_residual_norms), names)],
                raw[findfirst(==(:omitted_tail_residual_norms), names)],
                raw[findfirst(==(:weighted_l1_residual_norms), names)],
                raw[findfirst(==(:first_omitted_nonzero_mode), names)],
                raw[findfirst(==(:planned_convolution_pair_count), names)],
                raw[findfirst(==(:convolution_pair_count), names)],
                raw[findfirst(==(:analysis_exact_operation_count), names)],
                raw[findfirst(==(:limits), names)],
                flag_values,
            )
    end
    flag_values = Tuple(
        raw[findfirst(==(name), names)] for name in AUDIT_FLAG_FIELDS)
    limits_index = findfirst(==(:limits), names)
    certificate_index = findfirst(==(:certificate_sha256), names)
    return BindingAndCatalysis.ROExactPolynomialPeriodicFourierAudit(
        BindingAndCatalysis._ROPF_AUDIT_VALIDATED_TOKEN,
        raw[1:limits_index]...,
        flag_values,
        raw[certificate_index],
    )
end

raw_audit_with_field(
    audit::ROExactPolynomialPeriodicFourierAudit,
    field::Symbol,
    value;
    rehash::Bool=false,
) = raw_audit_with_fields(audit, field => value; rehash=rehash)

@testset "pre-c2b exact Fourier identity foundation" begin
    @testset "full complex half-spectrum admission is exact and canonical" begin
        cosine = fourier([q(0), q(1, 2)], [q(0), q(0)])
        sine = fourier([q(0), q(0)], [q(0), q(-1, 2)])
        @test cosine.half_bandwidth == 1
        @test full_mode(cosine, 1) == (q(1, 2), q(0))
        @test full_mode(sine, 1) == (q(0), q(-1, 2))
        @test validate_ro_exact_real_fourier_series(cosine)
        @test validate_ro_exact_real_fourier_series(sine)

        padded_real = Any[1.0, 0.5, 0.0, -0.0]
        padded_imag = Any[0.0, 0.0, 0.0, 0.0]
        canonical = fourier(padded_real, padded_imag)
        unpadded = fourier([1.0, 0.5], [0.0, 0.0])
        @test canonical == unpadded
        @test canonical.half_bandwidth == 1
        @test canonical.real_parts == (q(1), q(1, 2))
        @test canonical.imag_parts == (q(0), q(0))
        padded_real[1] = 99.0
        padded_imag[2] = 99.0
        @test canonical == unpadded

        @test_throws ArgumentError fourier([q(1)], [q(1)])
        @test_throws ArgumentError fourier([true], [false])
        @test_throws ArgumentError fourier(Float32[1, 0.5], Float32[0, 0])
        @test_throws ArgumentError fourier([NaN], [0.0])
        @test_throws ArgumentError fourier([Inf], [0.0])
        @test_throws DimensionMismatch fourier([q(1)], [q(0), q(0)])
    end

    @testset "independent cos^2, cos*sin, derivative, and cos^3 oracles" begin
        cosine = fourier([q(0), q(1, 2)], [q(0), q(0)])
        sine = fourier([q(0), q(0)], [q(0), q(-1, 2)])

        square = audit_oracle(
            polynomial_oracle_system(2), [cosine];
            galerkin_half_bandwidth=1,
        )
        # R=omega*x_theta-x^2.  Independently,
        # cos^2=(1+cos(2theta))/2 and d cos/dtheta=-sin.
        @test full_mode(square.residual_series[1], 0) == (q(-1, 2), q(0))
        @test full_mode(square.residual_series[1], 1) == (q(0), q(1, 2))
        @test full_mode(square.residual_series[1], 2) == (q(-1, 4), q(0))
        @test square.first_omitted_nonzero_mode == 2
        @test square.galerkin_head_residual_exactly_zero == false
        @test !square.full_residual_exactly_zero

        product = audit_oracle(
            product_oracle_system(), [cosine, sine];
            galerkin_half_bandwidth=1,
        )
        # cos(theta)sin(theta)=sin(2theta)/2, so subtracting the
        # product contributes +i/4 in the stored k=2 coefficient.
        @test full_mode(product.residual_series[1], 0) == (q(0), q(0))
        @test full_mode(product.residual_series[1], 1) == (q(0), q(1, 2))
        @test full_mode(product.residual_series[1], 2) == (q(0), q(1, 4))
        @test all(full_mode(product.residual_series[2], k) == (q(0), q(0))
            for k in 0:product.residual_series[2].half_bandwidth)

        split_bandwidth_limits = fourier_limits_with(
            max_input_half_bandwidth=1,
            max_output_half_bandwidth=3,
            max_workspace_entries=7,
        )
        cube = audit_oracle(
            polynomial_oracle_system(3), [cosine];
            galerkin_half_bandwidth=1,
            limits=split_bandwidth_limits,
        )
        # cos^3=(3cos(theta)+cos(3theta))/4.
        @test full_mode(cube.residual_series[1], 1) ==
            (q(-3, 8), q(1, 2))
        @test full_mode(cube.residual_series[1], 3) == (q(-1, 8), q(0))
        @test cube.first_omitted_nonzero_mode == 3
        @test !cube.full_residual_exactly_zero

        forbidden_input = fourier(
            [q(0), q(0), q(1)],
            [q(0), q(0), q(0)],
        )
        @test_throws ROPeriodicFourierLimitExceeded audit_oracle(
            polynomial_oracle_system(1), [forbidden_input];
            galerkin_half_bandwidth=2,
            limits=split_bandwidth_limits,
        )
    end

    @testset "full generated residual proves one exact finite-support orbit" begin
        system = forced_second_harmonic_hopf_system()
        dynamics = dynamics_binding(system)
        audit = audit_forced_hopf(system=system, dynamics=dynamics)

        @test validate_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, audit)
        @test replay_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, audit) == audit
        @test audit.system_declaration_sha256 == system.declaration_sha256
        @test audit.dynamics_binding_declaration_sha256 ==
            dynamics.declaration_sha256
        @test audit.controls == (q(257, 256),)
        @test audit.omega == q(1)
        @test audit.galerkin_half_bandwidth == 2
        @test audit.weight_nu == q(2)
        @test audit.full_residual_exactly_zero
        @test audit.galerkin_head_residual_exactly_zero
        @test audit.first_omitted_nonzero_mode === nothing
        @test all(==(q(0)), audit.weighted_l1_residual_norms)
        @test audit.nonconstant_parameterization
        @test audit.single_exact_ode_periodic_parameterization_certified
        @test occursin(r"^[0-9a-f]{64}$", audit.certificate_sha256)

        for residual in audit.residual_series
            @test all(full_mode(residual, k) == (q(0), q(0))
                for k in 0:residual.half_bandwidth)
        end

        # This layer is intentionally below c2b.  It proves one source-bound
        # polynomial identity, not a validated branch, infinite-tail/radii
        # theorem, event incidence, Floquet result, population theorem, or
        # global dynamical statement.
        for field in (
            :validated_periodic_orbit_branch_certified,
            :constructive_hopf_event_incidence_certified,
            :infinite_fourier_tail_radii_certified,
            :floquet_spectrum_certified,
            :full_state_periodic_orbit_stability_certified,
            :periodic_orbit_population_complete,
            :stable_periodic_orbit_population_complete,
            :global_continuation_certified,
            :true_hysteresis_certified,
            :minimal_period_certified,
        )
            @test !getfield(audit, field)
        end
    end

    @testset "Galerkin head can vanish while the first omitted mode is nonzero" begin
        trap = audit_forced_hopf(
            state_series=exact_hopf_series(
                include_z_second_harmonic=false),
            galerkin_half_bandwidth=1,
        )
        @test trap.galerkin_head_residual_exactly_zero
        @test !trap.full_residual_exactly_zero
        @test trap.first_omitted_nonzero_mode == 2
        @test full_mode(trap.residual_series[3], 2) ==
            (q(-1, 1024), q(0))
        @test trap.galerkin_head_residual_norms ==
            (q(0), q(0), q(0))
        @test trap.omitted_tail_residual_norms ==
            (q(0), q(0), q(1, 128))
        @test trap.weighted_l1_residual_norms ==
            (q(0), q(0), q(1, 128))
        @test !trap.omitted_residual_tail_exactly_zero
        @test !trap.single_exact_ode_periodic_parameterization_certified
        @test trap.nonconstant_parameterization
    end

    @testset "three-factor convolution retains intermediate support" begin
        # x=e^(2i theta)+e^(-2i theta).  The +2 coefficient of x^3 is 3:
        # the (+2,+2,-2) contribution and its three permutations must survive.
        # Truncating x*x back to |k|<=2 before the third multiplication would
        # incorrectly produce 2 instead of 3.
        high_mode = fourier(
            [q(0), q(0), q(1)],
            [q(0), q(0), q(0)],
        )
        audit = audit_oracle(
            polynomial_oracle_system(3), [high_mode];
            galerkin_half_bandwidth=2,
        )
        @test full_mode(audit.residual_series[1], 2) == (q(-3), q(2))
        @test full_mode(audit.residual_series[1], 6) == (q(-1), q(0))
        @test audit.first_omitted_nonzero_mode == 6
    end

    @testset "constant equilibria and transverse instability stay scoped" begin
        system = forced_second_harmonic_hopf_system()
        constant = [
            fourier([q(1)], [q(0)]),
            fourier([q(1)], [q(0)]),
            fourier([q(1)], [q(0)]),
        ]
        equilibrium = audit_forced_hopf(
            system=system,
            dynamics=dynamics_binding(system),
            state_series=constant,
            controls=[q(1)],
            galerkin_half_bandwidth=0,
        )
        @test equilibrium.full_residual_exactly_zero
        @test !equilibrium.nonconstant_parameterization
        @test !equilibrium.single_exact_ode_periodic_parameterization_certified

        transverse_system = unstable_transverse_forced_hopf_system()
        transverse_series = [
            exact_hopf_series()...,
            fourier([q(1)], [q(0)]),
        ]
        transverse = audit_forced_hopf(
            system=transverse_system,
            dynamics=dynamics_binding(transverse_system),
            state_series=transverse_series,
        )
        @test transverse.full_residual_exactly_zero
        @test transverse.nonconstant_parameterization
        @test transverse.single_exact_ode_periodic_parameterization_certified
        @test !transverse.floquet_spectrum_certified
        @test !transverse.full_state_periodic_orbit_stability_certified

        @test_throws ArgumentError audit_forced_hopf(omega=q(0))
        @test_throws ArgumentError audit_forced_hopf(omega=q(-1))
        @test_throws ArgumentError audit_forced_hopf(weight_nu=q(1))
        @test_throws ArgumentError audit_forced_hopf(weight_nu=q(0))
    end

    @testset "numeric admission, snapshots, and source-bound replay" begin
        # Every admitted Float64 is interpreted as its exact binary rational.
        # These values are dyadic, so the stored controls/frequency agree with
        # their short rational forms without rounding.
        float_audit = audit_forced_hopf(
            controls=[257.0 / 256.0],
            omega=1.0,
            weight_nu=2.0,
        )
        @test float_audit.controls == (q(257, 256),)
        @test float_audit.omega == q(1)
        @test float_audit.weight_nu == q(2)

        @test_throws ArgumentError audit_forced_hopf(controls=[true])
        @test_throws ArgumentError audit_forced_hopf(controls=Float32[1])
        @test_throws ArgumentError audit_forced_hopf(controls=[NaN])
        @test_throws ArgumentError audit_forced_hopf(controls=[Inf])
        @test_throws ArgumentError audit_forced_hopf(omega=true)
        @test_throws ArgumentError audit_forced_hopf(omega=Float32(1))
        @test_throws ArgumentError audit_forced_hopf(omega=NaN)
        @test_throws ArgumentError audit_forced_hopf(omega=Inf)
        @test_throws ArgumentError audit_forced_hopf(weight_nu=Float32(2))
        @test_throws ArgumentError audit_forced_hopf(weight_nu=NaN)
        @test_throws ArgumentError audit_forced_hopf(weight_nu=Inf)

        state_series = exact_hopf_series()
        controls = Any[q(257, 256)]
        audit = audit_forced_hopf(
            state_series=state_series,
            controls=controls,
        )
        baseline = audit_forced_hopf()
        state_series[1] = fourier([q(99)], [q(0)])
        controls[1] = q(99)
        @test audit == baseline

        # Rational{BigInt} values can only be altered through low-level Julia
        # internals, but a consumer still must not treat the object or its
        # self-hash as authority.  Both nested-series and source-bound audit
        # validation fail closed after such a mutation.
        mutated = audit_forced_hopf()
        mutated_series = getfield(mutated, :state_series)[1]
        mutated_value = getfield(mutated_series, :real_parts)[1]
        Base.GMP.MPZ.set!(numerator(mutated_value), BigInt(99))
        @test_throws ArgumentError validate_ro_exact_real_fourier_series(
            mutated_series)
        @test_throws ArgumentError validate_ro_exact_polynomial_periodic_fourier_residual(
            forced_second_harmonic_hopf_system(),
            dynamics_binding(forced_second_harmonic_hopf_system()),
            mutated,
        )

        system = forced_second_harmonic_hopf_system()
        dynamics = dynamics_binding(system)
        foreign_system = forced_second_harmonic_hopf_system(
            state_units=["foreign", "concentration", "concentration"],
        )
        foreign_dynamics = dynamics_binding(system; policy=repeat("9", 64))
        @test_throws ArgumentError validate_ro_exact_polynomial_periodic_fourier_residual(
            foreign_system, dynamics_binding(foreign_system), audit)
        @test_throws ArgumentError replay_ro_exact_polynomial_periodic_fourier_residual(
            foreign_system, dynamics_binding(foreign_system), audit)
        @test_throws ArgumentError validate_ro_exact_polynomial_periodic_fourier_residual(
            system, foreign_dynamics, audit)
        @test_throws ArgumentError replay_ro_exact_polynomial_periodic_fourier_residual(
            system, foreign_dynamics, audit)
    end

    @testset "raw tampering and self-consistent hashes have no authority" begin
        system = forced_second_harmonic_hopf_system()
        dynamics = dynamics_binding(system)
        audit = audit_forced_hopf(system=system, dynamics=dynamics)
        raw = Any[getfield(audit, name) for name in fieldnames(typeof(audit))]
        @test_throws MethodError ROExactPolynomialPeriodicFourierAudit(raw...)

        fake_source = raw_audit_with_field(
            audit, :system_declaration_sha256, repeat("f", 64); rehash=true)
        fake_dynamics = raw_audit_with_field(
            audit, :dynamics_binding_declaration_sha256,
            repeat("e", 64); rehash=true)
        @test_throws ArgumentError replay_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, fake_source)
        @test_throws ArgumentError replay_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, fake_dynamics)

        trap = audit_forced_hopf(
            state_series=exact_hopf_series(
                include_z_second_harmonic=false),
            galerkin_half_bandwidth=1,
        )
        # A plain norm-policy tamper is caught by the content hash.  Rehashing
        # a different nu would instead be a legitimate new request when the
        # exact residual is zero, so it is not mislabeled as a forgery here.
        @test_throws ArgumentError raw_audit_with_field(
            audit, :weight_nu, q(3))
        valid_nu_variant = raw_audit_with_field(
            audit, :weight_nu, q(3); rehash=true)
        @test validate_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, valid_nu_variant)
        valid_cutoff_variant = raw_audit_with_field(
            audit, :galerkin_half_bandwidth, 3; rehash=true)
        @test validate_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, valid_cutoff_variant)
        for (field, value) in (
            (:controls, (q(1),)),
            (:omega, q(2)),
            (:state_series, trap.state_series),
        )
            forged = raw_audit_with_field(
                audit, field, value; rehash=true)
            @test_throws Exception validate_ro_exact_polynomial_periodic_fourier_residual(
                system, dynamics, forged)
        end

        # Structural reconstruction rejects inconsistent nested limits,
        # candidate cutoffs, or residual/norm pairs before any caller can
        # obtain a self-hashed object with contradictory derived claims.
        @test_throws Exception raw_audit_with_field(
            audit, :galerkin_half_bandwidth, 1; rehash=true)
        @test_throws Exception raw_audit_with_field(
            audit, :limits,
            fourier_limits_with(audit.limits; max_states=15);
            rehash=true,
        )
        @test_throws Exception raw_audit_with_field(
            audit, :residual_series, trap.residual_series; rehash=true)

        # Even a structurally self-consistent fabricated residual receipt is
        # replaced by source replay and rejected by the validator.  Here the
        # forged residual's k=2 defect is placed inside a cutoff-2 head, and
        # every dependent norm/flag/hash is changed consistently.
        residual_forgery = raw_audit_with_fields(
            audit,
            :residual_series => trap.residual_series,
            :galerkin_head_residual_norms => (q(0), q(0), q(1, 128)),
            :omitted_tail_residual_norms => (q(0), q(0), q(0)),
            :weighted_l1_residual_norms => (q(0), q(0), q(1, 128)),
            :first_omitted_nonzero_mode => nothing,
            :galerkin_head_residual_exactly_zero => false,
            :omitted_residual_tail_exactly_zero => true,
            :full_residual_exactly_zero => false,
            :single_exact_ode_periodic_parameterization_certified => false;
            rehash=true,
        )
        @test replay_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, residual_forgery) == audit
        @test_throws ArgumentError validate_ro_exact_polynomial_periodic_fourier_residual(
            system, dynamics, residual_forgery)

        for field in AUDIT_FLAG_FIELDS[8:end]
            @test_throws ArgumentError raw_audit_with_field(
                audit, field, true; rehash=true)
        end
    end

    @testset "static resource receipts, exact thresholds, and cancellation" begin
        baseline = audit_forced_hopf()
        @test baseline.planned_convolution_pair_count == 218
        @test baseline.convolution_pair_count == 218
        @test baseline.analysis_exact_operation_count == 2_274

        tight = tight_hopf_limits()
        tight_audit = audit_forced_hopf(limits=tight)
        @test tight_audit.planned_convolution_pair_count == 218
        @test tight_audit.analysis_exact_operation_count == 2_274
        @test validate_ro_exact_polynomial_periodic_fourier_residual(
            forced_second_harmonic_hopf_system(),
            dynamics_binding(forced_second_harmonic_hopf_system()),
            tight_audit,
        )

        for limits in (
            tight_hopf_limits(max_states=2),
            tight_hopf_limits(max_controls=1, max_series_entries=10),
            tight_hopf_limits(max_source_terms=23),
            tight_hopf_limits(max_total_degree=2),
            tight_hopf_limits(max_convolution_pairs=217),
            tight_hopf_limits(max_exact_operations=2_273),
            tight_hopf_limits(max_canonical_payload_bytes=1_638),
        )
            @test_throws ROPeriodicFourierLimitExceeded audit_forced_hopf(
                limits=limits)
        end
        @test_throws ROPeriodicFourierLimitExceeded audit_forced_hopf(
            limits=tight_hopf_limits(max_input_half_bandwidth=1))
        @test_throws ArgumentError tight_hopf_limits(
            max_workspace_entries=6)
        @test_throws ArgumentError ROPeriodicFourierLimits(
            max_exact_operand_bits=1_075)
        @test_throws ArgumentError ROPeriodicFourierLimits(
            max_output_half_bandwidth=typemax(Int),
            max_workspace_entries=typemax(Int),
        )

        oversized_touched = Ref(false)
        oversized = NoReadFourierVector{ROExactRealFourierSeries}(
            typemax(Int), oversized_touched)
        @test_throws DimensionMismatch audit_forced_hopf(
            state_series=oversized)
        @test !oversized_touched[]

        source_calls = Ref(0)
        source_cancel = () -> (source_calls[] += 1)
        @test_throws ROPeriodicFourierLimitExceeded audit_forced_hopf(
            limits=tight_hopf_limits(max_source_terms=23),
            cancel_check=source_cancel,
        )
        @test source_calls[] == 0

        output_calls = Ref(0)
        output_cancel = () -> (output_calls[] += 1)
        @test_throws ROPeriodicFourierLimitExceeded audit_forced_hopf(
            limits=tight_hopf_limits(
                max_output_half_bandwidth=2,
                max_workspace_entries=5,
            ),
            cancel_check=output_cancel,
        )
        @test output_calls[] == 0
        @test_throws ROPeriodicFourierRejected audit_forced_hopf(
            galerkin_half_bandwidth=1)

        huge = (BigInt(1) << 1_076) // BigInt(1)
        @test_throws ROPeriodicFourierLimitExceeded fourier(
            [huge], [q(0)];
            limits=fourier_limits_with(
                max_exact_operand_bits=1_076),
        )
        huge_series = fourier(
            [huge], [q(0)];
            limits=fourier_limits_with(
                max_exact_operand_bits=1_077),
        )
        @test huge_series.real_parts == (huge,)

        static_calls = Ref(0)
        static_cancel = () -> (static_calls[] += 1)
        static_error = try
            audit_forced_hopf(
                galerkin_half_bandwidth=typemax(Int),
                cancel_check=static_cancel,
            )
            nothing
        catch caught
            caught
        end
        @test static_error isa ROPeriodicFourierLimitExceeded
        @test static_error.phase == :galerkin_half_bandwidth
        @test static_calls[] == 0

        saw_convolution = Ref(false)
        deep_cancel = () -> begin
            if any(frame -> occursin("_ropf_convolve", string(frame.func)),
                    stacktrace())
                saw_convolution[] = true
                throw(FourierCancelProbe())
            end
            return nothing
        end
        @test_throws FourierCancelProbe audit_forced_hopf(
            cancel_check=deep_cancel)
        @test saw_convolution[]

        baseline_cancel_calls = Ref(0)
        counted = audit_forced_hopf(
            cancel_check=() -> (baseline_cancel_calls[] += 1))
        @test counted == baseline
        @test baseline_cancel_calls[] > 0
        final_cancel_calls = Ref(0)
        final_cancel = () -> begin
            final_cancel_calls[] += 1
            final_cancel_calls[] == baseline_cancel_calls[] &&
                throw(FourierCancelProbe())
            return nothing
        end
        @test_throws FourierCancelProbe audit_forced_hopf(
            cancel_check=final_cancel)
        @test final_cancel_calls[] == baseline_cancel_calls[]

        retry = audit_forced_hopf()
        @test retry == baseline
        @test retry.certificate_sha256 == baseline.certificate_sha256
    end

end

end # module
