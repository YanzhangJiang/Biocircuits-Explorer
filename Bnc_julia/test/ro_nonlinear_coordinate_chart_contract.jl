module RONonlinearCoordinateChartContract

using Test
using LinearAlgebra
using BindingAndCatalysis

struct _RONCHugeSourceArray <: AbstractMatrix{Float64} end
Base.size(::_RONCHugeSourceArray) = (typemax(Int), 2)
Base.getindex(::_RONCHugeSourceArray, ::Int, ::Int) = 0.0

function one_dimensional_chart(;
    source_reference=[0.0],
    jacobian=[1.0;;],
    hessian=reshape([0.0], 1, 1, 1),
    domain=(-1.0, 1.0),
    rank_atol=0.0,
    rank_rtol=1e-12,
    max_condition_number=1e10,
    limits=RONonlinearChartLimits(),
    cancel_check=() -> nothing,
    source_name="theta",
    source_unit="log10 concentration",
    control_name="u",
    control_unit="log10 dose",
)
    return RONonlinearInputChart(
        source_coordinate_names=[source_name],
        source_coordinate_units=[source_unit],
        control_coordinate_names=[control_name],
        control_coordinate_units=[control_unit],
        control_reference=[0.0],
        domain_lower=[domain[1]],
        domain_upper=[domain[2]],
        source_reference=source_reference,
        source_jacobian_at_reference=jacobian,
        source_hessians=hessian,
        rank_atol=rank_atol,
        rank_rtol=rank_rtol,
        max_condition_number=max_condition_number,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function bound_source_derivatives(
    chart::RONonlinearInputChart,
    gradient,
    hessian;
    control_coordinates=chart.control_reference,
    source_coordinates=map_ro_nonlinear_source_coordinates(
        chart, control_coordinates),
    chart_declaration_sha256=chart.declaration_sha256,
    output_name="z",
    output_unit="dimensionless",
    source_coordinate_names=chart.source_coordinate_names,
    source_coordinate_units=chart.source_coordinate_units,
    limits=RONonlinearChartLimits(),
)
    return RONonlinearSourceDerivatives(
        chart_declaration_sha256=chart_declaration_sha256,
        output_name=output_name,
        output_unit=output_unit,
        source_coordinate_names=source_coordinate_names,
        source_coordinate_units=source_coordinate_units,
        source_coordinates=source_coordinates,
        gradient=gradient,
        hessian=hessian,
        limits=limits,
    )
end

function source_axis_identity(chart::RONonlinearInputChart, controls)
    return (
        source_coordinate_names=chart.source_coordinate_names,
        source_coordinate_units=chart.source_coordinate_units,
        source_coordinates=map_ro_nonlinear_source_coordinates(
            chart, controls),
    )
end

function raw_rebuild(
    chart::RONonlinearInputChart;
    source_coordinate_names=
        copy(getfield(chart, :source_coordinate_names)),
    source_coordinate_units=
        copy(getfield(chart, :source_coordinate_units)),
    source_jacobian_at_reference=
        copy(getfield(chart, :source_jacobian_at_reference)),
    source_hessians=copy(getfield(chart, :source_hessians)),
    reference_singular_values=
        copy(getfield(chart, :reference_singular_values)),
    reference_numerical_rank=getfield(chart, :reference_numerical_rank),
    reference_condition_number=getfield(chart, :reference_condition_number),
    immersion_scope=getfield(chart, :immersion_scope),
    global_injectivity_certified=
        getfield(chart, :global_injectivity_certified),
    declaration_sha256=getfield(chart, :declaration_sha256),
    content_sha256=getfield(chart, :content_sha256),
)
    return RONonlinearInputChart(
        source_coordinate_names,
        source_coordinate_units,
        copy(getfield(chart, :control_coordinate_names)),
        copy(getfield(chart, :control_coordinate_units)),
        copy(getfield(chart, :control_reference)),
        copy(getfield(chart, :domain_lower)),
        copy(getfield(chart, :domain_upper)),
        copy(getfield(chart, :source_reference)),
        source_jacobian_at_reference,
        source_hessians,
        getfield(chart, :rank_atol),
        getfield(chart, :rank_rtol),
        getfield(chart, :max_condition_number),
        reference_singular_values,
        reference_numerical_rank,
        reference_condition_number,
        immersion_scope,
        global_injectivity_certified,
        getfield(chart, :limits),
        declaration_sha256,
        content_sha256,
        Val(:validated),
    )
end

@testset "declarative quadratic nonlinear RO input chart" begin
    @testset "analytic quadratic map, local Jacobian, and full chain rule" begin
        source_reference = [0.5, -0.3]
        jacobian0 = [1.0 0.2; -0.1 0.8]
        hessians = zeros(2, 2, 2)
        hessians[1, :, :] = [0.4 0.1; 0.1 -0.2]
        hessians[2, :, :] = [-0.2 0.05; 0.05 0.3]
        chart = RONonlinearInputChart(
            source_coordinate_names=["theta_A", "theta_B"],
            source_coordinate_units=["log10 molar", "log10 molar"],
            control_coordinate_names=["dose", "ratio"],
            control_coordinate_units=["log10 molar", "dimensionless"],
            control_reference=[0.0, 0.0],
            domain_lower=[-0.5, -0.5],
            domain_upper=[0.5, 0.5],
            source_reference=source_reference,
            source_jacobian_at_reference=jacobian0,
            source_hessians=hessians,
        )
        u = [0.2, -0.1]
        expected_source = [
            source_reference[source] + dot(jacobian0[source, :], u) +
                0.5 * dot(u, hessians[source, :, :] * u)
            for source in 1:2
        ]
        expected_jacobian = copy(jacobian0)
        for source in 1:2
            expected_jacobian[source, :] .+= hessians[source, :, :] * u
        end

        evaluation = evaluate_ro_nonlinear_input_chart(chart, u)
        @test evaluation.source_coordinates ≈ expected_source rtol=1e-14
        @test evaluation.source_jacobian ≈ expected_jacobian rtol=1e-14
        @test evaluation.source_hessians == hessians
        @test evaluation.numerical_rank == 2
        @test evaluation.immersion_scope ==
            :pointwise_numerically_admitted_local_immersion_only
        @test !evaluation.global_injectivity_certified
        @test map_ro_nonlinear_source_coordinates(chart, u) ≈
            expected_source rtol=1e-14
        @test ro_nonlinear_source_jacobian(chart, u) ≈
            expected_jacobian rtol=1e-14

        source_ro = [1.0 -2.0; 0.25 0.75; -1.5 0.5]
        @test pullback_ro_nonlinear_matrix(
            chart, u, source_ro; source_axis_identity(chart, u)...) ≈
            source_ro * expected_jacobian rtol=1e-14
        source_tensor = reshape(Float64.(1:24), 2, 2, 3, 2)
        pulled_tensor = pullback_ro_nonlinear_tensor(
            chart, u, source_tensor; source_axis_identity(chart, u)...)
        @test size(pulled_tensor) == (2, 2, 3, 2)
        for index in CartesianIndices((2, 2, 3))
            leading = Tuple(index)
            @test vec(pulled_tensor[leading..., :]) ≈
                vec(source_tensor[leading..., :]' * expected_jacobian) rtol=1e-14
        end

        source_gradient = [1.2, -0.7]
        source_hessian = [0.5 0.1; 0.1 -0.4]
        source_derivatives = bound_source_derivatives(
            chart, source_gradient, source_hessian;
            control_coordinates=u,
            output_name="quadratic_score",
            output_unit="score",
        )
        second = pullback_ro_nonlinear_hessian(
            chart, u, source_derivatives)
        @test second.source_derivatives_sha256 ==
            source_derivatives.content_sha256
        @test second.output_name == "quadratic_score"
        @test second.output_unit == "score"
        expected_affine = expected_jacobian' * source_hessian *
            expected_jacobian
        expected_chart = source_gradient[1] .* hessians[1, :, :] +
            source_gradient[2] .* hessians[2, :, :]
        @test second.control_gradient ≈
            vec(source_gradient' * expected_jacobian) rtol=1e-14
        @test second.affine_sandwich_term ≈ expected_affine rtol=1e-14
        @test second.chart_hessian_term ≈ expected_chart rtol=1e-14
        @test second.control_hessian ≈
            expected_affine + expected_chart rtol=1e-14

        @test chart.source_coordinate_names == ["theta_A", "theta_B"]
        @test chart.source_coordinate_units ==
            ["log10 molar", "log10 molar"]
        @test chart.control_coordinate_names == ["dose", "ratio"]
        @test chart.control_coordinate_units ==
            ["log10 molar", "dimensionless"]
        @test chart.control_reference == zeros(2)
        @test chart.domain_lower == fill(-0.5, 2)
        @test chart.domain_upper == fill(0.5, 2)
        @test chart.source_reference == source_reference
        @test chart.source_jacobian_at_reference == jacobian0
        @test chart.source_hessians == hessians
        @test chart.immersion_scope ==
            :pointwise_numerically_admitted_local_immersion_only
        @test !chart.global_injectivity_certified
    end

    @testset "zero-Hessian affine degeneration agrees with affine chart" begin
        offset = [0.25, -0.5, 1.0]
        jacobian = [1.0 0.25; -0.2 1.0; 0.5 -0.4]
        affine = ROAffineInputChart(offset, jacobian)
        nonlinear = RONonlinearInputChart(
            source_coordinate_names=["a", "b", "c"],
            source_coordinate_units=fill("dimensionless", 3),
            control_coordinate_names=["sum", "difference"],
            control_coordinate_units=fill("dimensionless", 2),
            control_reference=zeros(2),
            domain_lower=fill(-2.0, 2),
            domain_upper=fill(2.0, 2),
            source_reference=offset,
            source_jacobian_at_reference=jacobian,
            source_hessians=zeros(3, 2, 2),
        )
        u = [0.4, -0.3]
        source_ro = reshape(Float64.(1:12), 4, 3)
        @test map_ro_nonlinear_source_coordinates(nonlinear, u) ≈
            map_ro_source_coordinates(affine, u) rtol=1e-14
        @test pullback_ro_nonlinear_matrix(
            nonlinear,
            u,
            source_ro;
            source_axis_identity(nonlinear, u)...,
        ) ≈
            pullback_ro_matrix(affine, source_ro) rtol=1e-14

        source_gradient = [0.4, -0.7, 1.2]
        source_hessian = [1.0 0.1 -0.2; 0.1 0.5 0.3; -0.2 0.3 -0.4]
        second = pullback_ro_nonlinear_hessian(
            nonlinear,
            u,
            bound_source_derivatives(
                nonlinear, source_gradient, source_hessian;
                control_coordinates=u,
            ),
        )
        @test second.chart_hessian_term == zeros(2, 2)
        @test second.control_hessian ≈
            jacobian' * source_hessian * jacobian rtol=1e-14
    end

    @testset "omitting the chart Hessian reverses mixed-curvature sign" begin
        # theta(u) = u - u^2 and z(theta) = theta + theta^2/2 at u=0.
        # The affine sandwich is +1, but grad(z)*H(theta) is -2, so the
        # complete pullback is -1. Dropping the second term flips the sign.
        chart = one_dimensional_chart(
            hessian=reshape([-2.0], 1, 1, 1),
            domain=(-0.1, 0.1),
        )
        second = pullback_ro_nonlinear_hessian(
            chart,
            [0.0],
            bound_source_derivatives(chart, [1.0], [1.0;;]),
        )
        @test second.affine_sandwich_term == [1.0;;]
        @test second.chart_hessian_term == [-2.0;;]
        @test second.control_hessian == [-1.0;;]
        @test sign(second.affine_sandwich_term[1, 1]) == 1
        @test sign(second.control_hessian[1, 1]) == -1
    end

    @testset "pointwise fold, rank grey zone, and conditioning reject" begin
        # theta = u - u^2/2 has J=1-u: the reference is an immersion, while
        # the declared endpoint u=1 is an exact fold and must fail at query.
        fold_chart = one_dimensional_chart(
            hessian=reshape([-1.0], 1, 1, 1),
            domain=(-0.5, 1.0),
        )
        @test evaluate_ro_nonlinear_input_chart(fold_chart, [0.5]).numerical_rank == 1
        fold_error = try
            evaluate_ro_nonlinear_input_chart(fold_chart, [1.0])
            nothing
        catch caught
            caught
        end
        @test fold_error isa RONonlinearChartImmersionRejected
        @test fold_error.reason == :non_immersion

        @test_throws RONonlinearChartImmersionRejected begin
            RONonlinearInputChart(
                source_coordinate_names=["a", "b"],
                source_coordinate_units=fill("1", 2),
                control_coordinate_names=["u", "v"],
                control_coordinate_units=fill("1", 2),
                control_reference=zeros(2),
                domain_lower=fill(-1.0, 2),
                domain_upper=fill(1.0, 2),
                source_reference=zeros(2),
                source_jacobian_at_reference=Matrix(Diagonal([1.0, 1e-13])),
                source_hessians=zeros(2, 2, 2),
                rank_rtol=1e-12,
            )
        end

        condition_error = try
            RONonlinearInputChart(
                source_coordinate_names=["a", "b"],
                source_coordinate_units=fill("1", 2),
                control_coordinate_names=["u", "v"],
                control_coordinate_units=fill("1", 2),
                control_reference=zeros(2),
                domain_lower=fill(-1.0, 2),
                domain_upper=fill(1.0, 2),
                source_reference=zeros(2),
                source_jacobian_at_reference=Matrix(Diagonal([1.0, 1e-8])),
                source_hessians=zeros(2, 2, 2),
                rank_rtol=1e-12,
                max_condition_number=1e7,
            )
            nothing
        catch caught
            caught
        end
        @test condition_error isa RONonlinearChartImmersionRejected
        @test condition_error.reason == :condition_number_grey_zone
        @test_throws DomainError evaluate_ro_nonlinear_input_chart(
            fold_chart, [nextfloat(1.0)])
    end

    @testset "shape, finite-value, metadata, and Hessian admission" begin
        asymmetric = zeros(2, 2, 2)
        asymmetric[1, 1, 2] = 0.25
        @test_throws ArgumentError RONonlinearInputChart(
            source_coordinate_names=["theta_1", "theta_2"],
            source_coordinate_units=["1", "1"],
            control_coordinate_names=["u", "v"],
            control_coordinate_units=["1", "1"],
            control_reference=zeros(2),
            domain_lower=fill(-1.0, 2),
            domain_upper=fill(1.0, 2),
            source_reference=zeros(2),
            source_jacobian_at_reference=Matrix{Float64}(I, 2, 2),
            source_hessians=asymmetric,
        )
        @test_throws DimensionMismatch RONonlinearInputChart(
            source_coordinate_names=["theta"],
            source_coordinate_units=["1"],
            control_coordinate_names=["u", "v"],
            control_coordinate_units=["1", "1"],
            control_reference=zeros(2),
            domain_lower=fill(-1.0, 2),
            domain_upper=fill(1.0, 2),
            source_reference=[0.0],
            source_jacobian_at_reference=ones(1, 2),
            source_hessians=zeros(1, 2, 2),
        )
        @test_throws ArgumentError one_dimensional_chart(
            hessian=reshape([NaN], 1, 1, 1))
        @test_throws ArgumentError one_dimensional_chart(
            source_reference=[Inf])
        @test_throws ArgumentError one_dimensional_chart(
            jacobian=[true;;])
        @test_throws ArgumentError one_dimensional_chart(domain=(0.0, 0.0))
        @test_throws ArgumentError one_dimensional_chart(source_name=" theta")
        @test_throws ArgumentError one_dimensional_chart(source_unit="1 ")
        @test_throws ArgumentError one_dimensional_chart(control_name="u\0x")
        @test_throws ArgumentError one_dimensional_chart(control_unit="\t1")

        chart = one_dimensional_chart()
        @test_throws DimensionMismatch evaluate_ro_nonlinear_input_chart(
            chart, [0.0, 1.0])
        @test_throws ArgumentError evaluate_ro_nonlinear_input_chart(
            chart, [NaN])
        @test_throws DimensionMismatch pullback_ro_nonlinear_matrix(
            chart,
            [0.0],
            ones(2, 2);
            source_axis_identity(chart, [0.0])...,
        )
        @test_throws ArgumentError pullback_ro_nonlinear_matrix(
            chart,
            [0.0],
            [NaN;;];
            source_axis_identity(chart, [0.0])...,
        )
        @test_throws DimensionMismatch RONonlinearSourceDerivatives(
            chart_declaration_sha256=chart.declaration_sha256,
            output_name="z",
            output_unit="1",
            source_coordinate_names=["theta", "other"],
            source_coordinate_units=["1", "1"],
            source_coordinates=zeros(2),
            gradient=[1.0, 2.0],
            hessian=[1.0;;],
        )
        @test_throws MethodError pullback_ro_nonlinear_hessian(
            chart, [0.0], [1.0], [1.0;;])

        two_source = RONonlinearInputChart(
            source_coordinate_names=["a", "b"],
            source_coordinate_units=["1", "1"],
            control_coordinate_names=["u"],
            control_coordinate_units=["1"],
            control_reference=[0.0],
            domain_lower=[-1.0],
            domain_upper=[1.0],
            source_reference=zeros(2),
            source_jacobian_at_reference=ones(2, 1),
            source_hessians=zeros(2, 1, 1),
        )
        @test_throws ArgumentError RONonlinearSourceDerivatives(
            chart_declaration_sha256=two_source.declaration_sha256,
            output_name="z",
            output_unit="1",
            source_coordinate_names=["a", "b"],
            source_coordinate_units=["1", "1"],
            source_coordinates=zeros(2),
            gradient=ones(2),
            hessian=[1.0 0.1; 0.0 1.0],
        )
        accepted = pullback_ro_nonlinear_hessian(
            two_source,
            [0.0],
            bound_source_derivatives(
                two_source, ones(2), [1.0 0.1; 0.1 1.0]),
        )
        @test isfinite(accepted.control_hessian[1, 1])
        @test_throws UndefKeywordError pullback_ro_nonlinear_matrix(
            two_source, [0.0], ones(1, 2))
        axis_identity = source_axis_identity(two_source, [0.0])
        @test_throws ArgumentError pullback_ro_nonlinear_matrix(
            two_source,
            [0.0],
            ones(1, 2);
            source_coordinate_names=["b", "a"],
            source_coordinate_units=axis_identity.source_coordinate_units,
            source_coordinates=axis_identity.source_coordinates,
        )
        @test_throws ArgumentError pullback_ro_nonlinear_matrix(
            two_source,
            [0.0],
            ones(1, 2);
            source_coordinate_names=axis_identity.source_coordinate_names,
            source_coordinate_units=["mol", "mol"],
            source_coordinates=axis_identity.source_coordinates,
        )
        @test_throws ArgumentError pullback_ro_nonlinear_matrix(
            two_source,
            [0.0],
            ones(1, 2);
            source_coordinate_names=axis_identity.source_coordinate_names,
            source_coordinate_units=axis_identity.source_coordinate_units,
            source_coordinates=axis_identity.source_coordinates .+ 1.0,
        )
        @test_throws RONonlinearChartLimitExceeded begin
            pullback_ro_nonlinear_matrix(
                two_source,
                [0.0],
                _RONCHugeSourceArray();
                axis_identity...,
            )
        end
        wrong_order = bound_source_derivatives(
            two_source,
            ones(2),
            Matrix{Float64}(I, 2, 2);
            source_coordinate_names=["b", "a"],
        )
        @test_throws ArgumentError pullback_ro_nonlinear_hessian(
            two_source, [0.0], wrong_order)
        wrong_units = bound_source_derivatives(
            two_source,
            ones(2),
            Matrix{Float64}(I, 2, 2);
            source_coordinate_units=["mol", "mol"],
        )
        @test_throws ArgumentError pullback_ro_nonlinear_hessian(
            two_source, [0.0], wrong_units)

        point_bound = bound_source_derivatives(
            two_source,
            ones(2),
            Matrix{Float64}(I, 2, 2);
            control_coordinates=[0.25],
        )
        @test_throws ArgumentError pullback_ro_nonlinear_hessian(
            two_source, [0.0], point_bound)

        same_labels_different_chart = RONonlinearInputChart(
            source_coordinate_names=["a", "b"],
            source_coordinate_units=["1", "1"],
            control_coordinate_names=["u"],
            control_coordinate_units=["1"],
            control_reference=[0.0],
            domain_lower=[-2.0],
            domain_upper=[2.0],
            source_reference=zeros(2),
            source_jacobian_at_reference=ones(2, 1),
            source_hessians=zeros(2, 1, 1),
        )
        @test map_ro_nonlinear_source_coordinates(
            same_labels_different_chart, [0.0]) == zeros(2)
        @test_throws ArgumentError pullback_ro_nonlinear_hessian(
            same_labels_different_chart,
            [0.0],
            bound_source_derivatives(
                two_source, ones(2), Matrix{Float64}(I, 2, 2)),
        )
    end

    @testset "finite budgets and cooperative cancellation" begin
        coefficient_limits = RONonlinearChartLimits(
            max_coefficient_scalars=2)
        @test_throws RONonlinearChartLimitExceeded one_dimensional_chart(
            limits=coefficient_limits)

        metadata_limits = RONonlinearChartLimits(max_metadata_bytes=3)
        @test_throws RONonlinearChartLimitExceeded one_dimensional_chart(
            limits=metadata_limits)

        factorization_limits = RONonlinearChartLimits(
            max_factorization_work=1,
            max_operation_scalars=100,
        )
        two_dimensional = () -> RONonlinearInputChart(
            source_coordinate_names=["a", "b"],
            source_coordinate_units=["1", "1"],
            control_coordinate_names=["u", "v"],
            control_coordinate_units=["1", "1"],
            control_reference=zeros(2),
            domain_lower=fill(-1.0, 2),
            domain_upper=fill(1.0, 2),
            source_reference=zeros(2),
            source_jacobian_at_reference=Matrix{Float64}(I, 2, 2),
            source_hessians=zeros(2, 2, 2),
            limits=factorization_limits,
        )
        @test_throws RONonlinearChartLimitExceeded two_dimensional()

        operation_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_operation_scalars=2))
        @test_throws RONonlinearChartLimitExceeded begin
            evaluate_ro_nonlinear_input_chart(operation_chart, [0.0])
        end

        exact_matrix_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_operation_scalars=14))
        @test pullback_ro_nonlinear_matrix(
            exact_matrix_chart,
            [0.0],
            [2.0;;];
            source_axis_identity(exact_matrix_chart, [0.0])...,
        ) == [2.0;;]
        under_matrix_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_operation_scalars=13))
        matrix_error = try
            pullback_ro_nonlinear_matrix(
                under_matrix_chart,
                [0.0],
                [2.0;;];
                source_axis_identity(under_matrix_chart, [0.0])...,
            )
            nothing
        catch error
            error
        end
        @test matrix_error isa RONonlinearChartLimitExceeded
        @test matrix_error.phase == :operation_scalars
        @test matrix_error.requested == 14
        @test matrix_error.limit == 13

        exact_hessian_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_operation_scalars=18))
        exact_derivatives = bound_source_derivatives(
            exact_hessian_chart, [1.0], [1.0;;])
        @test pullback_ro_nonlinear_hessian(
            exact_hessian_chart, [0.0], exact_derivatives).control_hessian ==
            [1.0;;]
        under_hessian_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_operation_scalars=17))
        under_derivatives = bound_source_derivatives(
            under_hessian_chart, [1.0], [1.0;;])
        @test_throws RONonlinearChartLimitExceeded begin
            pullback_ro_nonlinear_hessian(
                under_hessian_chart, [0.0], under_derivatives)
        end
        @test_throws ArgumentError BindingAndCatalysis._ronc_limit(
            :operation_scalars, -1, 1)

        constructor_calls = Ref(0)
        constructor_cancel = () -> begin
            constructor_calls[] += 1
            constructor_calls[] >= 3 && throw(InterruptException())
        end
        @test_throws InterruptException one_dimensional_chart(
            cancel_check=constructor_cancel)
        @test constructor_calls[] == 3

        chart = one_dimensional_chart()
        derivatives = bound_source_derivatives(chart, [1.0], [1.0;;])
        @test_throws InterruptException evaluate_ro_nonlinear_input_chart(
            chart, [0.0]; cancel_check=() -> throw(InterruptException()))
        @test_throws InterruptException pullback_ro_nonlinear_hessian(
            chart,
            [0.0],
            derivatives;
            cancel_check=() -> throw(InterruptException()),
        )
    end

    @testset "detached inputs, content seal, raw bypass, and replay" begin
        names = ["theta"]
        units = ["1"]
        controls = ["u"]
        control_units = ["1"]
        reference = [0.0]
        lower = [-1.0]
        upper = [1.0]
        source_reference = [0.25]
        jacobian = [1.0;;]
        hessians = reshape([0.2], 1, 1, 1)
        chart = RONonlinearInputChart(
            source_coordinate_names=names,
            source_coordinate_units=units,
            control_coordinate_names=controls,
            control_coordinate_units=control_units,
            control_reference=reference,
            domain_lower=lower,
            domain_upper=upper,
            source_reference=source_reference,
            source_jacobian_at_reference=jacobian,
            source_hessians=hessians,
        )
        names[1] = "mutated"
        units[1] = "mutated"
        controls[1] = "mutated"
        control_units[1] = "mutated"
        reference[1] = 0.5
        lower[1] = -2.0
        upper[1] = 2.0
        source_reference[1] = 99.0
        jacobian[1, 1] = 99.0
        hessians[1, 1, 1] = 99.0
        @test chart.source_coordinate_names == ["theta"]
        @test chart.source_coordinate_units == ["1"]
        @test chart.control_coordinate_names == ["u"]
        @test chart.control_reference == [0.0]
        @test chart.domain_lower == [-1.0]
        @test chart.domain_upper == [1.0]
        @test chart.source_reference == [0.25]
        @test chart.source_jacobian_at_reference == [1.0;;]
        @test chart.source_hessians == reshape([0.2], 1, 1, 1)

        public_hessians = chart.source_hessians
        public_jacobian = chart.source_jacobian_at_reference
        public_names = chart.source_coordinate_names
        public_hessians .= 0.0
        public_jacobian .= 0.0
        public_names[1] = "changed"
        @test chart.source_hessians == reshape([0.2], 1, 1, 1)
        @test chart.source_jacobian_at_reference == [1.0;;]
        @test chart.source_coordinate_names == ["theta"]

        replayed = replay_ro_nonlinear_input_chart(chart)
        @test replayed.declaration_sha256 == chart.declaration_sha256
        @test replayed.content_sha256 == chart.content_sha256
        @test replayed.source_hessians == chart.source_hessians
        duplicate = one_dimensional_chart(
            source_reference=[0.25],
            hessian=reshape([0.2], 1, 1, 1),
        )
        @test duplicate.declaration_sha256 != chart.declaration_sha256
        # Metadata is part of replay identity, so otherwise equal coefficients
        # with different units/names do not collide.

        stale_singular_values = [2.0]
        stale_content = BindingAndCatalysis._ronc_content_sha256(
            getfield(chart, :declaration_sha256),
            stale_singular_values,
            1,
            1.0,
            RO_NONLINEAR_INPUT_CHART_SCOPE,
            false,
        )
        @test_throws ArgumentError raw_rebuild(
            chart;
            reference_singular_values=stale_singular_values,
            content_sha256=stale_content,
        )

        zero_jacobian = zeros(1, 1)
        forged_declaration = BindingAndCatalysis._ronc_declaration_sha256(
            copy(getfield(chart, :source_coordinate_names)),
            copy(getfield(chart, :source_coordinate_units)),
            copy(getfield(chart, :control_coordinate_names)),
            copy(getfield(chart, :control_coordinate_units)),
            copy(getfield(chart, :control_reference)),
            copy(getfield(chart, :domain_lower)),
            copy(getfield(chart, :domain_upper)),
            copy(getfield(chart, :source_reference)),
            zero_jacobian,
            copy(getfield(chart, :source_hessians)),
            getfield(chart, :rank_atol),
            getfield(chart, :rank_rtol),
            getfield(chart, :max_condition_number),
            getfield(chart, :limits),
        )
        forged_content = BindingAndCatalysis._ronc_content_sha256(
            forged_declaration,
            [1.0],
            1,
            1.0,
            RO_NONLINEAR_INPUT_CHART_SCOPE,
            false,
        )
        @test_throws RONonlinearChartImmersionRejected raw_rebuild(
            chart;
            source_jacobian_at_reference=zero_jacobian,
            reference_singular_values=[1.0],
            declaration_sha256=forged_declaration,
            content_sha256=forged_content,
        )

        global_content = BindingAndCatalysis._ronc_content_sha256(
            getfield(chart, :declaration_sha256),
            copy(getfield(chart, :reference_singular_values)),
            getfield(chart, :reference_numerical_rank),
            getfield(chart, :reference_condition_number),
            getfield(chart, :immersion_scope),
            true,
        )
        @test_throws ArgumentError raw_rebuild(
            chart;
            global_injectivity_certified=true,
            content_sha256=global_content,
        )
        @test_throws ArgumentError raw_rebuild(
            chart; source_coordinate_names=[" theta"])
        @test_throws ArgumentError raw_rebuild(
            chart; source_coordinate_units=["1\0unit"])

        bounded_metadata_chart = one_dimensional_chart(
            limits=RONonlinearChartLimits(max_metadata_bytes=100))
        @test_throws RONonlinearChartLimitExceeded raw_rebuild(
            bounded_metadata_chart;
            source_coordinate_names=["x"^101],
        )

        derivative_gradient = [1.0]
        derivative_hessian = [2.0;;]
        derivatives = bound_source_derivatives(
            chart, derivative_gradient, derivative_hessian)
        derivative_gradient[1] = 99.0
        derivative_hessian[1, 1] = 99.0
        @test derivatives.gradient == [1.0]
        @test derivatives.hessian == [2.0;;]
        getfield(derivatives, :gradient)[1] = 3.0
        @test_throws ArgumentError derivatives.gradient
        @test_throws ArgumentError pullback_ro_nonlinear_hessian(
            chart, [0.0], derivatives)

        fresh_derivatives = bound_source_derivatives(
            chart, [1.0], [2.0;;];
            output_name="sealed_output",
            output_unit="score",
        )
        stale_point = copy(getfield(
            fresh_derivatives, :source_coordinates))
        stale_point[1] = nextfloat(stale_point[1])
        @test_throws ArgumentError RONonlinearSourceDerivatives(
            getfield(fresh_derivatives, :chart_declaration_sha256),
            getfield(fresh_derivatives, :output_name),
            getfield(fresh_derivatives, :output_unit),
            copy(getfield(fresh_derivatives, :source_coordinate_names)),
            copy(getfield(fresh_derivatives, :source_coordinate_units)),
            stale_point,
            copy(getfield(fresh_derivatives, :gradient)),
            copy(getfield(fresh_derivatives, :hessian)),
            getfield(fresh_derivatives, :limits),
            getfield(fresh_derivatives, :content_sha256),
            Val(:validated),
        )
        point_mutated = bound_source_derivatives(
            chart, [1.0], [2.0;;])
        getfield(point_mutated, :source_coordinates)[1] = 0.75
        @test_throws ArgumentError point_mutated.source_coordinates

        evaluation = evaluate_ro_nonlinear_input_chart(chart, [0.0])
        @test_throws MethodError RONonlinearChartEvaluation(
            getfield(evaluation, :chart_declaration_sha256),
            copy(getfield(evaluation, :control_coordinates)),
            copy(getfield(evaluation, :source_coordinates)),
            copy(getfield(evaluation, :source_jacobian)),
            copy(getfield(evaluation, :source_hessians)),
            copy(getfield(evaluation, :singular_values)),
            getfield(evaluation, :numerical_rank),
            getfield(evaluation, :condition_number),
            getfield(evaluation, :immersion_scope),
            false,
        )
        @test_throws ArgumentError RONonlinearChartEvaluation(
            getfield(evaluation, :chart_declaration_sha256),
            copy(getfield(evaluation, :control_coordinates)),
            copy(getfield(evaluation, :source_coordinates)),
            copy(getfield(evaluation, :source_jacobian)),
            copy(getfield(evaluation, :source_hessians)),
            copy(getfield(evaluation, :singular_values)),
            getfield(evaluation, :numerical_rank),
            getfield(evaluation, :condition_number),
            getfield(evaluation, :immersion_scope),
            true,
            Val(:validated),
        )

        audited_pullback = pullback_ro_nonlinear_hessian(
            chart, [0.0], fresh_derivatives)
        forged_total = copy(getfield(audited_pullback, :control_hessian))
        forged_total[1, 1] += 1.0
        @test_throws ArgumentError RONonlinearHessianPullback(
            getfield(audited_pullback, :chart_declaration_sha256),
            getfield(audited_pullback, :source_derivatives_sha256),
            getfield(audited_pullback, :output_name),
            getfield(audited_pullback, :output_unit),
            copy(getfield(audited_pullback, :control_coordinates)),
            copy(getfield(audited_pullback, :source_coordinates)),
            copy(getfield(audited_pullback, :local_source_jacobian)),
            copy(getfield(audited_pullback, :control_gradient)),
            copy(getfield(audited_pullback, :affine_sandwich_term)),
            copy(getfield(audited_pullback, :chart_hessian_term)),
            forged_total,
            Val(:validated),
        )

        tampered = one_dimensional_chart()
        getfield(tampered, :source_hessians)[1, 1, 1] = 0.5
        @test_throws ArgumentError tampered.source_hessians
        @test_throws ArgumentError evaluate_ro_nonlinear_input_chart(
            tampered, [0.0])
        @test_throws ArgumentError replay_ro_nonlinear_input_chart(tampered)
    end
end

end # module
