using Test
using LinearAlgebra
using BindingAndCatalysis

struct _ROOTestCancellation <: Exception end

function _roo_compose(
    chart,
    source_values,
    source_reaction_orders,
    source_input_hessians;
    kwargs...,
)
    return compose_ro_observable_jet(
        chart,
        source_values,
        source_reaction_orders,
        source_input_hessians;
        source_component_order=chart.source_component_order,
        source_units=chart.source_units,
        kwargs...,
    )
end

function _roo_test_affine_chart(;
    limits=ROObservableChartLimits(),
    cancel_check=() -> nothing,
)
    return ROAffineObservableChart(
        ["z_A", "z_B", "z_C"],
        ["y_sum", "y_contrast"],
        [
            1.0  2.0 -1.0
            0.5 -1.0  3.0
        ];
        source_units=["log10 molar", "log10 molar", "log10 molar"],
        output_units=["score", "score"],
        source_reference=[1.0, -2.0, 0.5],
        output_reference=[10.0, -3.0],
        domain_lower=[-4.0, -5.0, -2.0],
        domain_upper=[4.0, 3.0, 2.0],
        limits=limits,
        cancel_check=cancel_check,
    )
end

function _roo_test_quadratic_chart(;
    limits=ROObservableChartLimits(),
    cancel_check=() -> nothing,
)
    hessians = zeros(2, 2, 2)
    hessians[1, :, :] .= [2.0 3.0; 3.0 4.0]
    hessians[2, :, :] .= [0.0 -2.0; -2.0 6.0]
    return ROQuadraticObservableChart(
        ["z_left", "z_right"],
        ["y_curved", "y_coupled"],
        [1.0 -2.0; -1.0 0.5],
        hessians;
        source_units=["log10 occupancy", "log10 occupancy"],
        output_units=["curvature score", "coupling score"],
        source_reference=[0.5, -1.0],
        output_reference=[7.0, -4.0],
        domain_lower=[-2.0, -3.0],
        domain_upper=[3.0, 3.0],
        limits=limits,
        cancel_check=cancel_check,
    )
end

function _roo_manual_chain(jacobian, observable_hessians, source_ro,
                           source_hessians)
    output_count, source_count = size(jacobian)
    control_count = size(source_ro, 2)
    first = zeros(output_count, control_count)
    second = zeros(output_count, control_count, control_count)
    source_only_second = zeros(output_count, control_count, control_count)
    for output in 1:output_count, left_control in 1:control_count
        for source in 1:source_count
            first[output, left_control] +=
                jacobian[output, source] * source_ro[source, left_control]
        end
        for right_control in 1:control_count
            for source in 1:source_count
                source_only_second[output, left_control, right_control] +=
                    jacobian[output, source] *
                    source_hessians[source, left_control, right_control]
            end
            second[output, left_control, right_control] =
                source_only_second[output, left_control, right_control]
            for left_source in 1:source_count,
                right_source in 1:source_count
                second[output, left_control, right_control] +=
                    observable_hessians[output, left_source, right_source] *
                    source_ro[left_source, left_control] *
                    source_ro[right_source, right_control]
            end
        end
    end
    return first, second, source_only_second
end

@testset "declarative affine/quadratic observable charts" begin
    @testset "affine value, first order, and second-order parity" begin
        chart = _roo_test_affine_chart()
        @test ro_source_observable_count(chart) == 3
        @test ro_derived_observable_count(chart) == 2
        @test chart.map_kind == :affine
        @test chart.regularity == :C2
        @test length(chart.observable_chart_identity) == 64
        @test RO_OBSERVABLE_CHART_VERSION ==
            "bne-ro-observable-chart/v1.0.0"
        @test RO_OBSERVABLE_CHART_SCOPE ==
            "finite_declarative_affine_or_quadratic_observable_map"

        source_values = [2.0, -1.0, -0.5]
        delta = source_values - [1.0, -2.0, 0.5]
        linear = [1.0 2.0 -1.0; 0.5 -1.0 3.0]
        evaluated = evaluate_ro_observables(chart, source_values)
        @test evaluated.output_values ≈ [10.0, -3.0] + linear * delta
        @test evaluated.observable_jacobian == linear
        @test all(iszero, evaluated.observable_hessians)
        @test evaluated.observable_chart_version ==
            RO_OBSERVABLE_CHART_VERSION
        @test evaluated.observable_chart_scope == RO_OBSERVABLE_CHART_SCOPE
        @test evaluated.source_component_order == ["z_A", "z_B", "z_C"]
        @test evaluated.output_component_order == ["y_sum", "y_contrast"]

        source_ro = [1.0 2.0; -1.0 0.5; 3.0 -2.0]
        source_hessians = zeros(3, 2, 2)
        source_hessians[1, :, :] .= [1.0 0.25; 0.25 -2.0]
        source_hessians[2, :, :] .= [-1.0 2.0; 2.0 0.5]
        source_hessians[3, :, :] .= [0.5 -3.0; -3.0 4.0]
        result = _roo_compose(
            chart,
            source_values,
            source_ro,
            source_hessians;
            control_component_order=["u_ligand", "u_total"],
            control_units=["log10 molar", "log10 molar"],
        )
        @test result.reaction_orders ≈ linear * source_ro
        for output in 1:2
            expected = zeros(2, 2)
            for source in 1:3
                expected .+= linear[output, source] .* source_hessians[source, :, :]
            end
            @test result.input_hessians[output, :, :] ≈ expected
        end
        @test result.control_component_order == ["u_ligand", "u_total"]
        @test result.control_units == ["log10 molar", "log10 molar"]
        @test result.output_component_order == ["y_sum", "y_contrast"]
        @test result.observable_chart_version == RO_OBSERVABLE_CHART_VERSION
        @test result.observable_chart_scope == RO_OBSERVABLE_CHART_SCOPE
    end

    @testset "observable Hessian term is mandatory" begin
        # y = z1*z2 at z=reference, z(u)=u. J_phi=0 and H_u z=0, so
        # omitting H_phi[R_z,R_z] would incorrectly report an all-zero Hessian.
        hessian = reshape([0.0, 1.0, 1.0, 0.0], 1, 2, 2)
        chart = ROQuadraticObservableChart(
            ["z1", "z2"], ["product"], zeros(1, 2), hessian;
            source_units=["a.u.", "a.u."],
            output_units=["a.u.^2"],
            source_reference=zeros(2),
            output_reference=[0.0],
            domain_lower=fill(-2.0, 2),
            domain_upper=fill(2.0, 2),
        )
        result = _roo_compose(
            chart,
            zeros(2),
            Matrix{Float64}(I, 2, 2),
            zeros(2, 2, 2);
            control_component_order=["u1", "u2"],
            control_units=["a.u.", "a.u."],
        )
        @test result.observable_jacobian == zeros(1, 2)
        @test result.reaction_orders == zeros(1, 2)
        @test result.input_hessians[1, :, :] == [0.0 1.0; 1.0 0.0]
        @test !all(iszero, result.input_hessians)
    end

    @testset "multi-output order and nonlinear coupling" begin
        chart = _roo_test_quadratic_chart()
        source_values = [1.5, 1.0]
        source_ro = [2.0 -1.0; 0.5 3.0]
        source_hessians = zeros(2, 2, 2)
        source_hessians[1, :, :] .= [1.0 0.25; 0.25 -2.0]
        source_hessians[2, :, :] .= [-1.0 2.0; 2.0 0.5]
        result = _roo_compose(
            chart,
            source_values,
            source_ro,
            source_hessians;
            control_component_order=["u_A", "u_B"],
            control_units=["log10 molar", "log10 molar"],
        )

        expected_jacobian = [9.0 9.0; -5.0 10.5]
        @test result.output_values ≈ [19.0, 4.0]
        @test result.observable_jacobian ≈ expected_jacobian
        expected_first, expected_second, source_only_second =
            _roo_manual_chain(expected_jacobian, result.observable_hessians,
                source_ro, source_hessians)
        @test result.reaction_orders ≈ expected_first
        @test result.input_hessians ≈ expected_second
        @test maximum(abs, result.input_hessians - source_only_second) > 1.0
        @test result.output_component_order == ["y_curved", "y_coupled"]
        @test result.input_hessians[1, :, :] !=
            result.input_hessians[2, :, :]

        changed_units = ROQuadraticObservableChart(
            chart.source_component_order,
            chart.output_component_order,
            chart.linear_jacobian,
            chart.quadratic_hessians;
            source_units=chart.source_units,
            output_units=["other", "coupling score"],
            source_reference=chart.source_reference,
            output_reference=chart.output_reference,
            domain_lower=chart.domain_lower,
            domain_upper=chart.domain_upper,
        )
        @test changed_units.observable_chart_identity !=
            chart.observable_chart_identity

        swapped = ROQuadraticObservableChart(
            chart.source_component_order,
            reverse(chart.output_component_order),
            chart.linear_jacobian[[2, 1], :],
            chart.quadratic_hessians[[2, 1], :, :];
            source_units=chart.source_units,
            output_units=reverse(chart.output_units),
            source_reference=chart.source_reference,
            output_reference=reverse(chart.output_reference),
            domain_lower=chart.domain_lower,
            domain_upper=chart.domain_upper,
        )
        swapped_result = evaluate_ro_observables(swapped, source_values)
        swapped_chain = _roo_compose(
            swapped,
            source_values,
            source_ro,
            source_hessians;
            control_component_order=["u_A", "u_B"],
            control_units=["log10 molar", "log10 molar"],
        )
        @test swapped_result.output_component_order ==
            ["y_coupled", "y_curved"]
        @test swapped_result.output_values ≈ reverse(result.output_values)
        @test swapped_chain.reaction_orders ≈ result.reaction_orders[[2, 1], :]
        @test swapped_chain.input_hessians ≈
            result.input_hessians[[2, 1], :, :]
        @test swapped.observable_chart_identity != chart.observable_chart_identity
    end

    @testset "closed domain and strict shape/finiteness admission" begin
        chart = _roo_test_quadratic_chart()
        @test evaluate_ro_observables(chart, [-2.0, 3.0]).source_values ==
            [-2.0, 3.0]
        @test_throws DomainError evaluate_ro_observables(chart, [-2.0, 3.01])
        @test_throws DomainError _roo_compose(
            chart, [3.01, 0.0], ones(2, 1), zeros(2, 1, 1);
            control_component_order=["u"], control_units=["a.u."])
        @test_throws DimensionMismatch evaluate_ro_observables(chart, [0.0])
        @test_throws ArgumentError evaluate_ro_observables(chart, [NaN, 0.0])
        @test_throws DimensionMismatch _roo_compose(
            chart, zeros(2), ones(2, 2), zeros(2, 3, 3);
            control_component_order=["u1", "u2"],
            control_units=["a.u.", "a.u."])
        @test_throws ArgumentError _roo_compose(
            chart, zeros(2), [1.0 NaN; 0.0 1.0], zeros(2, 2, 2);
            control_component_order=["u1", "u2"],
            control_units=["a.u.", "a.u."])
        @test_throws ArgumentError _roo_compose(
            chart, zeros(2), ones(2, 2), zeros(2, 2, 2);
            control_component_order=["u", "u"],
            control_units=["a.u.", "a.u."])
        @test_throws DimensionMismatch _roo_compose(
            chart, zeros(2), ones(2, 2), zeros(2, 2, 2);
            control_component_order=["u1", "u2"],
            control_units=["a.u."])
        @test_throws UndefKeywordError compose_ro_observable_jet(
            chart, zeros(2), ones(2, 1), zeros(2, 1, 1);
            control_component_order=["u"], control_units=["a.u."])
        @test_throws ArgumentError compose_ro_observable_jet(
            chart, zeros(2), ones(2, 1), zeros(2, 1, 1);
            source_component_order=reverse(chart.source_component_order),
            source_units=chart.source_units,
            control_component_order=["u"], control_units=["a.u."])
        @test_throws ArgumentError compose_ro_observable_jet(
            chart, zeros(2), ones(2, 1), zeros(2, 1, 1);
            source_component_order=chart.source_component_order,
            source_units=["wrong", chart.source_units[2]],
            control_component_order=["u"], control_units=["a.u."])

        asymmetric = reshape([0.0, 2.0, 1.0, 0.0], 1, 2, 2)
        @test_throws ArgumentError ROQuadraticObservableChart(
            ["z1", "z2"], ["y"], zeros(1, 2), asymmetric;
            source_units=["u", "u"], output_units=["v"],
            source_reference=zeros(2), output_reference=zeros(1),
            domain_lower=fill(-1.0, 2), domain_upper=fill(1.0, 2))
        @test_throws ArgumentError ROQuadraticObservableChart(
            ["z1", "z2"], ["y"], zeros(1, 2), zeros(1, 2, 2);
            source_units=["u", "u"], output_units=["v"],
            source_reference=zeros(2), output_reference=zeros(1),
            domain_lower=fill(-1.0, 2), domain_upper=fill(1.0, 2))
        @test_throws ArgumentError ROAffineObservableChart(
            ["z", "z"], ["y"], ones(1, 2);
            source_units=["u", "u"], output_units=["v"],
            source_reference=zeros(2), output_reference=zeros(1),
            domain_lower=fill(-1.0, 2), domain_upper=fill(1.0, 2))
        @test_throws ArgumentError ROAffineObservableChart(
            ["z"], ["y"], ones(1, 1);
            source_units=["u"], output_units=["v"],
            source_reference=[2.0], output_reference=[0.0],
            domain_lower=[-1.0], domain_upper=[1.0])
        @test_throws ArgumentError ROAffineObservableChart(
            ["z"], ["y"], [NaN;;];
            source_units=["u"], output_units=["v"],
            source_reference=[0.0], output_reference=[0.0],
            domain_lower=[-1.0], domain_upper=[1.0])
        @test_throws ArgumentError ROAffineObservableChart(
            ["z"], ["y"], ones(1, 1);
            source_units=["u"], output_units=["v"],
            source_reference=[0.0], output_reference=[0.0],
            domain_lower=[-1.0], domain_upper=[1.0], regularity=:C1)

        overflow_chart = ROAffineObservableChart(
            ["z"], ["y"], [floatmax(Float64);;];
            source_units=["u"], output_units=["v"],
            source_reference=[0.0], output_reference=[0.0],
            domain_lower=[-3.0], domain_upper=[3.0])
        @test_throws OverflowError evaluate_ro_observables(
            overflow_chart, [2.0])
    end

    @testset "limits and cooperative cancellation" begin
        coefficient_limits = ROObservableChartLimits(max_coefficients=5)
        @test_throws ROObservableChartLimitExceeded _roo_test_affine_chart(
            limits=coefficient_limits)
        metadata_limits = ROObservableChartLimits(max_metadata_bytes=5)
        @test_throws ROObservableChartLimitExceeded _roo_test_affine_chart(
            limits=metadata_limits)

        chain_limits = ROObservableChartLimits(max_chain_rule_terms=10)
        chart = _roo_test_quadratic_chart(limits=chain_limits)
        @test_throws ROObservableChartLimitExceeded _roo_compose(
            chart, zeros(2), ones(2, 2), zeros(2, 2, 2);
            control_component_order=["u1", "u2"],
            control_units=["u", "u"])

        @test_throws _ROOTestCancellation _roo_test_affine_chart(
            cancel_check=() -> throw(_ROOTestCancellation()))
        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 10 && throw(_ROOTestCancellation())
            nothing
        end
        cancel_chart = _roo_test_quadratic_chart()
        @test_throws _ROOTestCancellation _roo_compose(
            cancel_chart, zeros(2), ones(2, 2),
            zeros(2, 2, 2);
            control_component_order=["u1", "u2"],
            control_units=["u", "u"], cancel_check=cancel)
        @test calls[] == 10
    end

    @testset "detached storage, mutation detection, and raw-constructor gate" begin
        sources = ["z_A", "z_B", "z_C"]
        linear = [1.0 2.0 -1.0; 0.5 -1.0 3.0]
        chart = ROAffineObservableChart(
            sources, ["y_sum", "y_contrast"], linear;
            source_units=fill("u", 3), output_units=fill("v", 2),
            source_reference=[1.0, -2.0, 0.5],
            output_reference=[10.0, -3.0],
            domain_lower=[-4.0, -5.0, -2.0],
            domain_upper=[4.0, 3.0, 2.0])
        sources[1] = "changed"
        linear[1, 1] = 99.0
        @test chart.source_component_order == ["z_A", "z_B", "z_C"]
        @test chart.linear_jacobian[1, 1] == 1.0

        public_sources = chart.source_component_order
        public_linear = chart.linear_jacobian
        public_sources[1] = "changed"
        public_linear[1, 1] = 99.0
        @test chart.source_component_order[1] == "z_A"
        @test chart.linear_jacobian[1, 1] == 1.0

        mutated = _roo_test_affine_chart()
        getfield(mutated, :linear_jacobian)[1, 1] = 99.0
        @test_throws ArgumentError evaluate_ro_observables(
            mutated, [1.0, -2.0, 0.5])
        @test_throws ArgumentError mutated.linear_jacobian

        admitted = _roo_test_quadratic_chart()
        args = (
            getfield(admitted, :source_component_order),
            getfield(admitted, :output_component_order),
            getfield(admitted, :source_units),
            getfield(admitted, :output_units),
            getfield(admitted, :source_reference),
            getfield(admitted, :output_reference),
            getfield(admitted, :domain_lower),
            getfield(admitted, :domain_upper),
            getfield(admitted, :linear_jacobian),
            getfield(admitted, :quadratic_hessians),
            getfield(admitted, :map_kind),
            getfield(admitted, :regularity),
            getfield(admitted, :limits),
        )
        poisoned_quadratic = copy(args[10])
        poisoned_quadratic[1] = NaN
        tight_limits = ROObservableChartLimits(max_coefficients=5)
        preflight_error = try
            RODeclarativeObservableChart(
                args[1], args[2], args[3], args[4], args[5], args[6],
                args[7], args[8], args[9], poisoned_quadratic, args[11],
                args[12], tight_limits, "0"^64, Val(:validated))
            nothing
        catch caught
            caught
        end
        @test preflight_error isa ROObservableChartLimitExceeded
        @test preflight_error.phase == :coefficients
        @test_throws ArgumentError RODeclarativeObservableChart(
            args..., "0"^64, Val(:validated))

        forged_quadratic = copy(getfield(admitted, :quadratic_hessians))
        forged_quadratic[1, 1, 2] += 1.0
        forged_identity = BindingAndCatalysis._roo_content_sha256(
            args[1], args[2], args[3], args[4], args[5], args[6], args[7],
            args[8], args[9], forged_quadratic, args[11], args[12], args[13])
        @test_throws ArgumentError RODeclarativeObservableChart(
            args[1], args[2], args[3], args[4], args[5], args[6], args[7],
            args[8], args[9], forged_quadratic, args[11], args[12], args[13],
            forged_identity, Val(:validated))

        affine = _roo_test_affine_chart()
        zero_quadratic = copy(getfield(affine, :quadratic_hessians))
        forged_kind_identity = BindingAndCatalysis._roo_content_sha256(
            getfield(affine, :source_component_order),
            getfield(affine, :output_component_order),
            getfield(affine, :source_units),
            getfield(affine, :output_units),
            getfield(affine, :source_reference),
            getfield(affine, :output_reference),
            getfield(affine, :domain_lower),
            getfield(affine, :domain_upper),
            getfield(affine, :linear_jacobian),
            zero_quadratic,
            :quadratic,
            :C2,
            getfield(affine, :limits),
        )
        @test_throws ArgumentError RODeclarativeObservableChart(
            getfield(affine, :source_component_order),
            getfield(affine, :output_component_order),
            getfield(affine, :source_units),
            getfield(affine, :output_units),
            getfield(affine, :source_reference),
            getfield(affine, :output_reference),
            getfield(affine, :domain_lower),
            getfield(affine, :domain_upper),
            getfield(affine, :linear_jacobian),
            zero_quadratic,
            :quadratic,
            :C2,
            getfield(affine, :limits),
            forged_kind_identity,
            Val(:validated),
        )
    end
end
