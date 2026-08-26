using Test
using LinearAlgebra
using BindingAndCatalysis

ENV["BNC_NO_PROGRESS"] = "1"

function _ro_field_single_model()
    return Bnc(
        N=reshape([1, 1, -1], 1, 3),
        L=[1 0 1; 0 1 1],
        x_sym=[:A, :L, :AL],
        q_sym=[:tA, :tL],
        K_sym=[:Kd1],
    )
end

function _ro_field_competitive_model()
    return Bnc(
        N=[1 0 1 -1 0; 0 1 1 0 -1],
        L=[1 0 0 1 0; 0 1 0 0 1; 0 0 1 1 1],
        x_sym=[:A, :B, :L, :AL, :BL],
        q_sym=[:tA, :tB, :tL],
        K_sym=[:Kd1, :Kd2],
    )
end

@testset "qK regime membership separates closed cells from relaxed fallback" begin
    model = _ro_field_competitive_model()
    relaxed_first = assign_regime_qK(
        model,
        zeros(model.n);
        input_logspace=true,
        return_idx=true,
        eps=Inf,
        membership=:relaxed,
    )
    witness = nothing
    for coordinates in Iterators.product(fill((-12.0, 0.0, 12.0), model.n)...)
        point = collect(coordinates)
        closed_idx = assign_regime_qK(
            model,
            point;
            input_logspace=true,
            return_idx=true,
            membership=:closed_cell,
        )
        if closed_idx >= 1 && closed_idx != relaxed_first
            witness = (point, closed_idx)
            break
        end
    end
    @test witness !== nothing
    if witness !== nothing
        point, closed_idx = witness
        @test assign_regime_qK(
            model,
            point;
            input_logspace=true,
            return_idx=true,
            membership=:closed_cell,
        ) == closed_idx
        @test assign_regime_qK(
            model,
            point;
            input_logspace=true,
            return_idx=true,
            eps=Inf,
            membership=:relaxed,
        ) == relaxed_first
    end

    @test_throws ArgumentError assign_regime_qK(
        model,
        zeros(model.n);
        input_logspace=true,
        eps=Inf,
        strict=true,
    )
    @test_throws ArgumentError assign_regime_qK(
        model,
        zeros(model.n);
        input_logspace=true,
        eps=1.0e-8,
        membership=:closed_cell,
    )
    @test_throws ArgumentError assign_regime_qK(
        model,
        zeros(model.n);
        input_logspace=true,
        strict=true,
        membership=:relaxed,
    )
    @test_throws ArgumentError assign_regime_qK(
        model,
        zeros(model.n);
        input_logspace=true,
        membership=:unsupported,
    )
    @test assign_regime_qK(
        model,
        fill(NaN, model.n);
        input_logspace=true,
        return_idx=true,
        membership=:closed_cell,
    ) == 0
    @test assign_regime_qK(
        model,
        fill(NaN, model.n);
        input_logspace=true,
        membership=:closed_cell,
    ) === nothing
end

@testset "multi-input numerical reaction-order field" begin
    @testset "Jacobian API and independent sanity checks" begin
        model = _ro_field_single_model()
        logqK = zeros(model.n)
        logx = qK2x(
            model,
            logqK;
            input_logspace=true,
            output_logspace=true,
        )
        jacobian = ∂logx_∂logqK(
            model;
            x=logx,
            qK=logqK,
            input_logspace=true,
        )

        # For tA=tL=Kd=1, AL=(3-sqrt(5))/2. Implicit differentiation
        # of AL*Kd=(tA-AL)*(tL-AL) gives these exact log derivatives.
        @test jacobian[3, 1] ≈ (5 + sqrt(5)) / 10 rtol=1e-10
        @test jacobian[3, 2] ≈ (5 + sqrt(5)) / 10 rtol=1e-10
        @test jacobian[3, 3] ≈ -inv(sqrt(5)) rtol=1e-10

        epsilon = 1e-5
        finite_difference = Matrix{Float64}(undef, model.n, model.n)
        for parameter_idx in 1:model.n
            perturbation = zeros(model.n)
            perturbation[parameter_idx] = epsilon
            plus = qK2x(
                model,
                logqK + perturbation;
                input_logspace=true,
                output_logspace=true,
            )
            minus = qK2x(
                model,
                logqK - perturbation;
                input_logspace=true,
                output_logspace=true,
            )
            finite_difference[:, parameter_idx] .=
                (plus - minus) ./ (2epsilon)
        end
        @test jacobian ≈ finite_difference rtol=2e-7 atol=2e-8

        # The trajectory helper previously accessed removed private LU fields.
        # It now delegates to the same current Jacobian implementation and can
        # reconstruct the conserved totals directly from a linear-space x row.
        trajectory_ro = BindingAndCatalysis.get_reaction_order(
            model,
            permutedims(exp10.(logx)),
        )
        @test size(trajectory_ro) == (1, model.n, model.n)
        @test dropdims(trajectory_ro; dims=1) ≈ jacobian rtol=1e-12
    end

    @testset "bounded 2D and 3D sampled fields" begin
        model = _ro_field_competitive_model()
        axis_a = [-0.2, 0.0, 0.2]
        axis_b = [-0.1, 0.1]
        fixed = zeros(model.n)

        field = sample_reaction_order_field(
            model,
            [1, 2],
            [axis_a, axis_b],
            [4, 5],
            fixed;
            max_grid_points=6,
        )
        @test field.axis_indices == [1, 2]
        @test field.axis_coordinates_log10 == [axis_a, axis_b]
        @test field.output_indices == [4, 5]
        @test size(field.output_log10) == (3, 2, 2)
        @test size(field.reaction_orders) == (3, 2, 2, 2)
        @test size(field.validity) == size(field.regime_ids) == (3, 2)
        @test all(field.validity)
        @test all(>=(1), field.regime_ids)
        @test all(isfinite, field.output_log10)
        @test all(isfinite, field.reaction_orders)

        point_logqK = copy(fixed)
        point_logqK[1] = axis_a[2]
        point_logqK[2] = axis_b[1]
        point_logx = qK2x(
            model,
            point_logqK;
            input_logspace=true,
            output_logspace=true,
        )
        point_jacobian = ∂logx_∂logqK(
            model;
            x=point_logx,
            qK=point_logqK,
            input_logspace=true,
        )
        @test field.output_log10[2, 1, :] ≈ point_logx[[4, 5]] rtol=1e-12
        @test field.reaction_orders[2, 1, :, :] ≈
              point_jacobian[[4, 5], [1, 2]] rtol=1e-12

        field_3d = sample_reaction_order_field(
            model,
            [1, 2, 3],
            [[-0.1, 0.1], [-0.1, 0.1], [-0.1, 0.1]],
            [4],
            fixed;
            max_grid_points=8,
        )
        @test size(field_3d.output_log10) == (2, 2, 2, 1)
        @test size(field_3d.reaction_orders) == (2, 2, 2, 1, 3)
        @test size(field_3d.validity) == size(field_3d.regime_ids) == (2, 2, 2)
        @test all(field_3d.validity)

        # Swapping semantic axis order transposes domain dimensions and swaps
        # the final derivative-component axis; it does not change the field.
        swapped = sample_reaction_order_field(
            model,
            [2, 1],
            [axis_b, axis_a],
            [4, 5],
            fixed;
            max_grid_points=6,
        )
        @test field.output_log10 ≈
              permutedims(swapped.output_log10, (2, 1, 3)) rtol=1e-12
        swapped_ro = permutedims(swapped.reaction_orders, (2, 1, 3, 4))
        @test field.reaction_orders ≈ swapped_ro[:, :, :, [2, 1]] rtol=1e-12
        @test field.validity == permutedims(swapped.validity, (2, 1))
        @test field.regime_ids == permutedims(swapped.regime_ids, (2, 1))

        # A deliberately insufficient solver iteration budget produces honest
        # point gaps, not zero-valued outputs or derivatives.
        invalid = sample_reaction_order_field(
            model,
            [1],
            [[-6.0, 6.0]],
            [4],
            fixed;
            max_grid_points=2,
            maxiters=1,
        )
        @test !any(invalid.validity)
        @test all(isnan, invalid.output_log10)
        @test all(isnan, invalid.reaction_orders)
        @test all(==(0), invalid.regime_ids)
    end

    @testset "point budget and cooperative cancellation" begin
        budget_model = _ro_field_competitive_model()
        work_started = Ref(false)
        err = try
            sample_reaction_order_field(
                budget_model,
                [1, 2, 3],
                [
                    range(-1.0, 1.0; length=1_000_000),
                    range(-1.0, 1.0; length=1_000_000),
                    range(-1.0, 1.0; length=1_000_000),
                ],
                [4],
                zeros(budget_model.n);
                max_grid_points=100,
                cancel_check=() -> (work_started[] = true),
            )
            nothing
        catch caught
            caught
        end
        @test err isa ROFieldGridLimitExceeded
        @test err.requested_shape == [1_000_000, 1_000_000, 1_000_000]
        @test err.max_grid_points == 100
        @test !work_started[]
        @test budget_model.BindRegimes === nothing

        cancel_checks = Ref(0)
        @test_throws ErrorException sample_reaction_order_field(
            budget_model,
            [1],
            [[0.0]],
            [4],
            zeros(budget_model.n);
            max_grid_points=1,
            cancel_check=() -> begin
                cancel_checks[] += 1
                error("cancel sampled reaction-order field")
            end,
        )
        @test cancel_checks[] == 1
        @test budget_model.BindRegimes === nothing
    end
end
