using Test
using LinearAlgebra
using BindingAndCatalysis

@testset "full-column-rank affine RO input chart" begin
    @testset "identity chart and detached constructor inputs" begin
        offset = [0.5, -1.0, 2.0]
        jacobian = Matrix{Float64}(I, 3, 3)
        chart = ROAffineInputChart(offset, jacobian)

        @test ro_source_coordinate_count(chart) == 3
        @test ro_control_coordinate_count(chart) == 3
        @test chart.numerical_rank == 3
        @test chart.singular_values == ones(3)
        @test chart.condition_number == 1.0
        @test map_ro_source_coordinates(chart, [1.0, 2.0, 3.0]) ==
            [1.5, 1.0, 5.0]

        source_matrix = [1.0 2.0 3.0; -1.0 0.0 4.0]
        @test pullback_ro_matrix(chart, source_matrix) == source_matrix

        offset[1] = 99.0
        jacobian[1, 1] = 99.0
        @test chart.source_offset == [0.5, -1.0, 2.0]
        @test chart.source_jacobian == Matrix{Float64}(I, 3, 3)

        # Public array properties are detached snapshots, so ordinary property
        # access cannot invalidate the admitted rank/conditioning evidence.
        public_offset = chart.source_offset
        public_jacobian = chart.source_jacobian
        public_singular_values = chart.singular_values
        public_offset .= 0.0
        public_jacobian .= 0.0
        public_singular_values .= 0.0
        @test chart.source_offset == [0.5, -1.0, 2.0]
        @test chart.source_jacobian == Matrix{Float64}(I, 3, 3)
        @test chart.singular_values == ones(3)
    end

    @testset "backing-storage mutation fails closed" begin
        chart = ROAffineInputChart(zeros(2), Matrix{Float64}(I, 2, 2))
        getfield(chart, :source_jacobian)[1, 2] = 0.25
        @test_throws ArgumentError ro_source_coordinate_count(chart)
        @test_throws ArgumentError ro_control_coordinate_count(chart)
        @test_throws ArgumentError map_ro_source_coordinates(chart, ones(2))
        @test_throws ArgumentError pullback_ro_matrix(chart, ones(1, 2))

        chart = ROAffineInputChart(zeros(2), Matrix{Float64}(I, 2, 2))
        getfield(chart, :source_offset)[1] = 0.5
        @test_throws ArgumentError chart.source_offset

        chart = ROAffineInputChart(zeros(2), Matrix{Float64}(I, 2, 2))
        getfield(chart, :singular_values)[1] = 2.0
        @test_throws ArgumentError chart.singular_values
    end

    @testset "raw constructor cannot bypass admission" begin
        offset = zeros(2)
        rank_deficient = [1.0 1.0; 2.0 2.0]
        singular_values = Vector{Float64}(svdvals(rank_deficient))
        forged_rank = 2
        forged_condition = 1.0
        seal = BindingAndCatalysis._ro_chart_content_sha256(
            offset,
            rank_deficient,
            singular_values,
            forged_rank,
            forged_condition,
            1e-12,
            1e10,
        )
        @test_throws ArgumentError ROAffineInputChart(
            offset,
            rank_deficient,
            singular_values,
            forged_rank,
            forged_condition,
            1e-12,
            1e10,
            seal,
            Val(:validated),
        )

        jacobian = Matrix{Float64}(I, 2, 2)
        stale_singular_values = [2.0, 1.0]
        stale_seal = BindingAndCatalysis._ro_chart_content_sha256(
            offset,
            jacobian,
            stale_singular_values,
            2,
            1.0,
            1e-12,
            1e10,
        )
        @test_throws ArgumentError ROAffineInputChart(
            offset,
            jacobian,
            stale_singular_values,
            2,
            1.0,
            1e-12,
            1e10,
            stale_seal,
            Val(:validated),
        )
        @test_throws ArgumentError ROAffineInputChart(
            offset,
            jacobian,
            ones(2),
            2,
            1.0,
            1e-12,
            1e10,
            "0"^64,
            Val(:validated),
        )
    end

    @testset "symmetric analytic chart and rotated controls" begin
        # At tA=tL=Kd=1 for A+L<->AL, both total derivatives of log(AL)
        # equal (5+sqrt(5))/10. Correlated sum/difference controls therefore
        # produce one doubled component and one exactly cancelled component.
        r = (5 + sqrt(5)) / 10
        source_ro = reshape([r, r, -inv(sqrt(5))], 1, 3)
        sum_difference = [
            1.0  1.0
            1.0 -1.0
            0.0  0.0
        ]
        chart = ROAffineInputChart(zeros(3), sum_difference)
        pulled = pullback_ro_matrix(chart, source_ro)
        @test pulled[1, 1] ≈ (5 + sqrt(5)) / 5 rtol=1e-14
        @test pulled[1, 2] ≈ 0.0 atol=1e-15
        @test map_ro_source_coordinates(chart, [2.0, 0.5]) ==
            [2.5, 1.5, 0.0]

        rotation = [1.0 -1.0; 1.0 1.0] / sqrt(2)
        rotated = ROAffineInputChart([0.25, -0.5], rotation)
        matrix = [2.0 3.0; -1.0 4.0]
        @test pullback_ro_matrix(rotated, matrix) ≈ matrix * rotation rtol=1e-14
        @test map_ro_source_coordinates(rotated, [sqrt(2), 0.0]) ≈
            [1.25, 0.5] rtol=1e-14
    end

    @testset "arbitrary final-axis tensor pullback parity" begin
        source_jacobian = [
            1.0  0.0
            0.0  1.0
            1.0 -1.0
            0.5  2.0
        ]
        chart = ROAffineInputChart(zeros(4), source_jacobian)
        source_tensor = reshape(Float64.(1:48), 2, 3, 2, 4)
        pulled = pullback_ro_tensor(chart, source_tensor)
        @test size(pulled) == (2, 3, 2, 2)
        for index in CartesianIndices((2, 3, 2))
            leading = Tuple(index)
            expected = vec(source_tensor[leading..., :])' * source_jacobian
            @test vec(pulled[leading..., :]) ≈ vec(expected) rtol=1e-14
        end

        source_vector = [2.0, -1.0, 3.0, 0.5]
        @test pullback_ro_tensor(chart, source_vector) ≈
            vec(source_vector' * source_jacobian) rtol=1e-14
        @test pullback_ro_tensor(
            chart, reshape(source_vector, 1, 1, 4)) ≈
            reshape(source_vector' * source_jacobian, 1, 1, 2) rtol=1e-14
    end

    @testset "rank and conditioning fail closed" begin
        rank_deficient = [1.0 1.0; 2.0 2.0; 0.0 0.0]
        err = try
            ROAffineInputChart(zeros(3), rank_deficient)
            nothing
        catch caught
            caught
        end
        @test err isa ArgumentError
        @test occursin("numerically rank deficient", sprint(showerror, err))

        numerically_rank_deficient = Diagonal([1.0, 1e-13]) |> Matrix
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), numerically_rank_deficient; rank_rtol=1e-12)

        ill_conditioned = Diagonal([1.0, 1e-11]) |> Matrix
        err = try
            ROAffineInputChart(
                zeros(2), ill_conditioned;
                rank_rtol=1e-12,
                max_condition_number=1e10,
            )
            nothing
        catch caught
            caught
        end
        @test err isa ArgumentError
        @test occursin("condition number", sprint(showerror, err))

        admitted = ROAffineInputChart(
            zeros(2), ill_conditioned;
            rank_rtol=1e-12,
            max_condition_number=1e12,
        )
        @test admitted.numerical_rank == 2
        @test admitted.condition_number ≈ 1e11 rtol=1e-14
    end

    @testset "non-finite and shape inputs are rejected" begin
        @test_throws ArgumentError ROAffineInputChart([0.0, Inf], ones(2, 1))
        @test_throws ArgumentError ROAffineInputChart(zeros(2), [1.0; NaN;;])
        @test_throws ArgumentError ROAffineInputChart([false], ones(1, 1))
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), ones(2, 1); rank_rtol=NaN)
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), ones(2, 1); rank_rtol=0.0)
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), ones(2, 1); max_condition_number=Inf)
        @test_throws DimensionMismatch ROAffineInputChart(zeros(2), ones(3, 1))
        @test_throws DimensionMismatch ROAffineInputChart(zeros(1), ones(1, 2))
        @test_throws ArgumentError ROAffineInputChart(Float64[], zeros(0, 0))

        chart = ROAffineInputChart([0.0, 0.0], Matrix{Float64}(I, 2, 2))
        @test_throws DimensionMismatch map_ro_source_coordinates(chart, [1.0])
        @test_throws ArgumentError map_ro_source_coordinates(chart, [1.0, NaN])
        @test_throws ArgumentError map_ro_source_coordinates(chart, [true, false])
        @test_throws DimensionMismatch pullback_ro_matrix(chart, ones(3, 3))
        @test_throws DimensionMismatch pullback_ro_tensor(chart, ones(2, 3, 3))
        @test_throws ArgumentError pullback_ro_matrix(chart, [1.0 NaN])
        @test_throws ArgumentError pullback_ro_tensor(
            chart, reshape([1.0, Inf], 1, 2))
    end
end
