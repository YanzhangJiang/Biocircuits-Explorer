using Test
using LinearAlgebra
using BindingAndCatalysis

struct ROAffineChartCancelProbe <: Exception end

struct ROAffineUnreadArray{T,N} <: AbstractArray{T,N}
    dimensions::NTuple{N,Int}
    reads::Base.RefValue{Int}
    value::T
end

Base.size(array::ROAffineUnreadArray) = array.dimensions
Base.IndexStyle(::Type{<:ROAffineUnreadArray}) = IndexCartesian()

function Base.getindex(array::ROAffineUnreadArray, indices...)
    array.reads[] += 1
    return array.value
end

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

    @testset "axis-bound pullback receipt rejects anonymous permutations" begin
        source_ids = ["theta_A", "theta_B"]
        target_ids = ["dose", "ratio"]
        jacobian = [1.0 1.0; 1.0 -1.0]
        chart = ROAffineInputChart(
            zeros(2), jacobian;
            source_axis_ids=source_ids,
            target_axis_ids=target_ids,
        )
        source_matrix = [2.0 3.0; -1.0 4.0]

        @test chart.axis_identity_scope == :source_and_target_axis_ids_bound
        @test chart.source_axis_ids == source_ids
        @test chart.target_axis_ids == target_ids
        receipt = pullback_ro_matrix(
            chart, source_matrix; source_axis_ids=source_ids)
        @test receipt.source_values == source_matrix
        @test receipt.values == source_matrix * jacobian
        @test receipt.source_axis_ids == source_ids
        @test receipt.target_axis_ids == target_ids
        @test receipt.axis_identity_scope == :source_and_target_axis_ids_bound
        @test BindingAndCatalysis.validate_ro_affine_pullback_result(
            chart, receipt) === receipt
        direct_receipt = ROAffinePullbackResult(
            chart, source_matrix, source_ids)
        @test direct_receipt.values == source_matrix * jacobian

        # The former exported Val(:validated) constructor accepted arbitrary
        # values once the caller recomputed the self-hash. No constructor that
        # accepts caller-supplied result values remains.
        forged_values = copy(receipt.values)
        forged_values[1, 1] += 7.0
        forged_hash = BindingAndCatalysis._ro_chart_pullback_result_sha256(
            source_matrix,
            forged_values,
            source_ids,
            target_ids,
            :source_and_target_axis_ids_bound,
            chart.content_sha256,
        )
        @test_throws MethodError ROAffinePullbackResult(
            source_matrix,
            forged_values,
            source_ids,
            target_ids,
            :source_and_target_axis_ids_bound,
            chart.content_sha256,
            forged_hash,
            Val(:validated),
        )

        source_tensor = reshape(Float64.(1:8), 2, 2, 2)
        tensor_receipt = pullback_ro_tensor(
            chart, source_tensor; source_axis_ids=source_ids)
        @test tensor_receipt.source_values == source_tensor
        @test tensor_receipt.values == reshape(
            reshape(source_tensor, 4, 2) * jacobian, 2, 2, 2)
        @test BindingAndCatalysis.validate_ro_affine_pullback_result(
            chart, tensor_receipt) === tensor_receipt

        @test_throws ArgumentError pullback_ro_matrix(
            chart, source_matrix;
            source_axis_ids=reverse(source_ids),
        )
        @test_throws ArgumentError pullback_ro_matrix(
            ROAffineInputChart(zeros(2), jacobian),
            source_matrix;
            source_axis_ids=source_ids,
        )

        # The legacy no-identity entry point remains an explicitly degraded
        # numerical transform and does not return an evidence receipt.
        degraded = ROAffineInputChart(zeros(2), jacobian)
        @test degraded.axis_identity_scope == :dimension_only_numerical_transform
        @test pullback_ro_matrix(degraded, source_matrix) ==
            source_matrix * jacobian

        public_source_ids = chart.source_axis_ids
        public_target_ids = chart.target_axis_ids
        public_source_ids[1] = "mutated"
        public_target_ids[1] = "mutated"
        @test chart.source_axis_ids == source_ids
        @test chart.target_axis_ids == target_ids

        tampered = pullback_ro_matrix(
            chart, source_matrix; source_axis_ids=source_ids)
        getfield(tampered, :values)[1, 1] += 1.0
        @test_throws ArgumentError BindingAndCatalysis.validate_ro_affine_pullback_result(
            chart, tampered)
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

    @testset "affine chart work limits and cooperative cancellation" begin
        @test_throws ArgumentError ROAffineInputChartLimits(
            max_source_coordinates=0)
        @test_throws ArgumentError ROAffineInputChartLimits(
            max_array_elements=true)

        source_limit_error = try
            ROAffineInputChart(
                zeros(3),
                ones(3, 1);
                limits=ROAffineInputChartLimits(max_source_coordinates=2),
            )
            nothing
        catch err
            err
        end
        @test source_limit_error isa ROAffineInputChartLimitExceeded
        @test source_limit_error.phase === :source_coordinates
        @test source_limit_error.requested == 3

        array_limit_error = try
            ROAffineInputChart(
                zeros(2),
                Matrix{Float64}(I, 2, 2);
                limits=ROAffineInputChartLimits(max_array_elements=7),
            )
            nothing
        catch err
            err
        end
        @test array_limit_error isa ROAffineInputChartLimitExceeded
        @test array_limit_error.phase === :array_elements
        @test array_limit_error.requested == 8

        factorization_limit_error = try
            ROAffineInputChart(
                zeros(2),
                Matrix{Float64}(I, 2, 2);
                limits=ROAffineInputChartLimits(max_factorization_work=15),
            )
            nothing
        catch err
            err
        end
        @test factorization_limit_error isa ROAffineInputChartLimitExceeded
        @test factorization_limit_error.phase === :factorization_work
        @test factorization_limit_error.requested == 16

        raw_limits = ROAffineInputChartLimits(max_factorization_work=7)
        raw_offset = zeros(2)
        raw_jacobian = Matrix{Float64}(I, 2, 2)
        raw_singular_values = ones(2)
        raw_seal = BindingAndCatalysis._ro_chart_content_sha256(
            raw_offset,
            raw_jacobian,
            raw_singular_values,
            2,
            1.0,
            1e-12,
            1e10;
            limits=raw_limits,
        )
        raw_limit_error = try
            ROAffineInputChart(
                raw_offset,
                raw_jacobian,
                raw_singular_values,
                2,
                1.0,
                1e-12,
                1e10,
                raw_seal,
                Val(:validated);
                limits=raw_limits,
            )
            nothing
        catch err
            err
        end
        @test raw_limit_error isa ROAffineInputChartLimitExceeded
        @test raw_limit_error.phase === :factorization_work
        @test raw_limit_error.requested == 8

        hash_limit_error = try
            ROAffineInputChart(
                zeros(2),
                Matrix{Float64}(I, 2, 2);
                limits=ROAffineInputChartLimits(max_hash_bytes=555),
            )
            nothing
        catch err
            err
        end
        @test hash_limit_error isa ROAffineInputChartLimitExceeded
        @test hash_limit_error.phase === :hash_bytes
        @test hash_limit_error.requested == 556
        unread_offset_reads = Ref(0)
        unread_jacobian_reads = Ref(0)
        unread_offset = ROAffineUnreadArray(
            (2,), unread_offset_reads, 0.0)
        unread_jacobian = ROAffineUnreadArray(
            (2, 2), unread_jacobian_reads, 1.0)
        unread_chart_hash_error = try
            ROAffineInputChart(
                unread_offset,
                unread_jacobian;
                limits=ROAffineInputChartLimits(max_hash_bytes=555),
            )
            nothing
        catch err
            err
        end
        @test unread_chart_hash_error isa ROAffineInputChartLimitExceeded
        @test unread_chart_hash_error.phase === :hash_bytes
        @test unread_chart_hash_error.requested == 556
        @test unread_offset_reads[] == 0
        @test unread_jacobian_reads[] == 0
        @test ROAffineInputChart(
            zeros(2),
            Matrix{Float64}(I, 2, 2);
            limits=ROAffineInputChartLimits(max_hash_bytes=556),
        ) isa ROAffineInputChart

        chart = ROAffineInputChart(zeros(2), Matrix{Float64}(I, 2, 2))
        @test chart.content_sha256 ==
            "0238d3136705869a307d718d364475e0097668fb61d545567ee49b5990c0ba0a"
        map_limit_error = try
            map_ro_source_coordinates(
                chart,
                ones(2);
                limits=ROAffineInputChartLimits(max_operation_scalars=3),
            )
            nothing
        catch err
            err
        end
        @test map_limit_error isa ROAffineInputChartLimitExceeded
        @test map_limit_error.phase === :operation_scalars
        @test map_limit_error.requested == 4

        wrong_length_control_reads = Ref(0)
        wrong_length_controls = ROAffineUnreadArray(
            (10_000,), wrong_length_control_reads, 1.0)
        @test_throws DimensionMismatch map_ro_source_coordinates(
            chart, wrong_length_controls)
        @test wrong_length_control_reads[] == 0

        pullback_limit_error = try
            pullback_ro_matrix(
                chart,
                ones(2, 2);
                limits=ROAffineInputChartLimits(max_operation_scalars=7),
            )
            nothing
        catch err
            err
        end
        @test pullback_limit_error isa ROAffineInputChartLimitExceeded
        @test pullback_limit_error.phase === :operation_scalars
        @test pullback_limit_error.requested == 8

        bound_chart = ROAffineInputChart(
            zeros(1),
            ones(1, 1);
            source_axis_ids=["s"],
            target_axis_ids=["u"],
        )
        source_values = reshape(ones(4), 4, 1)
        result_payload_error = try
            pullback_ro_matrix(
                bound_chart,
                source_values;
                source_axis_ids=["s"],
                limits=ROAffineInputChartLimits(max_array_elements=7),
            )
            nothing
        catch err
            err
        end
        @test result_payload_error isa ROAffineInputChartLimitExceeded
        @test result_payload_error.phase === :array_elements
        @test result_payload_error.requested == 8

        unread_source_reads = Ref(0)
        unread_source_values = ROAffineUnreadArray(
            (4, 1), unread_source_reads, 1.0)
        direct_result_payload_error = try
            ROAffinePullbackResult(
                bound_chart,
                unread_source_values,
                ["s"];
                limits=ROAffineInputChartLimits(max_array_elements=7),
            )
            nothing
        catch err
            err
        end
        @test direct_result_payload_error isa
            ROAffineInputChartLimitExceeded
        @test direct_result_payload_error.phase === :array_elements
        @test direct_result_payload_error.requested == 8
        @test unread_source_reads[] == 0

        unread_axis_reads = Ref(0)
        unread_axis_ids = ROAffineUnreadArray(
            (3,), unread_axis_reads, "unused")
        @test_throws DimensionMismatch ROAffineInputChart(
            zeros(2),
            Matrix{Float64}(I, 2, 2);
            source_axis_ids=unread_axis_ids,
            target_axis_ids=["u", "v"],
        )
        @test unread_axis_reads[] == 0

        tampered_axis_chart = ROAffineInputChart(
            zeros(1),
            ones(1, 1);
            source_axis_ids=["s"],
            target_axis_ids=["u"],
        )
        push!(getfield(tampered_axis_chart, :source_axis_ids), "extra")
        @test_throws DimensionMismatch tampered_axis_chart.content_sha256

        result_hash_error = try
            pullback_ro_matrix(
                bound_chart,
                source_values;
                source_axis_ids=["s"],
                limits=ROAffineInputChartLimits(max_hash_bytes=861),
            )
            nothing
        catch err
            err
        end
        @test result_hash_error isa ROAffineInputChartLimitExceeded
        @test result_hash_error.phase === :hash_bytes
        @test result_hash_error.requested == 862
        exact_hash_receipt = pullback_ro_matrix(
            bound_chart,
            source_values;
            source_axis_ids=["s"],
            limits=ROAffineInputChartLimits(max_hash_bytes=862),
        )
        @test exact_hash_receipt.values == source_values

        unread_hash_source_reads = Ref(0)
        unread_hash_source = ROAffineUnreadArray(
            (4, 1), unread_hash_source_reads, 1.0)
        unread_hash_error = try
            pullback_ro_matrix(
                bound_chart,
                unread_hash_source;
                source_axis_ids=["s"],
                limits=ROAffineInputChartLimits(max_hash_bytes=861),
            )
            nothing
        catch err
            err
        end
        @test unread_hash_error isa ROAffineInputChartLimitExceeded
        @test unread_hash_error.phase === :hash_bytes
        @test unread_hash_error.requested == 862
        @test unread_hash_source_reads[] == 0

        unread_metadata_source_reads = Ref(0)
        unread_metadata_source = ROAffineUnreadArray(
            (4, 1), unread_metadata_source_reads, 1.0)
        metadata_error = try
            pullback_ro_matrix(
                bound_chart,
                unread_metadata_source;
                source_axis_ids=["s"],
                limits=ROAffineInputChartLimits(max_metadata_bytes=65),
            )
            nothing
        catch err
            err
        end
        @test metadata_error isa ROAffineInputChartLimitExceeded
        @test metadata_error.phase === :metadata_bytes
        @test metadata_error.requested == 66
        @test unread_metadata_source_reads[] == 0

        constructor_checks = Ref(0)
        ROAffineInputChart(
            zeros(2),
            Matrix{Float64}(I, 2, 2);
            cancel_check=() -> (constructor_checks[] += 1),
        )
        constructor_final_checkpoint = constructor_checks[]
        @test constructor_final_checkpoint > 9
        constructor_cancel_checks = Ref(0)
        @test_throws ROAffineChartCancelProbe ROAffineInputChart(
            zeros(2),
            Matrix{Float64}(I, 2, 2);
            cancel_check=() -> begin
                constructor_cancel_checks[] += 1
                constructor_cancel_checks[] == constructor_final_checkpoint &&
                    throw(ROAffineChartCancelProbe())
            end,
        )

        pullback_checks = Ref(0)
        pullback_ro_matrix(
            chart,
            ones(2, 2);
            cancel_check=() -> (pullback_checks[] += 1),
        )
        pullback_final_checkpoint = pullback_checks[]
        @test pullback_final_checkpoint > 1
        pullback_cancel_checks = Ref(0)
        @test_throws ROAffineChartCancelProbe pullback_ro_matrix(
            chart,
            ones(2, 2);
            cancel_check=() -> begin
                pullback_cancel_checks[] += 1
                pullback_cancel_checks[] == pullback_final_checkpoint &&
                    throw(ROAffineChartCancelProbe())
            end,
        )

        receipt = pullback_ro_matrix(
            bound_chart, source_values; source_axis_ids=["s"])
        @test receipt.content_sha256 ==
            "ef413ca047ca115ed44f496e7d9196233599e2c65306ad1410eb383175cd0fc8"
        @test validate_ro_affine_pullback_result(
            bound_chart,
            exact_hash_receipt;
            limits=ROAffineInputChartLimits(max_hash_bytes=862),
        ) === exact_hash_receipt
        validation_hash_error = try
            validate_ro_affine_pullback_result(
                bound_chart,
                exact_hash_receipt;
                limits=ROAffineInputChartLimits(max_hash_bytes=861),
            )
            nothing
        catch err
            err
        end
        @test validation_hash_error isa ROAffineInputChartLimitExceeded
        @test validation_hash_error.phase === :hash_bytes
        @test validation_hash_error.requested == 862

        hash_scaling_limits = ROAffineInputChartLimits(
            max_source_coordinates=100_000,
            max_array_elements=300_000,
            max_hash_bytes=4_000_000,
        )
        small_chart_hash_checks = Ref(0)
        BindingAndCatalysis._ro_chart_content_sha256(
            zeros(1), ones(1, 1), ones(1), 1, 1.0, 1e-12, 1e10;
            limits=hash_scaling_limits,
            cancel_check=() -> (small_chart_hash_checks[] += 1),
        )
        large_chart_hash_checks = Ref(0)
        BindingAndCatalysis._ro_chart_content_sha256(
            zeros(100_000), ones(100_000, 1), ones(1),
            1, 1.0, 1e-12, 1e10;
            limits=hash_scaling_limits,
            cancel_check=() -> (large_chart_hash_checks[] += 1),
        )
        @test large_chart_hash_checks[] > small_chart_hash_checks[]

        small_result_hash_checks = Ref(0)
        BindingAndCatalysis._ro_chart_pullback_result_sha256(
            ones(1, 1), ones(1, 1), ["s"], ["u"],
            :source_and_target_axis_ids_bound,
            getfield(bound_chart, :content_sha256);
            limits=hash_scaling_limits,
            cancel_check=() -> (small_result_hash_checks[] += 1),
        )
        large_result_hash_checks = Ref(0)
        BindingAndCatalysis._ro_chart_pullback_result_sha256(
            ones(100_000, 1), ones(100_000, 1), ["s"], ["u"],
            :source_and_target_axis_ids_bound,
            getfield(bound_chart, :content_sha256);
            limits=hash_scaling_limits,
            cancel_check=() -> (large_result_hash_checks[] += 1),
        )
        @test large_result_hash_checks[] > small_result_hash_checks[]

        small_copy_checks = Ref(0)
        BindingAndCatalysis._ro_chart_cancellable_copy(
            zeros(1), () -> (small_copy_checks[] += 1))
        large_copy_checks = Ref(0)
        BindingAndCatalysis._ro_chart_cancellable_copy(
            zeros(10_000), () -> (large_copy_checks[] += 1))
        @test large_copy_checks[] > small_copy_checks[]
        late_copy_checks = Ref(0)
        @test_throws ROAffineChartCancelProbe begin
            BindingAndCatalysis._ro_chart_cancellable_copy(
                zeros(10_000),
                () -> begin
                    late_copy_checks[] += 1
                    late_copy_checks[] == large_copy_checks[] &&
                        throw(ROAffineChartCancelProbe())
                end,
            )
        end
        @test late_copy_checks[] == large_copy_checks[]

        replay_checks = Ref(0)
        validate_ro_affine_pullback_result(
            bound_chart,
            receipt;
            cancel_check=() -> (replay_checks[] += 1),
        )
        replay_final_checkpoint = replay_checks[]
        @test replay_final_checkpoint > pullback_final_checkpoint
        replay_cancel_checks = Ref(0)
        @test_throws ROAffineChartCancelProbe validate_ro_affine_pullback_result(
            bound_chart,
            receipt;
            cancel_check=() -> begin
                replay_cancel_checks[] += 1
                replay_cancel_checks[] == replay_final_checkpoint &&
                    throw(ROAffineChartCancelProbe())
            end,
        )
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
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), Matrix{Float64}(I, 2, 2);
            source_axis_ids=["a", "a"], target_axis_ids=["u", "v"])
        @test_throws DimensionMismatch ROAffineInputChart(
            zeros(2), Matrix{Float64}(I, 2, 2);
            source_axis_ids=["a"], target_axis_ids=["u", "v"])
        @test_throws ArgumentError ROAffineInputChart(
            zeros(2), Matrix{Float64}(I, 2, 2);
            source_axis_ids=["a", "b"])

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
