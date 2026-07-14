using Test
using BindingAndCatalysis

const _VOLUME_TEST_SEED = 0x5eed1234

function _volume_contract_batch_counts(worker_count::Int; sampler::Symbol=:gaussian)
    Cs = Matrix{Float64}[
        [1.0 0.0; 0.0 1.0],
        [-1.0 0.0; 0.0 -1.0],
    ]
    b64 = [zeros(2), zeros(2)]
    return BindingAndCatalysis._volume_batch_counts(
        Cs,
        b64,
        [1, 2];
        sampler=sampler,
        μ=zeros(2),
        σ=1.0,
        log_lower=-2.0,
        log_upper=3.0,
        regime_judge_tol=0.0,
        contain_overlap=true,
        seed=_VOLUME_TEST_SEED,
        sample_start=0,
        sample_count=16_384,
        worker_count=worker_count,
    )
end

@testset "calc_volume sample-index reproducibility" begin
    adversarial_u = BindingAndCatalysis._volume_uniform01(
        UInt64(0x9450c48b3440b1c1), 1, 1)
    @test 0.0 < adversarial_u < 1.0
    @test all(0.0 < BindingAndCatalysis._volume_uniform01(
        UInt64(_VOLUME_TEST_SEED), sample, lane) < 1.0
        for sample in 1:128 for lane in 1:8)

    expected_counts = Dict(
        :gaussian => [4094, 4096],
        :uniform_box => [5931, 2598],
    )
    for sampler in (:gaussian, :uniform_box)
        serial_counts = _volume_contract_batch_counts(1; sampler=sampler)
        @test serial_counts == expected_counts[sampler]
        @test _volume_contract_batch_counts(2; sampler=sampler) == serial_counts
        @test _volume_contract_batch_counts(7; sampler=sampler) == serial_counts

        first_count = BindingAndCatalysis._volume_batch_counts(
            Matrix{Float64}[
                [1.0 0.0; 0.0 1.0],
                [-1.0 0.0; 0.0 -1.0],
            ],
            [zeros(2), zeros(2)],
            [1, 2];
            sampler=sampler,
            μ=zeros(2),
            σ=1.0,
            log_lower=-2.0,
            log_upper=3.0,
            regime_judge_tol=0.0,
            contain_overlap=true,
            seed=_VOLUME_TEST_SEED,
            sample_start=0,
            sample_count=7_000,
            worker_count=3,
        )
        second_count = BindingAndCatalysis._volume_batch_counts(
            Matrix{Float64}[
                [1.0 0.0; 0.0 1.0],
                [-1.0 0.0; 0.0 -1.0],
            ],
            [zeros(2), zeros(2)],
            [1, 2];
            sampler=sampler,
            μ=zeros(2),
            σ=1.0,
            log_lower=-2.0,
            log_upper=3.0,
            regime_judge_tol=0.0,
            contain_overlap=true,
            seed=_VOLUME_TEST_SEED,
            sample_start=7_000,
            sample_count=9_384,
            worker_count=5,
        )
        @test first_count + second_count == serial_counts
    end

    large_center, large_margin = BindingAndCatalysis._volume_wilson_center_margin(
        0, 1_600_000_000, 1.959963984540054)
    @test isfinite(large_center) && large_center > 0.0
    @test isfinite(large_margin) && large_margin > 0.0

    C = reshape([1.0], 1, 1)
    kwargs = (
        sampler=:uniform_box,
        log_lower=0.0,
        log_upper=4.0,
        batch_size=16_384,
        rel_tol=1.0,
        time_limit=Inf,
        seed=_VOLUME_TEST_SEED,
    )
    direct = calc_volume(C, [-1.0]; kwargs...)
    repeated = calc_volume(C, [-1.0]; kwargs...)
    nested = fetch(Threads.@spawn calc_volume(C, [-1.0]; kwargs...))

    @test direct.mean === repeated.mean === nested.mean
    @test direct.var === repeated.var === nested.var
    @test 0.0 <= direct.mean <= 1.0
    @test direct.var >= 0.0
    @test isapprox(direct.mean, 0.75; atol=0.02)

    gaussian_half = calc_volume(C, [0.0];
        sampler=:gaussian,
        batch_size=16_384,
        rel_tol=1.0,
        time_limit=Inf,
        seed=_VOLUME_TEST_SEED)
    @test 0.0 <= gaussian_half.mean <= 1.0
    @test isapprox(gaussian_half.mean, 0.5; atol=0.02)

    changed_seed = calc_volume(C, [-1.0];
        merge(kwargs, (seed=_VOLUME_TEST_SEED + 1,))...)
    @test (changed_seed.mean, changed_seed.var) != (direct.mean, direct.var)

    # Exclusive first-match classification is defined over the complete ordered
    # region list, even after an earlier region meets its own error tolerance.
    # Two identical full-space regions therefore assign all mass to the first.
    full_space = reshape([0.0], 1, 1)
    exclusive = calc_volume(
        [full_space, copy(full_space)],
        [[1.0], [1.0]];
        sampler=:uniform_box,
        log_lower=-1.0,
        log_upper=1.0,
        contain_overlap=false,
        batch_size=1_000,
        rel_tol=0.02,
        abs_tol=0.001,
        time_limit=Inf,
        seed=_VOLUME_TEST_SEED,
    )
    @test exclusive[1].mean > 0.99
    @test exclusive[2].mean < 0.01

    for invalid_time_limit in (-1.0, 0.0, NaN, -Inf)
        @test_throws ArgumentError calc_volume(
            C, [-1.0]; time_limit=invalid_time_limit)
    end
    tiny_time_result = calc_volume(
        C, [0.0]; sampler=:uniform_box, log_lower=-1.0, log_upper=1.0,
        batch_size=1_000, rel_tol=1.0, time_limit=eps(Float64),
        seed=_VOLUME_TEST_SEED)
    @test 0.4 < tiny_time_result.mean < 0.6
    @test tiny_time_result.var > 0.0

    @test_throws ArgumentError calc_volume(reshape([NaN], 1, 1), [0.0])
    @test_throws ArgumentError calc_volume(C, [NaN])
    @test_throws ArgumentError calc_volume(C, [0.0]; μ=[NaN])
    @test_throws ArgumentError calc_volume(C, [0.0]; σ=Inf)
    @test_throws ArgumentError calc_volume(C, [0.0]; regime_judge_tol=NaN)
    @test_throws ArgumentError calc_volume(C, [0.0]; confidence_level=1.0e-300)
    @test_throws ArgumentError calc_volume(
        C, [0.0]; sampler=:uniform_box, log_lower=0.0, log_upper=Inf)
    @test_throws ArgumentError calc_volume(
        C, [0.0]; sampler=:uniform_box,
        log_lower=-floatmax(Float64), log_upper=floatmax(Float64))

    @test_throws ArgumentError BindingAndCatalysis._volume_batch_counts(
        [C], [[0.0]], [1];
        sampler=:uniform_box,
        μ=zeros(1),
        σ=1.0,
        log_lower=0.0,
        log_upper=1.0,
        regime_judge_tol=0.0,
        contain_overlap=true,
        seed=_VOLUME_TEST_SEED,
        sample_start=typemax(Int),
        sample_count=1,
        worker_count=1,
    )

    workspaces = BindingAndCatalysis._volume_workspaces(3, 11, 5, 7)
    @test length(workspaces) == 3
    @test all(length(workspace.counts) == 11 for workspace in workspaces)
    @test all(length(workspace.x) == 5 for workspace in workspaces)
    @test all(length(workspace.y) == 7 for workspace in workspaces)
end

@testset "legacy volume caches are default-configuration only" begin
    same_volume(a, b) = a.mean === b.mean && a.var === b.var

    @test BindingAndCatalysis._volume_request_is_cacheable(false, nothing, (;))
    @test !BindingAndCatalysis._volume_request_is_cacheable(
        false, reshape([1.0], 1, 1), (;))
    @test !BindingAndCatalysis._volume_request_is_cacheable(
        false, nothing, (seed=_VOLUME_TEST_SEED,))

    model = Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :L, :AL],
        q_sym=[:tA, :tL],
        K_sym=[:Kd1],
    )
    find_all_regimes!(model)
    regime_idx = first(get_regimes(model; return_idx=true))
    regime = get_regime(model, regime_idx; inv_info=true)
    sentinel = BindingAndCatalysis.Volume(0.123456789, 0.987654321)
    custom_kwargs = (
        sampler=:uniform_box,
        log_lower=-2.0,
        log_upper=2.0,
        batch_size=1_000,
        rel_tol=1.0,
        abs_tol=0.0,
        time_limit=Inf,
        seed=_VOLUME_TEST_SEED,
    )

    regime.volume = sentinel
    @test isempty(get_volumes(model, Int[]; custom_kwargs...))
    custom_regime = get_volumes(model, [regime_idx]; custom_kwargs...)[1]
    duplicate_custom_regime = get_volumes(
        model, [regime_idx, regime_idx]; custom_kwargs...)
    @test custom_regime.mean != sentinel.mean
    @test length(duplicate_custom_regime) == 2
    @test all(volume -> same_volume(volume, custom_regime), duplicate_custom_regime)
    @test regime.volume.mean == sentinel.mean
    @test get_volumes(model, [regime_idx])[1].mean == sentinel.mean
    @test all(
        volume -> same_volume(volume, sentinel),
        get_volumes(model, [regime_idx, regime_idx]),
    )

    siso = SISOPaths(model, 1)
    siso.path_volume[1] = sentinel
    siso.path_volume_is_calc[1] = true
    @test isempty(get_volumes(siso, Int[]; custom_kwargs...))
    custom_siso = get_volumes(siso, [1]; custom_kwargs...)[1]
    duplicate_custom_siso = get_volumes(siso, [1, 1]; custom_kwargs...)
    @test custom_siso.mean != sentinel.mean
    @test length(duplicate_custom_siso) == 2
    @test all(volume -> same_volume(volume, custom_siso), duplicate_custom_siso)
    @test siso.path_volume[1].mean == sentinel.mean
    @test get_volumes(siso, [1])[1].mean == sentinel.mean
    @test all(volume -> same_volume(volume, sentinel), get_volumes(siso, [1, 1]))

    changes = ChangePaths(model, [:tA])
    changes.path_volume[1] = sentinel
    changes.path_volume_is_calc[1] = true
    @test isempty(get_volumes(changes, Int[]; custom_kwargs...))
    custom_change = get_volumes(changes, [1]; custom_kwargs...)[1]
    duplicate_custom_change = get_volumes(changes, [1, 1]; custom_kwargs...)
    @test custom_change.mean != sentinel.mean
    @test length(duplicate_custom_change) == 2
    @test all(volume -> same_volume(volume, custom_change), duplicate_custom_change)
    @test changes.path_volume[1].mean == sentinel.mean
    @test get_volumes(changes, [1])[1].mean == sentinel.mean
    @test all(volume -> same_volume(volume, sentinel), get_volumes(changes, [1, 1]))

    constraint_C = zeros(1, model.d + model.r)
    constraint_C0 = [1.0]
    @test_throws ErrorException get_volumes(
        changes,
        Int[];
        constraint_C=constraint_C,
        constraint_C0=nothing,
    )
    @test_throws AssertionError get_volumes(
        changes,
        Int[];
        rebase_K=true,
        rebase_mat=reshape([1.0], 1, 1),
    )
    @test isempty(get_volumes(
        changes,
        Int[];
        constraint_C=constraint_C,
        constraint_C0=constraint_C0,
        custom_kwargs...,
    ))
    constrained_single = get_volumes(
        changes,
        [1];
        constraint_C=constraint_C,
        constraint_C0=constraint_C0,
        custom_kwargs...,
    )[1]
    constrained_duplicate = get_volumes(
        changes,
        [1, 1];
        constraint_C=constraint_C,
        constraint_C0=constraint_C0,
        custom_kwargs...,
    )
    @test length(constrained_duplicate) == 2
    @test all(volume -> same_volume(volume, constrained_single), constrained_duplicate)
end
