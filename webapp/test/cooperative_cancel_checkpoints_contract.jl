module CooperativeCancelCheckpointsContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend
const Engine = Backend.BindingAndCatalysis

struct ROPathCancelProbe <: Exception end

@testset "Cooperative local-job cancellation checkpoints" begin
    @testset "atomic token raises the dedicated cancellation signal" begin
        token = Backend.LocalJobCancelToken("token-contract")
        @test Backend._check_cancelled(token) === nothing
        @test Backend._request_cancel!(token) === nothing
        @test_throws Backend.LocalJobCancelled Backend._check_cancelled(token)
    end

    @testset "scheduler stops dispatching at a checkpoint" begin
        checks = Ref(0)
        cancel_check = function ()
            checks[] += 1
            checks[] >= 4 && throw(Backend.LocalJobCancelled("scheduler-contract"))
            return nothing
        end

        @test_throws Backend.LocalJobCancelled Backend._run_network_jobs_parallel(
            identity,
            collect(1:20),
            1;
            cancel_check=cancel_check,
        )
        @test checks[] == 4
    end

    @testset "parallel worker cancellation is not hidden by TaskFailedException" begin
        build = function (job)
            job == 2 && throw(Backend.LocalJobCancelled("parallel-worker-contract"))
            return job
        end
        @test_throws Backend.LocalJobCancelled Backend._run_network_jobs_parallel(
            build,
            collect(1:4),
            2,
        )
    end

    @testset "enumeration and dispatch honor an already-cancelled token" begin
        token = Backend.LocalJobCancelToken("enumeration-contract")
        Backend._request_cancel!(token)
        cancel_check = () -> Backend._check_cancelled(token)

        @test_throws Backend.LocalJobCancelled enumerate_network_specs(
            Backend.AtlasEnumerationSpec(
                mode=:pairwise_binding,
                base_species_counts=[2],
                min_reactions=1,
                max_reactions=1,
            );
            cancel_check=cancel_check,
        )
        @test_throws Backend.LocalJobCancelled Backend._execute_local_job(
            "query_atlas",
            Dict{String, Any}();
            cancel_check=cancel_check,
        )
    end

    @testset "asynchronous placer replay has a separate finite policy" begin
        rules = [
            "A + B <-> AB",
            "A + C <-> AC",
            "A + D <-> AD",
            "A + E <-> AE",
            "A + F <-> AF",
            "A + G <-> AG",
        ]
        kd = ones(6)
        totals = Dict{Symbol, Float64}()

        # Interactive HTTP behavior remains unchanged.
        @test_throws Backend.SyncBudgetExceeded Backend.placer_dose_response(
            rules, kd, totals, :tA, "AB";
            param_min=-2.0, param_max=2.0, n_points=10)

        # ROP local-job replay is not rejected merely because this model has a
        # sixth reaction; it still traverses the job candidate/cost bounds and
        # the finite <=1000-point scan policy.
        replay = Backend.placer_dose_response(
            rules, kd, totals, :tA, "AB";
            param_min=-2.0,
            param_max=2.0,
            n_points=10,
            execution_policy=:asynchronous,
        )
        @test length(replay["valid"]) == 10
        @test replay["partial"] === false
        @test all(replay["valid"])
        @test_throws ArgumentError Backend.placer_dose_response(
            rules, kd, totals, :tA, "AB";
            n_points=1_001,
            execution_policy=:asynchronous,
        )
    end

    @testset "engine cancellation keeps default parallel policy and typed errors" begin
        @test Backend._no_cancel_check === Engine._NO_CANCEL_CHECK
        @test Engine._use_parallel_ro_paths(Backend._no_cancel_check)
        @test Engine._use_parallel_ro_paths(nothing)
        @test !Engine._use_parallel_ro_paths(() -> nothing)

        model, _, _, _ = Backend.build_model(
            ["A + B <-> AB"], [1.0])
        siso = Engine.SISOPaths(model, :tA)
        pre_path_checks = sum(length, siso.rgm_paths) +
            length(unique(vcat(siso.rgm_paths...)))
        ro_checks = Ref(0)
        @test_throws ROPathCancelProbe Engine.get_RO_paths(
            siso;
            observe_x=:AB,
            cancel_check=() -> begin
                ro_checks[] += 1
                ro_checks[] == pre_path_checks + 1 &&
                    throw(ROPathCancelProbe())
            end,
        )
        @test ro_checks[] == pre_path_checks + 1

        node_checks = Ref(0)
        @test_throws ROPathCancelProbe Engine._calc_RO_for_single_path(
            model, fill(1, 600), 1, 1;
            cancel_check=() -> begin
                node_checks[] += 1
                node_checks[] == 3 && throw(ROPathCancelProbe())
            end,
        )
        @test node_checks[] == 3

        change_paths = Engine.ChangePaths(model, [:tA, :tB])
        @test_throws ROPathCancelProbe Engine.get_RO_paths(
            change_paths;
            observe_x=:AB,
            cancel_check=() -> throw(ROPathCancelProbe()),
        )
        hard_error = try
            Backend._hard_bounded_change_paths(
                model, [:tA, :tB]; max_paths=1,
                max_total_nodes=Backend.MAX_WEB_MATERIALIZED_PATH_NODES)
            nothing
        catch err
            err
        end
        @test hard_error isa ArgumentError
        @test occursin("materialization hard bound",
                       sprint(showerror, hard_error))

        change_spec = Dict{String, Any}(
            "kind" => "orthant",
            "qk_symbols" => ["tA", "tB"],
            "qk_signs" => [1, 1],
            "label" => "+tA,+tB",
        )
        @test_throws ROPathCancelProbe Backend._build_change_paths(
            model, change_spec;
            cancel_check=() -> throw(ROPathCancelProbe()),
        )
    end
end

end # module
