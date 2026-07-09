module CooperativeCancelCheckpointsContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

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
end

end # module
