module TargetDesignAPITests

using Test
using HTTP
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function design_request(; input_count=1, output_count=1)
    names = ["X", "Y", "Z"][1:input_count]
    return Dict{String,Any}(
        "target" => Dict{String,Any}(
            "schema_version" => "bne-design-target/v1.0.0",
            "description" => "A sampled concentration response",
            "source" => "data",
            "inputs" => [Dict("name" => name, "min" => 0.1,
                "max" => 2.0, "scale" => "linear") for name in names],
            "outputs" => [Dict("name" => "response_$(index)",
                "species" => index == 1 ? "A" : names[index - 1],
                "transform" => "linear", "offset" => 0.0,
                "optimize_offset" => false) for index in 1:output_count],
            "samples" => [Dict("inputs" => fill(x, input_count),
                "outputs" => fill(0.5, output_count), "weight" => 1.0)
                for x in (0.2, 1.0)],
        ),
        "chemistry" => Dict("auxiliary_monomers" => 1,
            "max_complex_size" => 2, "max_reactions" => 48,
            "allow_homomers" => true),
        "optimization" => Dict{String,Any}("epochs" => 2, "restarts" => 1,
            "prune_rounds" => 1, "seed" => 1),
    )
end

function with_design_job_store(f)
    previous = Backend.LOCAL_JOB_STORE_DIR[]
    tasks = Task[]
    mktempdir() do directory
        Backend.LOCAL_JOB_STORE_DIR[] = directory
        try
            f(directory, tasks)
        finally
            for task in tasks
                timedwait(() -> istaskdone(task), 60; pollint=0.01) == :ok ||
                    error("Target design test job did not settle")
                wait(task)
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous
        end
    end
end

@testset "Target-driven design admission and schema dimensions" begin
    request = design_request()
    normalized = Backend.normalize_target_design_request(request)
    @test normalized["target"]["samples"][1]["inputs"] == [0.2]
    @test normalized["optimization"]["optimize_totals"] === true
    image_request = design_request(input_count=2)
    image_request["target"]["source"] = "image"
    image_request["target"]["samples"] = fill(
        first(image_request["target"]["samples"]), 4096)
    image_request["optimization"] = Dict{String,Any}()
    image_request["chemistry"]["auxiliary_monomers"] = 2
    image_request["chemistry"]["max_complex_size"] = 3
    @test length(Backend.normalize_target_design_request(
        image_request)["target"]["samples"]) == 4096
    too_much_work = deepcopy(image_request)
    too_much_work["optimization"] = Dict("epochs" => 2000,
        "restarts" => 8, "prune_rounds" => 12)
    @test_throws ArgumentError Backend.normalize_target_design_request(too_much_work)
    for (inputs, outputs) in ((1, 2), (2, 1), (3, 3))
        target = Backend.normalize_target_design_request(
            design_request(input_count=inputs, output_count=outputs))["target"]
        @test length(first(target["samples"])["inputs"]) == inputs
        @test length(first(target["samples"])["outputs"]) == outputs
    end
    malformed = Function[
        x -> (x["reactions"] = ["A + X <-> AX"]),
        x -> (x["target"]["samples"][1]["inputs"] = [0.2, 0.3]),
        x -> (x["target"]["samples"][1]["outputs"] = [1.0, 2.0]),
        x -> (x["target"]["samples"][1]["inputs"] = [0.0]),
        x -> (x["target"]["samples"][1]["weight"] = -1.0),
        x -> (x["target"]["outputs"][1]["species"] = "Missing"),
        x -> (x["optimization"]["epochs"] = true),
        x -> (x["optimization"]["epochs"] = 2001),
        x -> (x["optimization"]["optimize_totals"] = 1),
        x -> (x["target"]["samples"] = fill(
            first(x["target"]["samples"]), 4097)),
        x -> (x["target"]["validation_samples"] = fill(
            first(x["target"]["samples"]), 4095)),
    ]
    with_design_job_store() do directory, _
        for mutate! in malformed
            invalid = deepcopy(request)
            mutate!(invalid)
            response = Backend.router(HTTP.Request("POST", "/api/v1/design_network",
                [], JSON3.write(invalid)))
            @test response.status in (400, 422)
            @test isempty(readdir(directory))
        end
    end
    for value in (NaN, Inf, -Inf)
        invalid = deepcopy(request)
        invalid["target"]["samples"][1]["outputs"] = [value]
        @test_throws ArgumentError Backend.normalize_target_design_request(invalid)
    end
    @test Backend.router(HTTP.Request("GET", "/api/v1/design_network")).status == 405
    @test Backend.router(HTTP.Request("POST", "/api/design_network", [],
        JSON3.write(request))).status == 404
    @test :handle_design_network ∉ Backend.SYNC_HEAVY_HANDLER_NAMES
    @test_throws ArgumentError Backend.submit_biocircuits_job_from_spec(Dict(
        "kind" => "design_network", "spec" => request,
        "execution" => Dict("mode" => "aws_batch")))
end

@testset "Target design runs, reports progress, and persists selected replay" begin
    request = design_request(input_count=2)
    progress = Any[]
    result = Backend.target_design_from_spec(request;
        job_context=Dict("publish_progress" => item -> push!(progress, deepcopy(item))))
    @test result["status"] == "ok"
    @test result["initial_reaction_count"] > Backend.MAX_SYNC_REACTIONS
    @test result["selected_network"]["status"] == "ok"
    @test all(isfinite, Iterators.flatten(result["selected_network"]["predictions"]))
    @test result["artifact"]["kind"] == "design_network"
    @test result["termination"]["reason"] == (result["target_met"] ? "target_met" : "search_budget_exhausted")
    @test result["termination"]["iterations"] == sum(fit["iterations"] for fit in result["termination"]["fit_stops"])
    @test result["termination"]["epochs_per_fit"] == request["optimization"]["epochs"]
    selected = result["selected_network"]
    ir = Backend.parse_network_ir(selected["network_ir"])
    @test Backend.network_ir_hash(ir) == selected["network_ir_hash"]
    @test Set(item.name for item in ir.species) == Set(selected["species"])
    @test ir.extensions["design_totals"] == selected["totals"]
    @test ir.extensions["design_readouts"] == selected["outputs"]
    @test !isempty(progress)
    @test all(haskey(item, "phase") for item in progress)
    @test first(progress)["phase"] == "constructing"
    @test last(progress)["phase"] == "exporting"
    @test all(phase in [item["phase"] for item in progress]
        for phase in ("initializing", "fit", "checking_fit", "pruning", "prune_refit", "checking_pruned", "pruning_decision", "selecting", "checking_selected"))
    fits = filter(item -> item["phase"] in ("fit", "prune_refit"), progress)
    @test all(item["epochs"] == request["optimization"]["epochs"] for item in fits)
    @test all(item["restarts"] == request["optimization"]["restarts"] for item in fits)
    @test all(item["rmse"] ≈ sqrt(2item["loss"]) && item["best_rmse"] <= item["rmse"] for item in fits)
    @test all(item["prune_round"] == (item["phase"] == "fit" ? 0 : 1) for item in fits)
    @test all(item["initial_reactions"] == result["initial_reaction_count"] for item in fits)
    @test all(item["step"] <= item["epochs"] for item in fits)
    @test filter(item -> item["phase"] == "pruning_decision", progress)[1]["last_pruning"]["accepted"] == result["pruning_history"][1]["accepted"]
    @test !isempty(result["pruning_history"])
    previews = filter(item -> haskey(item, "predictions"), progress)
    @test !isempty(previews)
    @test last(previews)["predictions"] == selected["predictions"]
    @test all(!haskey(item, "predictions") for item in result["optimization_history"])
    samples = request["target"]["samples"]
    weightsum = sum(row["weight"] for row in samples)
    outputs = length(request["target"]["outputs"])
    for item in previews
        @test length(item["predictions"]) == length(samples)
        @test all(length(row) == outputs for row in item["predictions"])
        per_output = [sqrt(sum(samples[i]["weight"] * (item["predictions"][i][o] - samples[i]["outputs"][o])^2
            for i in eachindex(samples)) / weightsum) for o in 1:outputs]
        @test item["per_output_rmse"] ≈ per_output
        @test item["rmse"] ≈ sqrt(sum(abs2, per_output) / outputs)
    end

    with_design_job_store() do _, tasks
        response = Backend.router(HTTP.Request("POST", "/api/v1/design_network",
            ["Content-Type" => "application/json"], JSON3.write(request)))
        @test response.status == 202
        job = JSON3.read(response.body)
        @test job["kind"] == "design_network"
        @test job["executor"] == "local_async"
        id = String(job["job_id"])
        task = lock(Backend.JOBS_LOCK) do
            get(Backend.JOB_TASKS, id, nothing)
        end
        task === nothing || push!(tasks, task)
        @test timedwait(() -> Backend.get_biocircuits_job(id)["status"] in Backend.JOB_TERMINAL_STATUSES,
            60; pollint=0.01) == :ok
        status = Backend.get_biocircuits_job(id)
        @test status["status"] == "succeeded"
        response = Backend.router(HTTP.Request("POST", "/api/v1/jobs/$id/result",
            ["Content-Type" => "application/json"], "{}"))
        @test response.status == 200
        saved = JSON3.read(response.body)["result"]
        @test saved["selected_network"]["predictions"] ==
            result["selected_network"]["predictions"]
        prepared = Backend._prepare_job_spec_and_artifact_identity("design_network", request)
        @test saved["artifact"]["algorithm"]["config_hash"] == prepared.expected_artifact_config_hash
        @test_throws ArgumentError Backend.get_biocircuits_job_result(id;
            user_sub="another-owner")
    end
end

@testset "Reaction-free design remains a valid physical output" begin
    request = design_request()
    request["chemistry"]["auxiliary_monomers"] = 0
    request["chemistry"]["allow_homomers"] = false
    request["target"]["outputs"][1]["species"] = "X"
    for sample in request["target"]["samples"]
        sample["outputs"] = copy(sample["inputs"])
    end
    result = Backend.target_design_from_spec(request)
    @test result["status"] == "ok"
    @test result["target_met"] === true
    selected = result["selected_network"]
    @test isempty(selected["rules"])
    @test all(observed ≈ expected for (observed, expected) in
        zip(selected["predictions"], [[0.2], [1.0]]))
    @test !haskey(selected, "network_ir")
    @test selected["model_handoff"]["available"] === false
end

@testset "Target design cancellation and deadline fail without an artifact" begin
    request = design_request()
    @test_throws Backend.LocalJobCancelled Backend.target_design_from_spec(request;
        cancel_check=() -> throw(Backend.LocalJobCancelled("cancelled-contract")))
    ticks = Ref(0.0)
    @test_throws Backend.TargetDesignDeadlineExceeded Backend.target_design_from_spec(
        request; clock=() -> (ticks[] += 1.0), deadline_seconds=0.5)
    count = Ref(0)
    @test_throws Backend.LocalJobCancelled Backend.target_design_from_spec(request;
        job_context=Dict("publish_progress" => _ -> (count[] += 1)),
        cancel_check=() -> count[] > 0 &&
            throw(Backend.LocalJobCancelled("after-first-progress")))
    @test count[] == 1
    with_design_job_store() do _, tasks
        withenv("BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => "1",
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => "2") do
            held = "target-design-cancellation-gate"
            semaphore = Backend._reserve_local_job_admission!(held)
            Base.acquire(semaphore)
            job = nothing
            try
                job = Backend.submit_biocircuits_job_from_spec(Dict(
                    "kind" => "design_network", "spec" => request,
                    "execution" => Dict("mode" => "local_async")))
                id = String(job["job_id"])
                task = lock(Backend.JOBS_LOCK) do
                    get(Backend.JOB_TASKS, id, nothing)
                end
                task === nothing || push!(tasks, task)
                cancelled = Backend.cancel_biocircuits_job(id)
                @test cancelled["status"] == "cancelled"
                @test cancelled["result_available"] === false
                @test !isfile(Backend._job_result_path(id))
            finally
                Base.release(semaphore)
                Backend._release_local_job_admission!(held)
            end
        end
    end
end

end
