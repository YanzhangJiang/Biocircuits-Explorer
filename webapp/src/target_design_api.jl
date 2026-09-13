# Target-driven design uses the cancellable local job runner. Validation and
# finite work reservation happen before admission or durable job publication.
# In particular this does not inherit the five-reaction exact-regime limit:
# its equilibrium solver works in the free-monomer coordinates.
const TARGET_DESIGN_DEADLINE_SECONDS = 600.0

struct TargetDesignDeadlineExceeded <: Exception end
Base.showerror(io::IO, ::TargetDesignDeadlineExceeded) = print(io,
    "Network design exceeded its 600-second computation deadline. " *
    "Reduce samples, epochs, restarts, or pruning rounds and run again.")

function normalize_target_design_request(raw)
    raw isa AbstractDict || raw isa JSON3.Object || throw(ArgumentError(
        "Network design request must be an object"))
    request = _materialize(raw)
    allowed = Set(("target", "chemistry", "optimization"))
    isempty(setdiff(Set(String.(keys(request))), allowed)) || throw(ArgumentError(
        "Network design request accepts target, chemistry, and optimization only"))
    haskey(request, "target") || throw(ArgumentError("target is required"))
    target, chemistry, optimization = BindingAndCatalysis.normalize_design_problem(
        request["target"], get(request, "chemistry", Dict{String,Any}()),
        get(request, "optimization", Dict{String,Any}()))
    return Dict{String,Any}(
        "target" => target, "chemistry" => chemistry,
        "optimization" => optimization)
end

function _target_design_attach_network_ir!(result, spec)
    selected = result["selected_network"]
    selected["status"] == "ok" || error("Selected design did not pass physical replay")
    audit = selected["physical_audit"]
    audit["cold_replay"] === true || error("Selected design is missing cold replay evidence")
    for key in ("max_log10_mass_residual", "max_stepwise_log10_mass_action_residual")
        value = audit[key]
        value isa Real && isfinite(value) && 0 <= value <= 1e-7 ||
            error("Selected design failed its physical residual audit")
    end
    if isempty(selected["rules"])
        selected["model_handoff"] = Dict("available" => false,
            "reason" => "This reaction-free equilibrium is valid; the current Model Builder requires at least one reaction.")
        return result
    end
    monomers = Set(String.(selected["monomers"]))
    totals = selected["totals"]
    network = NetworkIR(
        label="Target-driven inverse design",
        species=[SpeciesDecl(name=String(name),
            role=name in monomers ? :free : :bound,
            initial_total=get(totals, name, nothing)) for name in selected["species"]],
        reactions=[ReactionDecl(formula=String(rule), kd=Float64(kd))
            for (rule, kd) in zip(selected["rules"], selected["kd"])],
        # A NetworkIR observable expression stays a concentration expression;
        # the readout transformation and fitted additive offset are explicit
        # metadata, never passed to its linear-expression parser as code.
        observables=[ObservableDecl(name=String(output["name"]),
            expression=String(output["species"]),
            metadata=Dict{String,Any}(
                "transform" => output["transform"], "offset" => output["offset"],
                "optimize_offset" => output["optimize_offset"]))
            for output in selected["outputs"]],
        parameter_distributions=[ParameterDistribution(
            symbol="t" * input["name"],
            kind=input["scale"] == "log" ? :loguniform : :uniform,
            log_min=log10(input["min"]), log_max=log10(input["max"]))
            for input in spec["target"]["inputs"]],
        provenance=Provenance(source="design_network",
            notes="Cold sampled equilibrium replay after Kd/total optimization and precursor-safe pruning."),
        extensions=Dict{String,Any}("design_readouts" => selected["outputs"],
            "design_totals" => totals, "design_inputs" => spec["target"]["inputs"]),
    )
    document = network_ir_to_dict(network)
    parse_network_ir(document) # Do not publish a non-round-trippable handoff.
    selected["network_ir"] = document
    selected["network_ir_hash"] = network_ir_hash(network)
    selected["model_handoff"] = Dict("available" => true,
        "build_mode" => "design_equilibrium")
    return result
end

function target_design_from_spec(raw;
        cancel_check::Function=_no_cancel_check,
        job_context=Dict{String,Any}(),
        clock::Function=time,
        deadline_seconds::Real=TARGET_DESIGN_DEADLINE_SECONDS)
    cancel_check()
    spec = normalize_target_design_request(raw)
    started = clock()
    last_yield = Ref(started)
    # Throw the job runner's cancellation exception directly rather than
    # translating cancellation into a numerical failure or partial result.
    checkpoint = function ()
        cancel_check()
        now = clock()
        now - started >= deadline_seconds && throw(TargetDesignDeadlineExceeded())
        # Also serve progress/cancel HTTP requests when Julia has one worker.
        # This checkpoint runs between target evaluations, never inside a solve.
        if now - last_yield[] >= 0.025
            last_yield[] = now
            yield()
            cancel_check()
        end
        return false
    end
    publisher = get(job_context, "publish_progress", nothing)
    callback = function (progress)
        checkpoint()
        publisher === nothing || publisher(progress)
        return nothing
    end
    result = BindingAndCatalysis.design_binding_network(
        spec["target"], spec["chemistry"], spec["optimization"];
        callback=callback, cancelled=checkpoint)
    checkpoint()
    result isa AbstractDict || error("Network design returned an invalid result")
    callback(Dict("phase" => "exporting", "reactions" => result["final_reaction_count"],
        "rmse" => result["selected_network"]["rmse"],
        "predictions" => result["selected_network"]["predictions"],
        "per_output_rmse" => result["selected_network"]["per_output_rmse"]))
    _target_design_attach_network_ir!(result, spec)
    return attach_artifact!(result, "design_network";
        algorithm_name="target_driven_implicit_gradient_prune_refit",
        config=spec, warnings=get(result, "warnings", String[]))
end

function handle_design_network(req)
    return json_response(submit_biocircuits_job_from_spec(
        Dict{String,Any}(
            "kind" => "design_network", "spec" => read_json(req),
            "execution" => Dict("mode" => "local_async"));
        user_sub=_request_user_sub(req)); status=202)
end
