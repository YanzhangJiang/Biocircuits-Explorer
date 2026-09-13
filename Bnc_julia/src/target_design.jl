export design_binding_network, normalize_design_problem, generate_design_network

# Target-driven binding design. The numerical equations and implicit derivative
# follow reproduction/runtime/{source/binding.py,model.py} from the verified
# Logo experiments. Here all dimensions are explicit and readout offsets are
# additive: transform(concentration) + offset. No sparsity/affinity surrogate is
# used: a smaller physical network must survive refitting and cold replay.
struct DesignEquilibriumError <: Exception
    message::String
end
Base.showerror(io::IO, e::DesignEquilibriumError) = print(io, e.message)

struct DesignBindingNetwork
    monomers::Vector{String}
    input_indices::Vector{Int}
    auxiliary_indices::Vector{Int}
    compositions::Matrix{Float64} # complexes × conserved monomers
    parents::Vector{Int}         # 0 means a free-monomer precursor
    added::Vector{Int}
    precursors::Vector{Int}      # free precursor when parent == 0
    names::Vector{String}
    rules::Vector{String}
    path::Matrix{Float64}        # effective formation logKd = path * step logKd
    available_count::Int
end

function _td_number(x, label; low=-Inf, high=Inf)
    x isa Real && !(x isa Bool) && isfinite(x) && low <= x <= high ||
        throw(ArgumentError("$label must be a finite number in [$low, $high]"))
    return Float64(x)
end
function _td_integer(x, label, low, high)
    x isa Integer && !(x isa Bool) && low <= x <= high ||
        throw(ArgumentError("$label must be an integer in [$low, $high]"))
    return Int(x)
end
function _td_bool(x, label)
    x isa Bool || throw(ArgumentError("$label must be true or false"))
    return x
end
function _td_string(x, label; maxbytes=80, identifier=false)
    x isa AbstractString && !isempty(strip(x)) && ncodeunits(x) <= maxbytes ||
        throw(ArgumentError("$label must be nonempty text of at most $maxbytes bytes"))
    value = String(strip(x))
    identifier && !occursin(r"^[A-Za-z][A-Za-z0-9_]{0,39}$", value) &&
        throw(ArgumentError("$label must be a simple chemical identifier"))
    return value
end
function _td_object(x, label)
    x isa AbstractDict || throw(ArgumentError("$label must be an object"))
    return x
end
function _td_keys(x, allowed, label)
    all(key -> key in allowed, keys(x)) || throw(ArgumentError("$label contains an unrecognized field"))
end
function _td_samples(rows, ni, no, label; allow_empty=false)
    rows isa AbstractVector && (allow_empty || !isempty(rows)) ||
        throw(ArgumentError("$label must be a sample array"))
    length(rows) <= 4096 || throw(ArgumentError("at most 4096 samples are supported"))
    return [begin
        _td_object(row, "$label[$i]")
        _td_keys(row, ("inputs", "outputs", "weight"), "$label[$i]")
        xx, yy = get(row, "inputs", nothing), get(row, "outputs", nothing)
        xx isa AbstractVector && length(xx) == ni || throw(ArgumentError("$label[$i].inputs has the wrong dimension"))
        yy isa AbstractVector && length(yy) == no || throw(ArgumentError("$label[$i].outputs has the wrong dimension"))
        Dict{String,Any}("inputs" => [_td_number(v, "sample input"; low=1e-8, high=1e8) for v in xx],
            "outputs" => [_td_number(v, "sample output"; low=-1e8, high=1e8) for v in yy],
            "weight" => _td_number(get(row, "weight", 1.0), "sample weight"; low=1e-12, high=1e12))
    end for (i, row) in enumerate(rows)]
end

"""Validate and normalize the public target/chemistry/optimization JSON contract."""
function normalize_design_problem(target, chemistry=Dict(), optimization=Dict())
    _td_object(target, "target"); _td_object(chemistry, "chemistry"); _td_object(optimization, "optimization")
    _td_keys(target, ("schema_version", "description", "source", "inputs", "outputs", "samples", "validation_samples"), "target")
    _td_keys(optimization, ("epochs", "restarts", "prune_rounds", "learning_rate", "prune_fraction", "prune_tolerance", "max_rmse", "optimize_totals", "seed"), "optimization")
    get(target, "schema_version", "") == "bne-design-target/v1.0.0" ||
        throw(ArgumentError("unsupported target schema_version"))
    ins, outs = get(target, "inputs", nothing), get(target, "outputs", nothing)
    ins isa AbstractVector && 1 <= length(ins) <= 3 || throw(ArgumentError("target needs one to three inputs"))
    outs isa AbstractVector && 1 <= length(outs) <= 3 || throw(ArgumentError("target needs one to three outputs"))
    inputs = [begin
        _td_object(row, "input")
        _td_keys(row, ("name", "min", "max", "scale"), "input")
        lo = _td_number(get(row, "min", nothing), "input min"; low=1e-8, high=1e8)
        hi = _td_number(get(row, "max", nothing), "input max"; low=1e-8, high=1e8)
        lo < hi || throw(ArgumentError("input min must be below max"))
        scale = get(row, "scale", "log")
        scale in ("log", "linear") || throw(ArgumentError("input scale must be log or linear"))
        Dict{String,Any}("name" => _td_string(get(row, "name", nothing), "input name"; identifier=true),
            "min" => lo, "max" => hi, "scale" => scale)
    end for row in ins]
    length(unique(row["name"] for row in inputs)) == length(inputs) || throw(ArgumentError("input names must be unique"))
    outputs = [begin
        _td_object(row, "output")
        _td_keys(row, ("name", "species", "transform", "offset", "optimize_offset", "min", "max"), "output")
        transform = get(row, "transform", "linear")
        transform in ("linear", "log10") || throw(ArgumentError("output transform must be linear or log10"))
        result = Dict{String,Any}("name" => _td_string(get(row, "name", nothing), "output name"),
            "species" => _td_string(get(row, "species", nothing), "output species"; identifier=true),
            "transform" => transform,
            "offset" => _td_number(get(row, "offset", 0.0), "output offset"; low=-8, high=8),
            "optimize_offset" => _td_bool(get(row, "optimize_offset", false), "optimize_offset"))
        for key in ("min", "max")
            haskey(row, key) && (result[key] = _td_number(row[key], "output $key"; low=-1e8, high=1e8))
        end
        haskey(result, "min") && haskey(result, "max") && result["min"] >= result["max"] && throw(ArgumentError("output min must be below max"))
        result
    end for row in outs]
    length(unique(row["name"] for row in outputs)) == length(outputs) || throw(ArgumentError("output names must be unique"))
    description = get(target, "description", "")
    description isa AbstractString && ncodeunits(description) <= 16000 || throw(ArgumentError("description must be text of at most 16000 bytes"))
    source = get(target, "source", "data")
    source in ("curve", "image", "trajectory", "data", "agent") || throw(ArgumentError("unsupported target source"))
    samples = _td_samples(get(target, "samples", nothing), length(ins), length(outs), "samples")
    validation = _td_samples(get(target, "validation_samples", Any[]), length(ins), length(outs), "validation_samples"; allow_empty=true)
    length(samples) + length(validation) <= 4096 || throw(ArgumentError("at most 4096 combined training and validation samples are supported"))
    for row in Iterators.flatten((samples, validation)), (i, value) in enumerate(row["inputs"])
        inputs[i]["min"] <= value <= inputs[i]["max"] || throw(ArgumentError("sample input is outside its declared range"))
    end
    t = Dict{String,Any}("schema_version" => "bne-design-target/v1.0.0", "description" => String(description),
        "source" => source, "inputs" => inputs, "outputs" => outputs, "samples" => samples)
    isempty(validation) || (t["validation_samples"] = validation)
    c = Dict{String,Any}("auxiliary_monomers" => _td_integer(get(chemistry, "auxiliary_monomers", 2), "auxiliary_monomers", 0, 4),
        "max_complex_size" => _td_integer(get(chemistry, "max_complex_size", 3), "max_complex_size", 2, 8),
        "max_reactions" => _td_integer(get(chemistry, "max_reactions", 48), "max_reactions", 1, 256),
        "allow_homomers" => _td_bool(get(chemistry, "allow_homomers", true), "allow_homomers"))
    allowed_chemistry = Set(["auxiliary_monomers", "max_complex_size", "max_reactions", "allow_homomers", "max_copies", "forbidden_complexes", "binding_gates"])
    all(key -> key in allowed_chemistry, keys(chemistry)) || throw(ArgumentError("unrecognized chemistry constraint"))
    if haskey(chemistry, "max_copies")
        counts = _td_object(chemistry["max_copies"], "max_copies")
        c["max_copies"] = Dict(_td_string(name, "max_copies monomer"; identifier=true) => _td_integer(value, "max_copies value", 0, 8) for (name, value) in counts)
    end
    if haskey(chemistry, "forbidden_complexes")
        forbidden = chemistry["forbidden_complexes"]
        forbidden isa AbstractVector && length(forbidden) <= 256 || throw(ArgumentError("forbidden_complexes must be an array with at most 256 species"))
        c["forbidden_complexes"] = unique([_td_string(name, "forbidden complex"; identifier=true) for name in forbidden])
    end
    if haskey(chemistry, "binding_gates")
        gates = chemistry["binding_gates"]
        gates isa AbstractVector && length(gates) <= 24 || throw(ArgumentError("binding_gates must be an array with at most 24 rules"))
        c["binding_gates"] = [begin
            _td_object(gate, "binding gate")
            all(key -> key in ("monomer", "requires", "unless_core_count_at_least"), keys(gate)) || throw(ArgumentError("unrecognized binding gate constraint"))
            requires = _td_object(get(gate, "requires", nothing), "binding gate requires")
            isempty(requires) && throw(ArgumentError("binding gate requires must name at least one monomer"))
            result = Dict{String,Any}("monomer" => _td_string(get(gate, "monomer", nothing), "gated monomer"; identifier=true),
                "requires" => Dict(_td_string(name, "required monomer"; identifier=true) => _td_integer(value, "required count", 1, 8) for (name, value) in requires))
            haskey(gate, "unless_core_count_at_least") && (result["unless_core_count_at_least"] = _td_integer(gate["unless_core_count_at_least"], "unless_core_count_at_least", 0, 8))
            result
        end for gate in gates]
    end
    o = Dict{String,Any}("epochs" => _td_integer(get(optimization, "epochs", 150), "epochs", 1, 2000),
        "restarts" => _td_integer(get(optimization, "restarts", 2), "restarts", 1, 8),
        "prune_rounds" => _td_integer(get(optimization, "prune_rounds", 3), "prune_rounds", 0, 12),
        "learning_rate" => _td_number(get(optimization, "learning_rate", 0.03), "learning_rate"; low=1e-5, high=0.5),
        "prune_fraction" => _td_number(get(optimization, "prune_fraction", 0.2), "prune_fraction"; low=0.01, high=1),
        "prune_tolerance" => _td_number(get(optimization, "prune_tolerance", 0.02), "prune_tolerance"; low=0, high=1),
        "max_rmse" => _td_number(get(optimization, "max_rmse", 0.05), "max_rmse"; low=0, high=1e8),
        "optimize_totals" => _td_bool(get(optimization, "optimize_totals", true), "optimize_totals"),
        "seed" => _td_integer(get(optimization, "seed", 1), "seed", 0, 2147483647))
    network = generate_design_network(t, c)
    work = (length(samples) + length(validation)) * (o["epochs"] + 1) * o["restarts"] * (o["prune_rounds"] + 1) *
        max(1, length(network.rules)) * length(network.monomers)
    work <= 2_000_000_000 || throw(ArgumentError("design computation exceeds the interactive work budget; reduce samples, epochs, restarts, pruning rounds or chemistry size"))
    return (target=t, chemistry=c, optimization=o)
end

_td_name(c, monomers) = join((monomers[i] * (c[i] == 1 ? "" : string(Int(c[i]))) for i in eachindex(c) if c[i] > 0), "_")
function _td_parent(c)
    added = findlast(>(0), c)
    precursor = copy(c); precursor[added] -= 1
    return precursor, added
end
function _td_network(monomers, input_names, compositions, available_count=length(compositions))
    d, r = length(monomers), length(compositions)
    C = r == 0 ? zeros(0, d) : Float64.(reduce(hcat, compositions)')
    names = [_td_name(c, monomers) for c in compositions]
    length(unique(vcat(monomers, names))) == d + r || throw(ArgumentError("generated species names collide with monomer names; rename the input"))
    lookup = Dict(Tuple(c) => i for (i, c) in enumerate(compositions))
    parents, added, precursors, rules = Int[], Int[], Int[], String[]
    path = zeros(r, r)
    for (j, c) in enumerate(compositions)
        p, a = _td_parent(c)
        parent = sum(p) == 1 ? 0 : get(lookup, Tuple(p), -1)
        0 <= parent < j || throw(ArgumentError("network is missing a required precursor"))
        free = parent == 0 ? findfirst(>(0), p) : 0
        push!(parents, parent); push!(added, a); push!(precursors, free)
        push!(rules, "$(parent == 0 ? monomers[free] : names[parent]) + $(monomers[a]) <-> $(names[j])")
        parent > 0 && (path[j, :] .= path[parent, :])
        path[j, j] = 1.0
    end
    input_indices = [something(findfirst(==(name), monomers)) for name in input_names]
    return DesignBindingNetwork(monomers, input_indices, setdiff(1:d, input_indices), C, parents, added, precursors, names, rules, path, available_count)
end

"""Generate target-independent stoichiometry, retaining every required output precursor."""
function generate_design_network(target, chemistry)
    inputs = String[row["name"] for row in target["inputs"]]
    aux = String[]
    for char in 'A':'Z'
        length(aux) >= chemistry["auxiliary_monomers"] && break
        string(char) in inputs || push!(aux, string(char))
    end
    monomers = vcat(aux, inputs); d = length(monomers)
    all_compositions = Vector{Int}[]
    function enumerate!(c, start, left)
        if left == 0
            push!(all_compositions, copy(c)); return
        end
        for atom in start:d
            !chemistry["allow_homomers"] && c[atom] > 0 && continue
            c[atom] += 1; enumerate!(c, atom, left - 1); c[atom] -= 1
        end
    end
    for size in 2:chemistry["max_complex_size"]
        enumerate!(zeros(Int, d), 1, size)
    end
    unconstrained_names = Set(_td_name(c, monomers) for c in all_compositions)
    maxcopies = get(chemistry, "max_copies", Dict())
    all(name -> name in monomers, keys(maxcopies)) || throw(ArgumentError("max_copies refers to an unknown monomer"))
    forbidden = Set(get(chemistry, "forbidden_complexes", String[]))
    issubset(forbidden, unconstrained_names) || throw(ArgumentError("forbidden_complexes refers to an unknown or out-of-range complex"))
    gates = get(chemistry, "binding_gates", [])
    for gate in gates
        gate["monomer"] in monomers && all(name -> name in monomers, keys(gate["requires"])) ||
            throw(ArgumentError("binding gate refers to an unknown monomer"))
    end
    legal, legal_lookup = Vector{Int}[], Set{Tuple}()
    for composition in all_compositions
        any(composition[i] > get(maxcopies, name, chemistry["max_complex_size"]) for (i, name) in enumerate(monomers)) && continue
        _td_name(composition, monomers) in forbidden && continue
        p, added = _td_parent(composition)
        sum(p) > 1 && !(Tuple(p) in legal_lookup) && continue
        gated = false
        for gate in gates
            gate["monomer"] == monomers[added] || continue
            sum(composition[1:length(aux)]) >= get(gate, "unless_core_count_at_least", typemax(Int)) && continue
            if any(composition[something(findfirst(==(name), monomers))] < count for (name, count) in gate["requires"])
                gated = true; break
            end
        end
        gated && continue
        push!(legal, composition); push!(legal_lookup, Tuple(composition))
    end
    all_compositions = legal
    names = Dict(_td_name(c, monomers) => c for c in all_compositions)
    selected = Set{Tuple}()
    function closure(c)
        missing = Tuple[]; p = c
        while sum(p) > 1 && !(Tuple(p) in selected)
            push!(missing, Tuple(p)); p, _ = _td_parent(p)
        end
        return missing
    end
    for output in target["outputs"]
        species = output["species"]
        species in monomers && continue
        haskey(names, species) || throw(ArgumentError("output species '$species' is not in the legal generated chemistry; choose a monomer or a generated complex such as A_X"))
        union!(selected, closure(names[species]))
    end
    cap = chemistry["max_reactions"]
    length(selected) <= cap || throw(ArgumentError("max_reactions is too small to preserve the required output precursor closure"))
    # Breadth first, with a reproducible target-independent shuffle within each
    # size. A cap never leaves dangling complexes or changes their parent path.
    rng = Random.MersenneTwister(72103)
    ranked = Vector{Int}[]
    for size in 2:chemistry["max_complex_size"]
        append!(ranked, Random.shuffle(rng, filter(c -> sum(c) == size, all_compositions)))
    end
    for c in ranked
        missing = closure(c)
        length(selected) + length(missing) <= cap && union!(selected, missing)
    end
    chosen = filter(c -> Tuple(c) in selected, all_compositions)
    return _td_network(monomers, inputs, chosen, length(all_compositions))
end

function _td_state(la, effective, C)
    free = exp.(la)
    complex = exp.(C * la .- log(10.0) .* effective)
    q = free + transpose(C) * complex
    H = Matrix(Diagonal(free)) + transpose(C) * (complex .* C)
    return (free=free, complex=complex, q=q, H=H)
end
function _td_feasible_start(lq, effective, C)
    shift = 0.0
    for j in axes(C, 1)
        exponent = dot(@view(C[j, :]), lq) - log(10.0) * effective[j]
        for i in axes(C, 2)
            C[j, i] > 0 || continue
            shift = max(shift, (exponent + log(C[j, i]) + log(size(C, 1) + 1) - lq[i]) / sum(@view C[j, :]))
        end
    end
    return lq .- shift
end
function _td_spd(H, rhs, q)
    scale = sqrt.(q)
    factor = cholesky(Symmetric(H ./ (scale * transpose(scale))); check=false)
    issuccess(factor) || throw(DesignEquilibriumError("equilibrium sensitivity matrix is not positive definite"))
    return (factor \ (rhs ./ scale)) ./ scale
end
function _td_solve(lq, effective, C; warm=nothing)
    cold = _td_feasible_start(lq, effective, C)
    for initial in (isnothing(warm) ? (cold,) : (warm, cold))
        la = copy(initial)
        for iteration in 1:160
            state = _td_state(la, effective, C)
            residual = log.(state.q) .- lq
            norm = maximum(abs, residual)
            isfinite(norm) || break
            norm < 3e-10 && return (; state..., la=la, residual=norm, iterations=iteration)
            step = try _td_spd(state.H, state.q .* residual, state.q) catch err
                err isa DesignEquilibriumError || rethrow(); break
            end
            all(isfinite, step) || break
            step ./= max(1.0, maximum(abs, step) / 10.0)
            alpha, accepted = 1.0, false
            for _ in 1:35
                trial = la .- alpha .* step
                next = _td_state(trial, effective, C)
                nextnorm = maximum(abs, log.(next.q) .- lq)
                if nextnorm < norm * (1 - 1e-4 * alpha) || nextnorm < 3e-10
                    la = trial; accepted = true; break
                end
                alpha *= 0.5
            end
            accepted || break
        end
    end
    throw(DesignEquilibriumError("equilibrium unresolved after reduced Newton cold/warm solves; no valid network can be reported"))
end

function _td_layout(network, target, optimization)
    r, d = length(network.rules), length(network.monomers)
    total_indices = optimization["optimize_totals"] ? network.auxiliary_indices : Int[]
    offset_indices = findall(row -> row["optimize_offset"], target["outputs"])
    species = vcat(network.monomers, network.names)
    output_indices = [something(findfirst(==(row["species"]), species)) for row in target["outputs"]]
    return (r=r, d=d, total_indices=total_indices, offset_indices=offset_indices, output_indices=output_indices,
        count=r + length(total_indices) + length(offset_indices))
end

function _td_evaluate(network, target, optimization, theta; samples=target["samples"], gradients=true, warm=nothing, cancelled=()->false)
    layout = _td_layout(network, target, optimization)
    (; r, d, total_indices, offset_indices, output_indices) = layout
    length(theta) == layout.count || throw(DimensionMismatch("design parameter vector has wrong length"))
    effective = network.path * theta[1:r]
    totals = ones(d)
    totals[total_indices] .= exp10.(@view theta[r+1:r+length(total_indices)])
    offsets = Float64[row["offset"] for row in target["outputs"]]
    offsets[offset_indices] .= @view theta[r+length(total_indices)+1:end]
    count, no = length(samples), length(output_indices)
    predictions, targets = zeros(count, no), zeros(count, no)
    starts, occupancy = Vector{Vector{Float64}}(undef, count), zeros(r)
    gradient, loss, maxmass, maxaction = zeros(layout.count), 0.0, 0.0, 0.0
    output_mse = zeros(no)
    weightsum = sum(row["weight"] for row in samples)
    for (b, sample) in enumerate(samples)
        cancelled() && throw(InterruptException())
        q = copy(totals); q[network.input_indices] .= sample["inputs"]
        state = _td_solve(log.(q), effective, network.compositions; warm=isnothing(warm) ? nothing : warm[b])
        starts[b] = state.la
        maxmass = max(maxmass, maximum(abs, log10.(state.q) - log10.(q)))
        logspecies = vcat(state.la ./ log(10.0), network.compositions * state.la ./ log(10.0) - effective)
        for j in 1:r
            precursor = network.parents[j] > 0 ? logspecies[d + network.parents[j]] : logspecies[network.precursors[j]]
            maxaction = max(maxaction, abs(precursor + logspecies[network.added[j]] - logspecies[d+j] - theta[j]))
            occupancy[j] += maximum(network.compositions[j, :] .* state.complex[j] ./ q) / count
        end
        rhs, direct = zeros(d), zeros(r)
        w = sample["weight"] / weightsum / no
        for o in 1:no
            k = output_indices[o]
            logreadout = logspecies[k]
            islog = target["outputs"][o]["transform"] == "log10"
            concentration = exp10(logreadout)
            value = (islog ? logreadout : concentration) + offsets[o]
            predictions[b, o] = value; targets[b, o] = sample["outputs"][o]
            residual = value - targets[b, o]
            loss += 0.5 * w * residual^2
            output_mse[o] += sample["weight"] / weightsum * residual^2
            gradients || continue
            delta = w * residual
            multiplier = islog ? 1 / log(10.0) : concentration
            if k <= d
                rhs[k] += delta * multiplier
            else
                j = k - d
                rhs .+= delta * multiplier .* @view(network.compositions[j, :])
                direct .-= delta * multiplier * log(10.0) .* @view(network.path[j, :])
            end
            offset_slot = findfirst(==(o), offset_indices)
            isnothing(offset_slot) || (gradient[r + length(total_indices) + offset_slot] += delta)
        end
        if gradients
            adjoint = _td_spd(state.H, rhs, state.q)
            gradient[1:r] .+= log(10.0) .* transpose(network.path) * (state.complex .* (network.compositions * adjoint)) + direct
            gradient[r+1:r+length(total_indices)] .+= log(10.0) .* q[total_indices] .* adjoint[total_indices]
        end
    end
    isfinite(loss) && all(isfinite, predictions) && all(isfinite, gradient) || throw(DesignEquilibriumError("non-finite design objective"))
    return (loss=loss, rmse=sqrt(2loss), per_output_rmse=sqrt.(output_mse), gradient=gradient, predictions=predictions, targets=targets, starts=starts,
        occupancy=occupancy, totals=totals, offsets=offsets, maxmass=maxmass, maxaction=maxaction)
end

function _td_initial(network, target, optimization, rng; previous=nothing)
    layout = _td_layout(network, target, optimization)
    theta = vcat(2 .* Random.rand(rng, layout.r) .- 1,
        0.6 .* Random.rand(rng, length(layout.total_indices)) .- 0.3,
        Float64[target["outputs"][i]["offset"] for i in layout.offset_indices])
    if previous !== nothing
        oldnetwork, oldtheta = previous
        oldlayout = _td_layout(oldnetwork, target, optimization)
        rulevalues = Dict(zip(oldnetwork.rules, oldtheta[1:oldlayout.r]))
        for (j, rule) in enumerate(network.rules); theta[j] = rulevalues[rule]; end
        totalvalues = Dict(oldnetwork.monomers[i] => oldtheta[oldlayout.r+j] for (j, i) in enumerate(oldlayout.total_indices))
        for (j, i) in enumerate(layout.total_indices); theta[layout.r+j] = totalvalues[network.monomers[i]]; end
        theta[layout.r+length(layout.total_indices)+1:end] .= oldtheta[oldlayout.r+length(oldlayout.total_indices)+1:end]
    end
    return theta
end
function _td_fit(network, target, optimization, initial; phase, restart, prune_round=0, history, callback, cancelled)
    theta, first, second = copy(initial), zeros(length(initial)), zeros(length(initial))
    layout = _td_layout(network, target, optimization)
    lower = vcat(fill(-8.0, layout.r), fill(-4.0, length(layout.total_indices)), fill(-8.0, length(layout.offset_indices)))
    upper = -lower
    besttheta, best, beststep = copy(theta), nothing, 0
    warm = nothing
    last_evaluated_step = 0
    stop_reason = "iteration_limit"
    for step in 0:optimization["epochs"]
        cancelled() && throw(InterruptException())
        evaluation = try
            _td_evaluate(network, target, optimization, theta; warm=warm, cancelled=cancelled)
        catch err
            err isa DesignEquilibriumError && best !== nothing || rethrow()
            state = Dict{String,Any}("phase" => "$(phase)_step_rejected", "step" => step,
                "loss" => best.loss, "rmse" => best.rmse, "best_rmse" => best.rmse,
                "reactions" => layout.r, "restart" => restart, "prune_round" => prune_round,
                "reason" => "invalid equilibrium update; retained the best evaluated checkpoint")
            state["evaluated_step"] = beststep
            push!(history, state); callback === nothing || callback(state, best)
            stop_reason = "invalid_equilibrium_update"
            break
        end
        last_evaluated_step = step
        warm = evaluation.starts
        if best === nothing || evaluation.loss < best.loss
            besttheta, best, beststep = copy(theta), evaluation, step
        end
        state = Dict{String,Any}("phase" => phase, "step" => step, "loss" => evaluation.loss,
            "rmse" => evaluation.rmse, "best_rmse" => best.rmse,
            "reactions" => layout.r, "restart" => restart, "prune_round" => prune_round)
        if step == 0 || step == optimization["epochs"] || step % 5 == 0; push!(history, state); end
        callback === nothing || callback(state, evaluation)
        step == optimization["epochs"] && break
        first .= 0.9 .* first + 0.1 .* evaluation.gradient
        second .= 0.999 .* second + 0.001 .* evaluation.gradient .^ 2
        theta .-= optimization["learning_rate"] .* (first ./ (1 - 0.9^(step+1))) ./ (sqrt.(second ./ (1 - 0.999^(step+1))) .+ 1e-8)
        theta .= clamp.(theta, lower, upper)
    end
    return (theta=besttheta, evaluation=best, step=beststep,
        iterations=last_evaluated_step, stop_reason=stop_reason)
end

function _td_prune(network, target, occupancy, fraction)
    protected = Set(row["species"] for row in target["outputs"])
    parents = Set(filter(>(0), network.parents))
    leaves = [j for j in eachindex(network.rules) if !(j in parents) && !(network.names[j] in protected)]
    isempty(leaves) && return nothing
    sort!(leaves; by=j -> (occupancy[j], network.names[j]))
    delete = Set(first(leaves, min(length(leaves), max(1, ceil(Int, length(network.rules) * fraction)))))
    keep = [j for j in eachindex(network.rules) if !(j in delete)]
    keepmonomers = [i for i in eachindex(network.monomers) if i in network.input_indices || network.monomers[i] in protected || any(network.compositions[j, i] > 0 for j in keep)]
    compositions = [Int.(network.compositions[j, keepmonomers]) for j in keep]
    inputs = network.monomers[network.input_indices]
    return _td_network(network.monomers[keepmonomers], inputs, compositions, network.available_count)
end

_td_rows(matrix) = [collect(@view matrix[i, :]) for i in axes(matrix, 1)]
function _td_replay(network, target, optimization, theta; cancelled)
    train = _td_evaluate(network, target, optimization, theta; gradients=false, cancelled=cancelled)
    validation = isempty(get(target, "validation_samples", [])) ? nothing :
        _td_evaluate(network, target, optimization, theta; samples=target["validation_samples"], gradients=false, cancelled=cancelled)
    metric = max(maximum(train.per_output_rmse), validation === nothing ? 0.0 : maximum(validation.per_output_rmse))
    return (train=train, validation=validation, metric=metric)
end

"""
    design_binding_network(target, chemistry, optimization; callback=nothing, cancelled=()->false)

Automatically construct a precursor-closed network, optimize step log10(Kd),
optional noninput log10 totals and explicitly enabled additive readout offsets,
then prune low-occupancy leaves and refit. Every accepted pruning step is bounded
by the fixed pre-pruning RMSE plus `prune_tolerance`; a met `max_rmse` may not be
lost. Held-out samples participate in acceptance/ranking, but never in gradients.
The selected physical network is replayed cold. This is sampled evidence and a
bounded search, not a global minimality or continuous-domain guarantee.
"""
function design_binding_network(target, chemistry=Dict(), optimization=Dict(); callback=nothing, cancelled=()->false)
    normalized = normalize_design_problem(target, chemistry, optimization)
    target, chemistry, optimization = normalized.target, normalized.chemistry, normalized.optimization
    started = time()
    initial_count = Ref{Union{Nothing,Int}}(nothing)
    last_pruning = Ref{Any}(nothing)
    last_report = Ref(0.0)
    last_stage = Ref(("", 0, 0))
    # Stage notifications are separate from evaluated optimization history:
    # constructing/checking a network must not invent an iteration or loss.
    report = function (state, evaluation=nothing)
        callback === nothing && return nothing
        now = time()
        stage = (String(state["phase"]), get(state, "restart", 0), get(state, "prune_round", 0))
        final_step = get(state, "step", -1) == optimization["epochs"]
        # Materialize all response rows only for a published checkpoint. A
        # preview and its RMSD come from the very same equilibrium evaluation;
        # full curves never accumulate in optimization_history.
        stage == last_stage[] && !final_step && now - last_report[] < 0.25 && return nothing
        payload = merge(Dict{String,Any}(
            "epochs" => optimization["epochs"], "restarts" => optimization["restarts"],
            "prune_rounds" => optimization["prune_rounds"],
            "initial_reactions" => initial_count[], "last_pruning" => last_pruning[],
            "elapsed_seconds" => now - started), state)
        if evaluation !== nothing
            payload["predictions"] = _td_rows(evaluation.predictions)
            payload["rmse"] = evaluation.rmse
            payload["per_output_rmse"] = copy(evaluation.per_output_rmse)
        end
        callback(payload)
        last_report[] = now
        last_stage[] = stage
        return nothing
    end
    report(Dict("phase" => "constructing"))
    initial = generate_design_network(target, chemistry)
    initial_count[] = length(initial.rules)
    history, pruning, warnings = Any[], Any[], String[]
    fit_stops = Any[]
    record_fit = (fit, phase, restart, round) -> push!(fit_stops, Dict(
        "phase" => phase, "restart" => restart, "prune_round" => round,
        "reason" => fit.stop_reason, "iterations" => fit.iterations,
        "best_step" => fit.step))
    initial.available_count > length(initial.rules) && push!(warnings, "The reaction cap selected a reproducible precursor-closed subset of the allowed chemistry.")
    checkpoints = Any[]
    rng = Random.MersenneTwister(optimization["seed"])
    for restart in 1:optimization["restarts"]
        cancelled() && throw(InterruptException())
        network = initial
        last_pruning[] = nothing
        report(Dict("phase" => "initializing", "restart" => restart, "prune_round" => 0,
            "reactions" => length(network.rules)))
        fit = try
            _td_fit(network, target, optimization, _td_initial(network, target, optimization, rng);
                phase="fit", restart=restart, history=history, callback=report, cancelled=cancelled)
        catch err
            err isa DesignEquilibriumError || rethrow()
            push!(warnings, "Restart $restart failed equilibrium validation: $(err.message)"); continue
        end
        record_fit(fit, "fit", restart, 0)
        report(Dict("phase" => "checking_fit", "restart" => restart, "prune_round" => 0,
            "reactions" => length(network.rules), "evaluated_step" => fit.step), fit.evaluation)
        replay = try
            _td_replay(network, target, optimization, fit.theta; cancelled=cancelled)
        catch err
            err isa DesignEquilibriumError || rethrow()
            push!(warnings, "Restart $restart failed cold physical replay: $(err.message)"); continue
        end
        push!(checkpoints, (network=network, theta=fit.theta, replay=replay))
        baseline = replay.metric
        ceiling = baseline + optimization["prune_tolerance"]
        baseline <= optimization["max_rmse"] && (ceiling = min(ceiling, optimization["max_rmse"]))
        for round in 1:optimization["prune_rounds"]
            report(Dict("phase" => "pruning", "restart" => restart, "prune_round" => round,
                "reactions" => length(network.rules), "evaluated_step" => fit.step), replay.train)
            candidate = _td_prune(network, target, replay.train.occupancy, optimization["prune_fraction"])
            candidate === nothing && break
            before = length(network.rules)
            attempt = Dict{String,Any}("before" => before, "after" => length(candidate.rules), "restart" => restart,
                "round" => round, "accepted" => false, "reason" => "", "rmse" => nothing)
            try
                candidatefit = _td_fit(candidate, target, optimization,
                    _td_initial(candidate, target, optimization, rng; previous=(network, fit.theta));
                    phase="prune_refit", restart=restart, prune_round=round, history=history, callback=report, cancelled=cancelled)
                record_fit(candidatefit, "prune_refit", restart, round)
                report(Dict("phase" => "checking_pruned", "restart" => restart, "prune_round" => round,
                    "reactions" => length(candidate.rules), "evaluated_step" => candidatefit.step), candidatefit.evaluation)
                candidatereplay = _td_replay(candidate, target, optimization, candidatefit.theta; cancelled=cancelled)
                accepted = candidatereplay.metric <= ceiling + 1e-12
                attempt["rmse"] = candidatereplay.train.rmse
                attempt["per_output_rmse"] = candidatereplay.train.per_output_rmse
                attempt["selection_rmse"] = candidatereplay.metric
                attempt["validation_rmse"] = candidatereplay.validation === nothing ? nothing : candidatereplay.validation.rmse
                attempt["accepted"] = accepted
                attempt["reason"] = accepted ? "refit and physical replay satisfy the fixed error ceiling" : "refit exceeds the fixed pre-pruning error ceiling"
                attempt["error_ceiling"] = ceiling
                if accepted
                    network, fit, replay = candidate, candidatefit, candidatereplay
                    push!(checkpoints, (network=network, theta=fit.theta, replay=replay))
                    # Once an intermediate network meets the target, subsequent
                    # pruning may not undo that achievement.
                    replay.metric <= optimization["max_rmse"] && (ceiling = min(ceiling, optimization["max_rmse"]))
                end
            catch err
                err isa DesignEquilibriumError || rethrow()
                attempt["reason"] = "refit equilibrium failed: $(err.message)"
            end
            push!(pruning, attempt)
            last_pruning[] = Dict(key => attempt[key] for key in ("before", "after", "accepted", "round", "reason"))
            report(Dict("phase" => "pruning_decision", "restart" => restart, "prune_round" => round,
                "reactions" => length(network.rules), "evaluated_step" => fit.step), replay.train)
            attempt["accepted"] || break
        end
    end
    isempty(checkpoints) && throw(DesignEquilibriumError("all design restarts failed physical equilibrium validation"))
    report(Dict("phase" => "selecting"))
    eligible = filter(checkpoint -> checkpoint.replay.metric <= optimization["max_rmse"], checkpoints)
    target_met = !isempty(eligible)
    pool = target_met ? eligible : checkpoints
    sort!(pool; by=checkpoint -> target_met ? (length(checkpoint.network.rules), length(checkpoint.network.monomers), checkpoint.replay.metric) :
        (checkpoint.replay.metric, length(checkpoint.network.rules), length(checkpoint.network.monomers)))
    selected = first(pool)
    network, theta = selected.network, selected.theta
    report(Dict("phase" => "checking_selected", "reactions" => length(network.rules),
        "rmse" => selected.replay.train.rmse), selected.replay.train)
    replay = _td_replay(network, target, optimization, theta; cancelled=cancelled)
    audit = Dict("max_log10_mass_residual" => max(replay.train.maxmass, replay.validation === nothing ? 0.0 : replay.validation.maxmass),
        "max_stepwise_log10_mass_action_residual" => max(replay.train.maxaction, replay.validation === nothing ? 0.0 : replay.validation.maxaction),
        "cold_replay" => true, "training_samples" => length(target["samples"]),
        "validation_samples" => length(get(target, "validation_samples", [])))
    max(audit["max_log10_mass_residual"], audit["max_stepwise_log10_mass_action_residual"]) < 1e-8 ||
        throw(DesignEquilibriumError("selected network failed the final physical residual audit"))
    outputs = [merge(row, Dict("offset" => replay.train.offsets[i])) for (i, row) in enumerate(target["outputs"])]
    result = Dict{String,Any}("status" => "ok", "target_met" => target_met, "target" => target,
        "selected_network" => Dict{String,Any}("status" => "ok", "rules" => network.rules,
            "kd" => exp10.(theta[1:length(network.rules)]), "totals" => Dict(network.monomers[i] => replay.train.totals[i] for i in network.auxiliary_indices),
            "monomers" => network.monomers, "species" => vcat(network.monomers, network.names),
            "inputs" => target["inputs"], "outputs" => outputs,
            "predictions" => _td_rows(replay.train.predictions), "targets" => _td_rows(replay.train.targets),
            "rmse" => replay.train.rmse, "fit_loss" => replay.train.loss,
            "per_output_rmse" => replay.train.per_output_rmse,
            "selection_rmse" => replay.metric,
            "evidence_tier" => "sampled_equilibrium_replay", "prediction_basis" => "selected_network", "physical_audit" => audit),
        "initial_reaction_count" => length(initial.rules), "final_reaction_count" => length(network.rules),
        "termination" => Dict{String,Any}(
            "reason" => target_met ? "target_met" : "search_budget_exhausted",
            "epochs_per_fit" => optimization["epochs"],
            "initializations" => optimization["restarts"],
            "pruning_round_limit" => optimization["prune_rounds"],
            "iterations" => sum(item["iterations"] for item in fit_stops),
            "fit_stops" => fit_stops),
        "pruning_history" => pruning, "optimization_history" => history,
        "algorithm" => Dict{String,Any}("method" => "reduced_newton_implicit_gradient_prune_refit",
            "step_log10_kd_bounds" => [-8, 8], "noninput_log10_total_bounds" => [-4, 4], "offset_bounds" => [-8, 8],
            "readout" => "transform(concentration) + offset", "loss" => "weighted mean squared residual / 2 across outputs",
            "pruning" => "low-occupancy leaves; protected inputs/outputs and precursor closure; refit and fixed-ceiling replay",
            "selection" => "fewest reactions among sampled targets met; otherwise lowest worst per-output train/validation RMSE",
            "global_minimality_claimed" => false, "chemistry" => chemistry, "optimization" => optimization), "warnings" => warnings)
    if replay.validation !== nothing
        result["validation"] = Dict("rmse" => replay.validation.rmse, "per_output_rmse" => replay.validation.per_output_rmse, "predictions" => _td_rows(replay.validation.predictions), "targets" => _td_rows(replay.validation.targets))
    else
        push!(warnings, "No held-out samples were supplied; target compliance is supported only at the supplied training samples.")
    end
    target_met || push!(warnings, "The design target was not met within the configured search budget.")
    return result
end
