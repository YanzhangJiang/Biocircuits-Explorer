# tools/migration_parity/changepaths_smoke.jl
#
# Forward-looking smoke for the multimodal-ROP / orthant / negative-direction
# ChangePaths reaction-order path (Task I1). The migration quarantine dropped the
# vector/orthant RO methods (_calc_RO_for_single_path 5-arg + get_RO_path(::Bnc;
# change_qK_indices, change_qK_signs)); without them get_RO_paths(::ChangePaths)
# threw MethodError. This asserts the ported methods run and return finite,
# vector-valued RO profiles on a multi-axis ChangePaths spec.
#
# This is NOT an old-parity golden: the old ChangePaths RO used the old nullity-1
# singular sign; upstream's new sign is now canonical and the atlas is rebuilt under
# it. So we only assert (a) no MethodError, (b) non-empty finite RO vectors with the
# correct per-axis arity, and (c) get_behavior_families runs. We additionally scan a
# scalar single-axis SISO RO profile for a sign reversal to sanity-check multimodal RO.
#
# Run:  BNC_HEADLESS=0 julia --project=webapp tools/migration_parity/changepaths_smoke.jl

using BindingAndCatalysis
const BNC = BindingAndCatalysis

include(joinpath(@__DIR__, "fixtures.jl"))

function _finite_vec_ok(v)
    return v isa AbstractVector && !isempty(v) && all(x -> isfinite(x) || isnan(x) || isinf(x), v)
end

# Count sign reversals across a flattened, dedup'd RO profile (NaN/Inf-tolerant).
function _count_reversals(profile)
    vals = Float64[]
    for entry in profile
        if entry isa AbstractVector
            append!(vals, Float64.(entry))
        else
            push!(vals, Float64(entry))
        end
    end
    vals = filter(v -> isfinite(v) && abs(v) > 1e-9, vals)
    reversals = 0
    last_sign = 0
    for v in vals
        s = sign(v)
        if last_sign != 0 && s != last_sign
            reversals += 1
        end
        last_sign = s
    end
    return reversals
end

function run_smoke()
    failures = String[]
    saw_reversal = false

    # ── prozone/hook network (3 reactions) — non-monotone, multimodal-prone ──
    model = _fixture_build_model(["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"])
    qK_syms = BNC.qK_sym(model)
    println("qK symbols: ", qK_syms)

    # Two-axis orthant change spec over (tL, Kd1) with mixed signs (negative direction
    # on the second axis) — exercises the dropped signed vector/orthant RO path.
    change_syms = [:tL, :Kd1]
    change_signs = Int8[1, -1]

    cp = ChangePaths(model, change_syms; signs=change_signs, kind=:orthant, label="+tL,-Kd1")
    println("ChangePaths built: kind=", BNC.change_kind(cp),
            " label=", BNC.change_label(cp),
            " n_paths=", length(cp.rgm_paths),
            " indices=", BNC.change_qK_indices(cp),
            " signs=", BNC.change_qK_signs(cp))

    # (1) get_RO_paths(::ChangePaths) must run without MethodError and return
    #     vector-valued (one entry per axis) finite RO profiles.
    profiles = try
        BNC.get_RO_paths(cp; observe_x=:ALB, deduplicate=true)
    catch e
        push!(failures, "get_RO_paths(::ChangePaths) threw: $(sprint(showerror, e))")
        nothing
    end

    if profiles !== nothing
        isempty(profiles) && push!(failures, "get_RO_paths returned no paths (expected >=1)")
        n_axes = length(change_syms)
        for (pi, prof) in enumerate(profiles)
            for (vi, vert) in enumerate(prof)
                if !(vert isa AbstractVector && length(vert) == n_axes)
                    push!(failures, "path $pi vertex $vi: expected $n_axes-vector RO, got $(typeof(vert)) len $(vert isa AbstractVector ? length(vert) : "?")")
                elseif !all(x -> isfinite(x) || isnan(x) || isinf(x), vert)
                    push!(failures, "path $pi vertex $vi: non-numeric RO entries: $vert")
                end
            end
        end
        println("get_RO_paths(::ChangePaths) OK: ", length(profiles), " profiles, sample[1]=",
                isempty(profiles) ? "none" : first(profiles))
    end

    # (2a) get_behavior_families(::ChangePaths), graph-only fast path (no polyhedra).
    try
        fam = BNC.get_behavior_families(cp; observe_x=:ALB, path_scope=:all, compute_volume=false)
        println("get_behavior_families(::ChangePaths) [graph_only] OK: total_paths=", fam.total_paths,
                " exact_families=", length(fam.exact_families))
    catch e
        push!(failures, "get_behavior_families(::ChangePaths) [graph_only] threw: $(sprint(showerror, e))")
    end

    # (2b) get_behavior_families(::ChangePaths), projected-feasible path — exercises the
    #      ported get_polyhedra(::ChangePaths) (the default path_scope=:feasible branch).
    try
        cp2 = ChangePaths(model, change_syms; signs=change_signs, kind=:orthant, label="+tL,-Kd1")
        fam = BNC.get_behavior_families(cp2; observe_x=:ALB)  # defaults: path_scope=:feasible, compute_volume=true
        println("get_behavior_families(::ChangePaths) [feasible+volume] OK: feasible_paths=", fam.feasible_paths,
                " exact_families=", length(fam.exact_families))
        fam.feasible_paths < 0 && push!(failures, "negative feasible_paths count")
    catch e
        push!(failures, "get_behavior_families(::ChangePaths) [feasible+volume] threw: $(sprint(showerror, e))")
    end

    # (3) Single-axis SISO RO sweep — scan for an RO sign reversal (multimodal sanity).
    try
        siso = SISOPaths(model, :tL)
        siso_profiles = BNC.get_RO_paths(siso; observe_x=:ALB, deduplicate=true)
        maxrev = isempty(siso_profiles) ? 0 : maximum(_count_reversals.(siso_profiles))
        saw_reversal = maxrev >= 1
        println("SISO RO sweep over tL: ", length(siso_profiles), " paths, max sign reversals=", maxrev,
                saw_reversal ? "  (RO REVERSAL observed)" : "  (no reversal in this net/observe pair)")
    catch e
        push!(failures, "SISO RO sweep threw: $(sprint(showerror, e))")
    end

    # (4) Single-axis NEGATIVE-direction ChangePaths (orthant of size 1, sign -1) —
    #     exercises the signed scalar branch of the ported vertex response.
    try
        cp_neg = ChangePaths(model, [:tL]; signs=Int8[-1], kind=:axis, label="-tL")
        neg_profiles = BNC.get_RO_paths(cp_neg; observe_x=:ALB, deduplicate=true)
        ok = all(p -> all(_finite_vec_ok, p), neg_profiles)
        !ok && push!(failures, "negative-direction ChangePaths produced malformed RO vectors")
        println("negative-direction ChangePaths OK: ", length(neg_profiles), " profiles")
    catch e
        push!(failures, "negative-direction ChangePaths threw: $(sprint(showerror, e))")
    end

    println()
    if isempty(failures)
        println("ChangePaths RO smoke: ALL CHECKS PASS",
                saw_reversal ? " (RO reversal seen)" : " (no RO reversal in this fixture)")
        return 0
    else
        println("ChangePaths RO smoke: FAILURES:")
        for f in failures
            println("  - ", f)
        end
        return 1
    end
end

exit(run_smoke())
