# Detached, resumable, full-scale rebuild-and-compare worker.
# Recomputes every atlas network's records with the MIGRATED engine (new accepted singular-regime sign)
# and diffs vs the frozen OLD atlas (read-only). Combined d<=4 in ONE run (both atlas DBs).
# RESUMABLE: appends one JSON line per finished network to results.jsonl; on restart, skips done networks.
# Writes progress.txt periodically. Runs unattended (nohup); a separate make_report.jl aggregates anytime.
#
# Env: OUTDIR, THREADS(64), PATH_CAP(100000 skip path-enum above old path), BEHAV_CAP(20000 skip behavior
#      recompute above old total_paths), HEARTBEAT(every N networks -> progress.txt).
using BindingAndCatalysis
using SQLite, DBInterface
import Graphs, JSON3
using Base.Threads
include("<redacted-storage>/Biocircuits-Explorer-atlas-mu5/webapp/src/reaction_parser.jl")
using .ReactionParser: build_model

const OUTDIR    = get(ENV, "OUTDIR", "<redacted-storage>/bcx-resync-rebuild")
const PATH_CAP  = parse(Int, get(ENV, "PATH_CAP", "100000"))
const BEHAV_CAP = parse(Int, get(ENV, "BEHAV_CAP", "20000"))
const HEARTBEAT = parse(Int, get(ENV, "HEARTBEAT", "50"))
const DO_BEHAV  = get(ENV, "DO_BEHAV", "1") in ("1","true","yes","on")  # behavior tier hits a shared-backend bug under threads; 0 = structure-only
const DBS = [
    ("atlas_full", "<redacted-storage>/Biocircuits-Explorer-atlas-mu5/atlas_full/atlas.sqlite"),
    ("atlas_d4",   "<redacted-storage>/Biocircuits-Explorer-atlas-mu5/atlas_d4/atlas.sqlite"),
]
mkpath(OUTDIR)
const RESULTS = joinpath(OUTDIR, "results.jsonl")
const PROGRESS = joinpath(OUTDIR, "progress.txt")
const LOGF = joinpath(OUTDIR, "worker.log")
logmsg(s) = open(LOGF,"a") do io; println(io, Dates_now(), "  ", s); end
Dates_now() = string(round(Int, time()))  # epoch secs (Date.now unavailable in some contexts; epoch is fine)
jparse(x) = JSON3.read(x)

# ---- build the combined worklist (lightweight): one entry per (atlas, network) ----
struct Net; atlas::String; db::String; nid::String; d::Int; r::Int; raw_rules::Vector{String}; end
function worklist()
    nets = Net[]
    for (name, db) in DBS
        h = SQLite.DB(db)
        for row in DBInterface.execute(h, "SELECT network_id, base_species_count d, reaction_count r, record_json nj FROM network_entries")
            rr = try String.(jparse(String(row.nj))["raw_rules"]) catch; String[] end
            isempty(rr) && continue
            push!(nets, Net(name, db, String(row.network_id), Int(row.d), Int(row.r), rr))
        end
    end
    return nets
end

# ---- per-network: load its graph-slices + behavior-slices (small fields only) on demand ----
function graph_slices(h, nid)
    out = NamedTuple[]
    for row in DBInterface.execute(h, "SELECT input_symbol, path_count, record_json rj FROM input_graph_slices WHERE network_id = '$(nid)'")
        rec = try jparse(String(row.rj)) catch; nothing end
        rec === nothing && continue
        push!(out, (insym=String(row.input_symbol), old_fv=Int(get(rec,"full_vertex_count",-1)),
                    old_fe=Int(get(rec,"full_edge_count",-1)), old_path=Int(row.path_count)))
    end
    return out
end
function behav_slices(h, nid)
    out = NamedTuple[]
    for row in DBInterface.execute(h, "SELECT input_symbol, output_symbol, total_paths, feasible_paths, record_json rj FROM behavior_slices WHERE network_id = '$(nid)'")
        rec = try jparse(String(row.rj)) catch; nothing end
        rec === nothing && continue
        push!(out, (insym=String(row.input_symbol), outsym=String(row.output_symbol),
                    old_total=Int(row.total_paths), old_feas=Int(row.feasible_paths),
                    old_exact=Int(get(rec,"exact_family_count",-1)),
                    path_scope=String(get(rec,"path_scope","feasible"))))
    end
    return out
end

# ---- compare one network ----
function compare_net(n::Net)
    res = Dict{String,Any}("nid"=>n.nid, "atlas"=>n.atlas, "d"=>n.d, "r"=>n.r,
        "graph_ok"=>0,"graph_bad"=>0,"path_ok"=>0,"path_bad"=>0,"path_skip"=>0,
        "behav_ok"=>0,"behav_bad"=>0,"behav_skip"=>0,"behav_err"=>0,"err"=>"","mm"=>String[])
    local model
    try; model = build_model(n.raw_rules, ones(Float64,length(n.raw_rules)))[1]
    catch e; res["err"]="build:"*sprint(showerror,e)[1:min(end,100)]; return res; end
    h = SQLite.DB(n.db)
    # structure
    for s in graph_slices(h, n.nid)
        try
            g = get_SISO_graph(model, Symbol(s.insym))
            (Graphs.nv(g)==s.old_fv && Graphs.ne(g)==s.old_fe) ? (res["graph_ok"]+=1) :
                (res["graph_bad"]+=1; length(res["mm"])<8 && push!(res["mm"],"GRAPH $(s.insym) v$(Graphs.nv(g))/$(s.old_fv) e$(Graphs.ne(g))/$(s.old_fe)"))
            if s.old_path <= PATH_CAP
                siso = SISOPaths(model, Symbol(s.insym))
                length(siso.rgm_paths)==s.old_path ? (res["path_ok"]+=1) :
                    (res["path_bad"]+=1; length(res["mm"])<8 && push!(res["mm"],"PATH $(s.insym) $(length(siso.rgm_paths))/$(s.old_path)"))
            else; res["path_skip"]+=1; end
        catch e; res["err"] *= " gslice:"*sprint(showerror,e)[1:min(end,60)]; end
    end
    # behavior (best-effort, bounded; disabled when DO_BEHAV=0 due to shared path-condition backend bug under threads)
    for b in (DO_BEHAV ? behav_slices(h, n.nid) : NamedTuple[])
        if b.old_total > BEHAV_CAP; res["behav_skip"]+=1; continue; end
        try
            siso = SISOPaths(model, Symbol(b.insym))
            bf = get_behavior_families(siso; observe_x=Symbol(b.outsym), path_scope=Symbol(b.path_scope), compute_volume=false)
            getc(k) = try (bf isa NamedTuple ? getfield(bf,k) : bf[String(k)]) catch; try bf[k] catch; nothing end end
            nt = getc(:total_paths); nf = getc(:feasible_paths); nx = getc(:exact_family_count)
            if nt===nothing && nf===nothing && nx===nothing
                res["behav_err"]+=1
            else
                okp = (nt===nothing || nt==b.old_total) && (nf===nothing || nf==b.old_feas)
                okx = (nx===nothing || nx==b.old_exact)
                (okp && okx) ? (res["behav_ok"]+=1) :
                    (res["behav_bad"]+=1; length(res["mm"])<12 && push!(res["mm"],"BEHAV $(b.insym)->$(b.outsym) feas $(nf)/$(b.old_feas) exact $(nx)/$(b.old_exact)"))
            end
        catch e; res["behav_err"]+=1; length(res["mm"])<12 && push!(res["mm"],"BEHAVERR $(b.insym)->$(b.outsym):"*sprint(showerror,e)[1:min(end,60)]); end
    end
    try; DBInterface.close!(h); catch; end
    return res
end

# ---- resume + main loop ----
function done_set()
    s = Set{String}()
    isfile(RESULTS) || return s
    for ln in eachline(RESULTS)
        isempty(strip(ln)) && continue
        try; push!(s, String(jparse(ln)["nid"])); catch; end
    end
    return s
end

function main()
    nets = worklist()
    done = done_set()
    todo = [n for n in nets if !(n.nid in done)]
    total = length(nets); already = length(done)
    logmsg("worklist total=$total already_done=$already todo=$(length(todo)) threads=$(nthreads())")
    flock = ReentrantLock()
    counter = Threads.Atomic{Int}(already)
    t0 = time()
    # dynamic work-queue (atomic index) — load-balances the highly variable per-network cost
    qidx = Threads.Atomic{Int}(0); ntodo = length(todo)
    @sync for _ in 1:nthreads()
        Threads.@spawn while true
            i = Threads.atomic_add!(qidx, 1) + 1
            i > ntodo && break
            r = compare_net(todo[i])
            line = sprint(io->JSON3.write(io, r))
            lock(flock) do
                open(RESULTS,"a") do io; println(io, line); end
            end
            c = Threads.atomic_add!(counter, 1) + 1
            if c % HEARTBEAT == 0 || c == total
                el = time()-t0; rate = (c-already)/max(el,1e-9)
                eta = rate>0 ? (total-c)/rate : -1
                open(PROGRESS,"w") do io
                    println(io, "done=$c / $total  (this_run=$(c-already))  elapsed=$(round(el/60,digits=1))min  rate=$(round(rate,digits=2))/s  eta=$(round(eta/3600,digits=2))h")
                end
            end
        end
    end
    open(joinpath(OUTDIR,"DONE.marker"),"w") do io; println(io,"all $total networks processed"); end
    logmsg("ALL DONE total=$total")
    println("ALL DONE")
end
main()
