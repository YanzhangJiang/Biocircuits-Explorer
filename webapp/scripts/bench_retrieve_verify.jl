#!/usr/bin/env julia
# Phase-1 DETERMINISTIC BASELINE over the FULL phenotyped corpus, retrieval-first.
# For each benchmark task: (1) RETRIEVE the top-N candidates cheaply from the
# precomputed dataset labels (no solver — rank by the label shape_support for the
# target class); (2) phenotype-VERIFY only the top-K with the exact phenotyper (the
# task's class + reshaped prior) and rank robust-first; report verified
# pass@1/5/20 + oracle calls (= verify count). This is the non-degenerate pass@k the
# 11-network seed pool could not produce. Self-contained: dataset rows carry
# `rules`, so build_model needs no atlas. Supports --task-shard i/n for parallelism.
#
#   julia --sysimage /path/to/bnc_sys.so --project=webapp_hpc \
#     webapp/scripts/bench_retrieve_verify.jl --dataset datasets/latent-atlas-v0 \
#     --tasks benchmarks/tasks --retrieve 80 --verify 20 --K 16 \
#     --out benchmarks/reports/baseline_corpus.json [--task-shard 0/4]
using JSON3, Dates, Statistics
const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
include(joinpath(HERE, "atlas_read.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using .AtlasRead: to_plain
using BindingAndCatalysis: locate_sym_qK

const CLASS_MAP = Dict(
    "monotone_activation"=>:monotone_activation, "activation_with_saturation"=>:monotone_activation,
    "monotone_repression"=>:monotone_repression, "repression_with_floor"=>:monotone_repression,
    "thresholded_activation"=>:thresholded_activation,
    "biphasic_peak"=>:biphasic_peak, "bandpass_with_plateau"=>:bandpass_with_plateau,
    "bandpass_like"=>:bandpass_with_plateau,
    "biphasic_valley"=>:biphasic_valley, "window_repression"=>:biphasic_valley)
rd(x) = (x isa Real && isfinite(x)) ? round(Float64(x); digits=3) : x

function parse_args(a)
    o = Dict{String,Any}("dataset"=>"datasets/latent-atlas-v0", "tasks"=>"benchmarks/tasks",
        "retrieve"=>80, "verify"=>20, "K"=>nothing, "seed"=>nothing, "out"=>"",
        "task_shard"=>"0/1")
    i=1; while i<=length(a)
        x=a[i]
        if     x=="--dataset";     o["dataset"]=a[i+1]; i+=2
        elseif x=="--tasks";       o["tasks"]=a[i+1]; i+=2
        elseif x=="--retrieve";    o["retrieve"]=parse(Int,a[i+1]); i+=2
        elseif x=="--verify";      o["verify"]=parse(Int,a[i+1]); i+=2
        elseif x=="--K";           o["K"]=parse(Int,a[i+1]); i+=2
        elseif x=="--seed";        o["seed"]=parse(Int,a[i+1]); i+=2
        elseif x=="--out";         o["out"]=a[i+1]; i+=2
        elseif x=="--task-shard";  o["task_shard"]=a[i+1]; i+=2
        else error("unknown arg: $x") end
    end; o
end

# ── gate eval against a phenotype VERIFY result (mirrors run_benchmark.jl) ──
function evaluate_gates(bspec, pr, min_rob)
    reasons=String[]; ng=0; st=pr.stats
    medof(m)=(haskey(st,m)&&isfinite(st[m].median)) ? st[m].median : NaN
    qaof(m) =(haskey(st,m)&&isfinite(st[m].q_alpha)) ? st[m].q_alpha : NaN
    ng+=1; pr.shape_support<min_rob && push!(reasons,"shape_support<$min_rob")
    cmin(k,m)=(if haskey(bspec,k); ng+=1; v=qaof(m); (isfinite(v)&&v>=bspec[k])||push!(reasons,"$m.qa<$(bspec[k])") end)
    cmax(k,m)=(if haskey(bspec,k); ng+=1; v=medof(m); (isfinite(v)&&v<=bspec[k])||push!(reasons,"$m.med>$(bspec[k])") end)
    cmin("fall_slope_min",:fall_slope); cmin("plateau_width_log10_input_min",:plateau_width_log10_input)
    cmin("peak_prominence_min",:peak_prominence); cmax("rise_slope_max",:rise_slope); cmax("baseline_return_max",:baseline_return)
    if haskey(bspec,"dynamic_range_log10"); ng+=1; rg=bspec["dynamic_range_log10"]; v=medof(:output_fold_change_log10)
        (isfinite(v)&&rg[1]<=v<=rg[2])||push!(reasons,"fold not in range") end
    design = ng==0 ? 1.0 : 1.0-length(reasons)/ng
    (length(reasons)==0, reasons, design)
end
objective(bspec,pr)=begin st=pr.stats; s=0.0
    q(m)=(haskey(st,m)&&isfinite(st[m].q_alpha)) ? st[m].q_alpha : NaN
    md(m)=(haskey(st,m)&&isfinite(st[m].median)) ? st[m].median : NaN
    for (k,m) in (("fall_slope_min",:fall_slope),("plateau_width_log10_input_min",:plateau_width_log10_input),("peak_prominence_min",:peak_prominence))
        haskey(bspec,k) && (v=q(m); s+= isfinite(v) ? v-bspec[k] : -10.0) end
    for (k,m) in (("rise_slope_max",:rise_slope),("baseline_return_max",:baseline_return))
        haskey(bspec,k) && (v=md(m); s+= isfinite(v) ? bspec[k]-v : -10.0) end
    s end

function main(args)
    o=parse_args(args); dp=PhenotyperPolicy()
    policy=PhenotyperPolicy(; K=something(o["K"],dp.K), seed=something(o["seed"],dp.seed))
    si,ns = (parse(Int,p) for p in split(o["task_shard"],"/"))
    # load dataset labels (retrieval pool)
    ds=joinpath(o["dataset"],"dataset.jsonl"); isfile(ds)||error("no dataset.jsonl in $(o["dataset"])")
    rows=NamedTuple[]
    for line in eachline(ds)
        isempty(strip(line)) && continue; r=JSON3.read(line)
        push!(rows,(network_id=String(r.network_id), rules=String.(r.rules),
            input=String(r.input_symbol), output=String(r.output_symbol),
            n_reactions=Int(r.n_reactions), sf=to_plain(r.shape_fractions)))
    end
    println("corpus: $(length(rows)) labeled slices; verify K=$(policy.K) retrieve=$(o["retrieve"]) verify=$(o["verify"]) shard=$si/$ns")
    taskfiles=sort([joinpath(o["tasks"],f) for f in readdir(o["tasks"]) if endswith(f,".json")])
    modelcache=Dict{String,Any}(); oracle=Ref(0); reports=Any[]; agg=Dict("p1"=>0,"p5"=>0,"p20"=>0,"n"=>0)
    for (ti,tf) in enumerate(taskfiles)
        ((ti-1)%ns==si) || continue
        task=to_plain(JSON3.read(read(tf,String))); bspec=task["behavior_spec"]; cls_s=String(bspec["behavior_class"])
        haskey(CLASS_MAP,cls_s)||error("unknown behavior_class $cls_s in $(basename(tf))")
        cls=CLASS_MAP[cls_s]; clskey=String(cls); maxr=Int(get(bspec,"max_reactions",99))
        kdp=get(bspec,"kd_profile",nothing); prior=PhenotypePipeline.reshape_prior(ParameterPrior(),kdp)
        succ=get(task,"success",Dict()); min_rob=Float64(get(succ,"min_robustness_score",0.2)); min_des=Float64(get(succ,"min_design_score",0.0))
        # RETRIEVE: filter by reaction count, rank by label shape_support for the class
        pool=[r for r in rows if r.n_reactions<=maxr]
        sort!(pool; by=r->-Float64(get(r.sf,clskey,0.0)))
        cands=pool[1:min(o["retrieve"],length(pool))]
        # VERIFY top-K with the exact phenotyper
        verified=NamedTuple[]
        for c in cands[1:min(o["verify"],length(cands))]
            model=get!(modelcache,c.network_id) do
                try; m,=build_model(c.rules,ones(Float64,length(c.rules))); m; catch; nothing end end
            model===nothing && continue
            locate_sym_qK(model,Symbol(c.input))===nothing && continue
            oracle[]+=1
            pr=try phenotype(model; input_sym=Symbol(c.input), output_expr=c.output, prior=prior, policy=policy, target_class=cls)
                catch; (; shape_support=0.0, stats=Dict{Symbol,Any}()) end
            hard,reasons,design=evaluate_gates(bspec,pr,min_rob)
            push!(verified,(network_id=c.network_id, input=c.input, output=c.output,
                ss=pr.shape_support, pass=(hard&&design>=min_des), design=design, obj=objective(bspec,pr), reasons=reasons))
        end
        ranked=sort(verified; by=r->(-r.ss,-r.obj))
        passk(k)=any(r.pass for r in ranked[1:min(k,length(ranked))])
        p1,p5,p20=passk(1),passk(5),passk(20)
        agg["n"]+=1; agg["p1"]+=p1; agg["p5"]+=p5; agg["p20"]+=p20
        push!(reports, Dict("task_id"=>task["task_id"],"behavior_class"=>cls_s,"verified"=>length(verified),
            "pass@1"=>p1,"pass@5"=>p5,"pass@20"=>p20,"best_ss"=>isempty(ranked) ? 0.0 : rd(ranked[1].ss),
            "top"=>[Dict("network_id"=>r.network_id,"in"=>r.input,"out"=>r.output,"ss"=>rd(r.ss),"design"=>rd(r.design),"pass"=>r.pass) for r in ranked[1:min(3,length(ranked))]]))
        println("  $(rpad(task["task_id"],38)) p@1=$p1 p@5=$p5 p@20=$p20  verified=$(length(verified)) best_ss=$(isempty(ranked) ? 0 : rd(ranked[1].ss))")
    end
    n=max(agg["n"],1)
    report=Dict("report_schema"=>"bne-corpus-baseline/v0.1.0","created_at"=>string(now()),
        "phenotyper_version"=>policy.version,"policy"=>Dict("K"=>policy.K,"retrieve"=>o["retrieve"],"verify"=>o["verify"]),
        "task_shard"=>o["task_shard"],"oracle_calls"=>oracle[],
        "aggregate"=>Dict("pass@1"=>rd(agg["p1"]/n),"pass@5"=>rd(agg["p5"]/n),"pass@20"=>rd(agg["p20"]/n),"n_tasks"=>agg["n"]),
        "tasks"=>reports)
    println("\nAGGREGATE pass@1=$(rd(agg["p1"]/n)) pass@5=$(rd(agg["p5"]/n)) pass@20=$(rd(agg["p20"]/n)) over $(agg["n"]) tasks; oracle_calls=$(oracle[])")
    if !isempty(o["out"]); mkpath(dirname(o["out"])); open(o["out"],"w") do io; JSON3.pretty(io,report) end; println("wrote -> $(o["out"])") end
end
main(ARGS)
