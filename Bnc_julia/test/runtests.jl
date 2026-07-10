# =============================================================================
# Golden-value / characterization regression suite for the BindingAndCatalysis
# ("verifier = truth") ROP math core.
#
# METHOD: These are CHARACTERIZATION tests. Most numeric literals below were
# captured by *running the engine* (Julia 1.12, homotopy solver) and are frozen
# here as a regression baseline. A silent change to the enumeration, the
# qK->x mapping, the parameter scan, or the volume estimator will trip the
# suite. Where an assertion is *also* validated against an independent hand
# calculation (not just a snapshot), it is marked  # SANITY-CHECKED.
#
# Five reference binding networks spanning the MVP family are tested:
#   1. single        L + A <-> AL
#   2. competitive   L + A <-> AL ; L + B <-> BL
#   3. cooperative   A + L <-> AL ; AL + L <-> AL2     (two-site)
#   4. prozone       L + A <-> AL ; L + B <-> BL ; AL + B <-> ALB   (hook)
#   5. ternary       A + L <-> AL ; AL + B <-> ALB     (sequential)
#
# Models are built from a stoichiometry matrix `N` via the public `Bnc(...)`
# constructor. To keep this suite independent of `webapp/`, the tiny rule->N
# parser from `webapp/src/reaction_parser.jl` (`parse_network_structure`) is
# replicated inline below. The species ordering convention (free species first
# in sorted order, then product/bound species in sorted order) and the
# reactants-minus-products log-space sign convention are kept identical so the
# golden values match what the webapp produces.
#
# ENGINE QUIRK discovered while characterizing (frozen here, not "fixed"):
#   * `find_all_vertices!(model)` MUST be called before any regime / volume
#     query; `get_vertices`, `get_volumes`, `assign_vertex_qK` all rely on the
#     cached enumeration (they call it internally, but we call it explicitly to
#     assert counts).
#   * `scan_parameter_1d`/`scan_parameter_2d` now call `assign_vertex_qK(...;
#     return_idx=true)` so they store the real Int vertex index. (Previously they
#     used the default, which returns a *perm vector*; storing it into a
#     `Vector{Int}` threw and the try/catch silently wrote 0 — so the regime
#     sequence was ALL ZEROS and the phenotyper's regime-transition bracketing
#     dead-ended. We now assert real, assigned regimes with transitions.)
# =============================================================================

using Test
using BindingAndCatalysis
using Graphs

# Keep the heavy progress meters quiet during the suite.
ENV["BNC_NO_PROGRESS"] = "1"

@testset "SISO path enumeration enforces pre-allocation limits" begin
    # Six chained diamonds have 2^6 source-to-sink paths while using only 19
    # vertices. This makes the exponential relationship deterministic without
    # constructing a large graph in the test.
    graph = SimpleDiGraph(19)
    current = 1
    next_vertex = 2
    for _ in 1:6
        left, right, merge = next_vertex, next_vertex + 1, next_vertex + 2
        add_edge!(graph, current, left)
        add_edge!(graph, current, right)
        add_edge!(graph, left, merge)
        add_edge!(graph, right, merge)
        current = merge
        next_vertex += 3
    end

    paths = BindingAndCatalysis._enumerate_paths(
        graph; sources=[1], sinks=[current])
    @test length(paths) == 64
    @test_throws PathEnumerationLimitExceeded BindingAndCatalysis._enumerate_paths(
        graph; sources=[1], sinks=[current], max_paths=32)
    @test_throws PathEnumerationLimitExceeded BindingAndCatalysis._enumerate_paths(
        graph; sources=[1], sinks=[current], max_total_nodes=100)
end

# -----------------------------------------------------------------------------
# Inline replica of webapp ReactionParser.parse_network_structure / build_model.
# (Bnc_julia tests must NOT depend on webapp/.)
# -----------------------------------------------------------------------------
const _ARROW_RE = r"<->|<=>|↔"

function _parse_term(term::AbstractString)
    t = strip(term)
    m = match(r"^([0-9]+)?\s*([A-Za-z_][A-Za-z0-9_]*)$", t)
    m === nothing && error("Bad term: $term")
    coeff = m.captures[1] === nothing ? 1 : parse(Int, m.captures[1])
    return Symbol(m.captures[2]), coeff
end

function _parse_side(side::AbstractString)
    dict = Dict{Symbol,Int}()
    for p in split(side, "+")
        sym, coeff = _parse_term(p)
        dict[sym] = get(dict, sym, 0) + coeff
    end
    return dict
end

function _build_N(rules::Vector{String})
    reactants = Dict{Symbol,Int}[]
    products  = Dict{Symbol,Int}[]
    for rule in rules
        m = match(_ARROW_RE, rule)
        m === nothing && error("Reaction must contain an arrow: $rule")
        left, right = split(rule, m.match)
        push!(reactants, _parse_side(left))
        push!(products, _parse_side(right))
    end
    r = length(rules)

    all_species = Set{Symbol}()
    for rd in reactants; union!(all_species, keys(rd)); end
    for pd in products;  union!(all_species, keys(pd)); end

    prod_set = Set{Symbol}()
    for pd in products; union!(prod_set, keys(pd)); end

    free_syms = sort([s for s in all_species if s ∉ prod_set])  # free species first
    prod_syms = sort([s for s in prod_set])                     # then bound species
    species   = vcat(free_syms, prod_syms)
    n = length(species)
    idx = Dict(s => i for (i, s) in enumerate(species))

    N = zeros(Int, r, n)
    for i in 1:r
        for (s, c) in reactants[i]; N[i, idx[s]] += c; end  # reactants - products
        for (s, c) in products[i];  N[i, idx[s]] -= c; end
    end
    return N, species, free_syms, prod_syms
end

"""Build a Bnc model from `<->` rule strings, mirroring webapp build_model."""
function build_model(rules::Vector{String})
    N, species, free_syms, prod_syms = _build_N(rules)
    r = length(rules)
    x_sym = Symbol.(species)
    q_sym = Symbol.("t" .* String.(free_syms))
    K_sym = Symbol.("Kd" .* string.(1:r))
    model = Bnc(N=N, x_sym=x_sym, q_sym=q_sym, K_sym=K_sym)
    return model, species, free_syms, prod_syms
end

# Small helpers for golden assertions.
const RTOL = 1e-6
approxeq(a, b; rtol=RTOL) = isapprox(a, b; rtol=rtol)

@testset "Julia 1.10-compatible parallel cache scheduling" begin
    completed = Threads.Atomic{Int}(0)
    Threads.@threads :dynamic for _ in 1:64
        Threads.atomic_add!(completed, 1)
    end
    @test completed[] == 64
end

# =============================================================================
@testset "Bnc constructor input invariants" begin
    @test_throws ArgumentError Bnc()

    dependent_N = [1 1 -1; 2 2 -2]
    err = try
        Bnc(N=dependent_N)
        nothing
    catch caught
        caught
    end
    @test err isa ArgumentError
    @test occursin("full row rank", sprint(showerror, err))

    N = reshape([1, 1, -1], 1, 3)
    L = [1 0 1; 0 1 1]
    model = Bnc(N=N, L=L)
    @test model.N == N
    @test model.L == L
    @test Bnc(L=L).N == N

    incompatible_L = [1 0 0; 0 1 0]
    @test_throws ArgumentError Bnc(N=N, L=incompatible_L)
end

# =============================================================================
@testset "calc_volume return and normalization contract" begin
    # On [0, 4], x >= 1 occupies 3/4 of the box.  The dimensional interval
    # length is 3, but calc_volume deliberately returns the normalized 0.75
    # fraction and a Volume object for the single-polyhedron overload.
    C = reshape([1.0], 1, 1)
    vol = calc_volume(C, [-1.0];
        sampler=:uniform_box,
        log_lower=0.0,
        log_upper=4.0,
        batch_size=20_000,
        rel_tol=1.0,
        time_limit=5.0)
    @test vol isa BindingAndCatalysis.Volume
    @test 0.0 <= vol.mean <= 1.0
    @test isapprox(vol.mean, 0.75; atol=0.03)
    @test vol.var >= 0.0
end

# =============================================================================
@testset "BindingAndCatalysis golden-value suite" begin

# -----------------------------------------------------------------------------
@testset "rule->N parser replica (structure sanity)" begin
    # SANITY-CHECKED: hand-derived stoichiometry for L + A <-> AL.
    # Species sorted: free {A,L} then bound {AL}  => [A, L, AL].
    # Row = reactants - products = A:+1, L:+1, AL:-1.
    N, species, free_syms, prod_syms = _build_N(["L + A <-> AL"])
    @test species   == [:A, :L, :AL]
    @test free_syms == [:A, :L]
    @test prod_syms == [:AL]
    @test N == reshape([1 1 -1;], 1, 3)
end

# -----------------------------------------------------------------------------
@testset "1. single  (L + A <-> AL)" begin
    model, species, free_syms, prod_syms = build_model(["L + A <-> AL"])

    # --- structural (exact) ---
    @test species   == [:A, :L, :AL]
    @test (model.n, model.d, model.r) == (3, 2, 1)          # SANITY-CHECKED: n=d+r
    @test model.N == reshape([1 1 -1;], 1, 3)               # SANITY-CHECKED
    @test model.L == [1 0 1; 0 1 1]                         # SANITY-CHECKED conservation: tA=A+AL, tL=L+AL
    # Symbolics.Num vectors must be compared structurally (== returns a symbolic
    # expression, not a Bool); use string form / isequal.
    @test string.(model.q_sym) == ["tA", "tL"]
    @test string.(model.K_sym) == ["Kd1"]

    # UPSTREAM CHANGE (b30087f): find_all_vertices! (alias for find_all_regimes!)
    # now returns `nothing` (mutates model in place) rather than returning the
    # permutation vector. Use n_vertices() to assert the count instead.
    find_all_vertices!(model)
    @test n_vertices(model) == 4                            # characterization snapshot
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 4   # snapshot
    @test length(get_vertices(model; singular=false, return_idx=true)) == 3    # snapshot

    # --- qK2x at fixed log10 points; qK layout = [tA, tL, Kd1] ---
    # SANITY-CHECKED high-ligand limit: tA=1, tL=1e3, Kd=1 => nearly all A bound,
    # so AL/tA -> 1 and free A -> tA*Kd/tL ~ 1e-3.
    x_hi = qK2x(model, log10.([1.0, 1e3, 1.0]); input_logspace=true)
    @test approxeq(x_hi[3], 0.9990000009999981)             # AL  (snapshot)
    @test x_hi[3] / 1.0 > 0.99                              # SANITY-CHECKED bound fraction -> 1
    @test approxeq(x_hi[1], 0.0009999990000020005)          # free A (snapshot)
    @test approxeq(x_hi[2], 999.0009999989993)              # free L (snapshot)

    # SANITY-CHECKED low-ligand limit: tA=1, tL=1e-3, Kd=1. With trace ligand,
    # free L is consumed and A ~ tA, so AL ~ A*L/Kd with L = tL/(1 + A/Kd),
    # giving AL ~ tA*tL/(Kd + tA) = 1e-3/2 = 5e-4 to leading order.
    x_lo = qK2x(model, log10.([1.0, 1e-3, 1.0]); input_logspace=true)
    @test approxeq(x_lo[3], 0.0004998750000073652)          # AL (snapshot)
    @test isapprox(x_lo[3], 1.0 * 1e-3 / (1.0 + 1.0); rtol=1e-3)  # SANITY-CHECKED ~ tA*tL/(Kd+tA)

    # Symmetric point tA=tL=Kd=1 (snapshot; AL solves x^2 form -> ~0.382).
    x_sym = qK2x(model, log10.([1.0, 1.0, 1.0]); input_logspace=true)
    @test approxeq(x_sym[1], 0.6180339887497848)            # A   (snapshot)
    @test approxeq(x_sym[2], 0.6180339887497848)            # L   (snapshot)
    @test approxeq(x_sym[3], 0.38196601124996904)           # AL  (snapshot)
    # SANITY-CHECKED equilibrium relation A*L/AL = Kd = 1 holds at the solved point.
    @test isapprox(x_sym[1] * x_sym[2] / x_sym[3], 1.0; rtol=1e-6)
    # SANITY-CHECKED conservation tA = A + AL = 1.
    @test isapprox(x_sym[1] + x_sym[3], 1.0; rtol=1e-6)

    # qK2x status Ref kwarg (guards the new optional convergence-status feature).
    st = Ref(:pending)
    qK2x(model, log10.([1.0, 1e3, 1.0]); input_logspace=true, status=st)
    @test st[] === :success

    # --- scan_parameter_1d: dose-response sweep of tL ---
    # qK layout [tA, tL, Kd1]; sweep tL (param_idx=2); fixed = [tA, Kd1].
    fixed   = log10.([1.0, 1.0])
    outcoef = [Float64[0, 0, 1]]                            # report AL
    rng     = collect(range(-3.0, 3.0; length=7))
    pr, traj, rgm = scan_parameter_1d(model, 2, rng, outcoef, fixed;
                                      input_logspace=true, output_logspace=true)
    @test pr == rng
    # Regimes are now real vertex indices (return_idx=true fix): every swept point
    # is assigned to a valid vertex (≥1) and the dose-response sweep crosses ≥2
    # regimes (low-occupancy → saturated), instead of the old all-zeros.
    @test all(>=(1), rgm)
    @test length(unique(rgm)) >= 2
    # log10(AL) dose-response trajectory (snapshot, tight tol).
    @test traj[:, 1] ≈ [-3.301138582852023, -2.3021170845024224,
                        -1.3120184288598458, -0.41797528050011223,
                        -0.045284614944816656, -0.004364366809091041,
                        -0.0004345113392893136] rtol=1e-6
    # SANITY-CHECKED monotonicity: more ligand -> more complex.
    @test issorted(traj[:, 1])

    # track_validity=true returns a 4-tuple with an all-true validity vector for
    # this clean scan (guards the new track_validity engine feature).
    out4 = scan_parameter_1d(model, 2, rng, outcoef, fixed;
                             input_logspace=true, output_logspace=true,
                             track_validity=true)
    @test length(out4) == 4
    pr2, traj2, rgm2, valid = out4
    @test valid isa Vector{Bool}
    @test all(valid)
    @test length(valid) == 7
    @test traj2 ≈ traj rtol=1e-6                            # same numbers, extra return value

    # --- locate_sym_qK / parse_linear_combination ---
    @test locate_sym_qK(model, :tA)  == 1                   # SANITY-CHECKED [q;K] layout
    @test locate_sym_qK(model, :tL)  == 2
    @test locate_sym_qK(model, :Kd1) == 3
    @test parse_linear_combination(model, "AL")     == [0.0, 0.0, 1.0]   # SANITY-CHECKED
    @test parse_linear_combination(model, "A + AL") == [1.0, 0.0, 1.0]   # SANITY-CHECKED
    @test_throws ErrorException parse_linear_combination(model, "1e309*AL")
    @test_throws ErrorException parse_linear_combination(model, "1e200*AL")

    # --- calc_volume (Monte-Carlo) on the asymptotic regimes ---
    # Generous ±15% band (header explains the fixed per-thread RNG seed). The
    # three non-singular asymptotic regimes each carry ~1/3 of the gaussian
    # probability mass and the singular one carries ~0; the masses sum to ~1.
    vols  = get_volumes(model; asymptotic=true, rel_tol=0.01, batch_size=50_000)
    means = [v.mean for v in vols]
    @test length(means) == 4
    @test isapprox(sum(means), 1.0; atol=0.05)              # SANITY-CHECKED: probabilities partition the space
    nz = filter(>(0.0), means)
    @test length(nz) == 3                                   # snapshot: 3 non-singular regimes
    for m in nz
        @test isapprox(m, 1/3; rtol=0.15)                  # ±15% band (Monte-Carlo)
    end
end

# -----------------------------------------------------------------------------
@testset "2. competitive  (L+A<->AL ; L+B<->BL)" begin
    model, species, free_syms, prod_syms = build_model(
        ["L + A <-> AL", "L + B <-> BL"])

    @test species == [:A, :B, :L, :AL, :BL]
    @test (model.n, model.d, model.r) == (5, 3, 2)          # SANITY-CHECKED: n=d+r
    @test model.N == [1 0 1 -1 0; 0 1 1 0 -1]               # SANITY-CHECKED
    @test model.L == [1 0 0 1 0; 0 1 0 0 1; 0 0 1 1 1]      # SANITY-CHECKED

    find_all_vertices!(model)
    @test length(model.vertices_perm) == 12                 # snapshot
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 12   # snapshot
    @test length(get_vertices(model; singular=false, return_idx=true)) == 8     # snapshot

    # qK layout = [tA, tB, tL, Kd1, Kd2]; symmetric all-ones point.
    x = qK2x(model, log10.([1.0, 1.0, 1.0, 1.0, 1.0]); input_logspace=true)
    @test approxeq(x[1], 0.7071067811865476)                # A   (snapshot)
    @test approxeq(x[2], 0.7071067811865475)                # B   (snapshot)
    @test approxeq(x[3], 0.4142135623730947)                # L   (snapshot)
    @test approxeq(x[4], 0.2928932188134522)                # AL  (snapshot)
    @test approxeq(x[5], 0.2928932188134522)                # BL  (snapshot)
    # SANITY-CHECKED symmetry: A and B (and AL and BL) are interchangeable here.
    @test isapprox(x[1], x[2]; rtol=1e-6)
    @test isapprox(x[4], x[5]; rtol=1e-6)
    # SANITY-CHECKED conservation tA = A + AL = 1.
    @test isapprox(x[1] + x[4], 1.0; rtol=1e-6)

    # scan tL (param_idx=3); fixed = [tA, tB, Kd1, Kd2].
    fixedc = log10.([1.0, 1.0, 1.0, 1.0])
    outc   = [Float64[0, 0, 0, 1, 0]]                       # AL
    rngc   = collect(range(-2.0, 2.0; length=5))
    prc, trajc, rgmc, validc = scan_parameter_1d(model, 3, rngc, outc, fixedc;
        input_logspace=true, output_logspace=true, track_validity=true)
    @test all(>=(1), rgmc)                                  # real vertex indices (was all-zeros)
    @test validc isa Vector{Bool}
    @test all(validc)                                       # clean scan -> all converged
    @test issorted(trajc[:, 1])                             # SANITY-CHECKED dose-response monotone

    # 2D scans expose the same convergence contract when requested and prewarm
    # mutable regime caches before row-level threading begins.
    grid_rng = collect(range(-1.0, 1.0; length=3))
    fixed2d = log10.([1.0, 1.0, 1.0])                      # [tL, Kd1, Kd2]
    _, _, grid2d, regimes2d, valid2d = scan_parameter_2d(
        model, 1, 2, grid_rng, grid_rng, outc[1], fixed2d;
        input_logspace=true, output_logspace=true, track_validity=true)
    @test size(grid2d) == size(regimes2d) == size(valid2d) == (3, 3)
    @test all(valid2d)
end

# -----------------------------------------------------------------------------
@testset "3. cooperative / two-site  (A+L<->AL ; AL+L<->AL2)" begin
    model, species, free_syms, prod_syms = build_model(
        ["A + L <-> AL", "AL + L <-> AL2"])

    @test species == [:A, :L, :AL, :AL2]
    @test (model.n, model.d, model.r) == (4, 2, 2)          # SANITY-CHECKED: n=d+r
    @test model.N == [1 1 -1 0; 0 1 1 -1]                   # SANITY-CHECKED
    @test model.L == [1 0 1 1; 0 1 1 2]                     # SANITY-CHECKED (AL2 holds 1 A, 2 L)

    find_all_vertices!(model)
    @test length(model.vertices_perm) == 8                  # snapshot
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 7    # snapshot
    @test length(get_vertices(model; singular=false, return_idx=true)) == 6     # snapshot

    # qK layout = [tA, tL, Kd1, Kd2]; point tA=1, tL=10, Kd1=Kd2=1.
    x = qK2x(model, log10.([1.0, 10.0, 1.0, 1.0]); input_logspace=true)
    @test approxeq(x[1], 0.013279163242026555)              # A    (snapshot)
    @test approxeq(x[2], 8.13457872528219)                  # L    (snapshot)
    @test approxeq(x[3], 0.1080203987981385)                # AL   (snapshot)
    @test approxeq(x[4], 0.8787004379598352)                # AL2  (snapshot)
    # SANITY-CHECKED conservation tA = A + AL + AL2 = 1.
    @test isapprox(x[1] + x[3] + x[4], 1.0; rtol=1e-6)
    # SANITY-CHECKED conservation tL = L + AL + 2*AL2 = 10.
    @test isapprox(x[2] + x[3] + 2 * x[4], 10.0; rtol=1e-6)
    # SANITY-CHECKED equilibria: A*L/AL = Kd1 = 1 and AL*L/AL2 = Kd2 = 1.
    @test isapprox(x[1] * x[2] / x[3], 1.0; rtol=1e-6)
    @test isapprox(x[3] * x[2] / x[4], 1.0; rtol=1e-6)
end

# -----------------------------------------------------------------------------
@testset "4. prozone / hook  (L+A<->AL ; L+B<->BL ; AL+B<->ALB)" begin
    model, species, free_syms, prod_syms = build_model(
        ["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"])

    @test species == [:A, :B, :L, :AL, :ALB, :BL]
    @test (model.n, model.d, model.r) == (6, 3, 3)          # SANITY-CHECKED: n=d+r
    @test model.N == [1 0 1 -1 0 0; 0 1 1 0 0 -1; 0 1 0 1 -1 0]   # SANITY-CHECKED
    @test model.L == [1 0 0 1 1 0; 0 1 0 0 1 1; 0 0 1 1 1 1]      # SANITY-CHECKED

    find_all_vertices!(model)
    @test length(model.vertices_perm) == 25                 # snapshot
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 25   # snapshot
    @test length(get_vertices(model; singular=false, return_idx=true)) == 15    # snapshot

    # qK layout = [tA, tB, tL, Kd1, Kd2, Kd3]; verify a solved point obeys the
    # conservation + equilibrium laws (SANITY-CHECKED rather than a raw snapshot,
    # since the prozone solution is less intuitive to eyeball).
    qK = log10.([1.0, 1.0, 1.0, 1.0, 1.0, 1.0])
    x  = qK2x(model, qK; input_logspace=true)
    A, B, L, AL, ALB, BL = x[1], x[2], x[3], x[4], x[5], x[6]
    @test isapprox(A + AL + ALB, 1.0; rtol=1e-6)            # SANITY-CHECKED tA = A + AL + ALB
    @test isapprox(B + ALB + BL, 1.0; rtol=1e-6)            # SANITY-CHECKED tB = B + ALB + BL
    @test isapprox(L + AL + ALB + BL, 1.0; rtol=1e-6)       # SANITY-CHECKED tL = L + AL + ALB + BL
    @test isapprox(A * L / AL, 1.0; rtol=1e-6)              # SANITY-CHECKED Kd1
    @test isapprox(B * L / BL, 1.0; rtol=1e-6)              # SANITY-CHECKED Kd2
    @test isapprox(AL * B / ALB, 1.0; rtol=1e-6)            # SANITY-CHECKED Kd3
    # Snapshot of the actual solved coordinates (tight tol) for regression.
    @test approxeq(A,   0.6180339887493963)
    @test approxeq(ALB, 0.14589803374990287)
end

# -----------------------------------------------------------------------------
@testset "5. sequential ternary  (A+L<->AL ; AL+B<->ALB)" begin
    model, species, free_syms, prod_syms = build_model(
        ["A + L <-> AL", "AL + B <-> ALB"])

    @test species == [:A, :B, :L, :AL, :ALB]
    @test (model.n, model.d, model.r) == (5, 3, 2)          # SANITY-CHECKED: n=d+r
    @test model.N == [1 0 1 -1 0; 0 1 0 1 -1]               # SANITY-CHECKED
    @test model.L == [1 0 0 1 1; 0 1 0 0 1; 0 0 1 1 1]      # SANITY-CHECKED

    find_all_vertices!(model)
    @test length(model.vertices_perm) == 14                 # snapshot
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 14   # snapshot
    @test length(get_vertices(model; singular=false, return_idx=true)) == 8     # snapshot

    # qK layout = [tA, tB, tL, Kd1, Kd2]; symmetric all-ones point.
    x = qK2x(model, log10.([1.0, 1.0, 1.0, 1.0, 1.0]); input_logspace=true)
    @test approxeq(x[1], 0.5187900636758842)                # A    (snapshot)
    @test approxeq(x[2], 0.7879331938447123)                # B    (snapshot)
    @test approxeq(x[3], 0.5187900636758842)                # L    (snapshot)
    @test approxeq(x[4], 0.269143130168828)                 # AL   (snapshot)
    @test approxeq(x[5], 0.21206680615528775)               # ALB  (snapshot)
    A, B, L, AL, ALB = x
    # SANITY-CHECKED conservation laws.
    @test isapprox(A + AL + ALB, 1.0; rtol=1e-6)            # tA
    @test isapprox(B + ALB, 1.0; rtol=1e-6)                 # tB
    @test isapprox(L + AL + ALB, 1.0; rtol=1e-6)            # tL
    # SANITY-CHECKED equilibria A*L/AL = Kd1 = 1, AL*B/ALB = Kd2 = 1.
    @test isapprox(A * L / AL, 1.0; rtol=1e-6)
    @test isapprox(AL * B / ALB, 1.0; rtol=1e-6)

    # Volume band on the ternary network's asymptotic regimes: the gaussian
    # probability masses of the non-singular asymptotic regimes sum to ~1.
    vols  = get_volumes(model; asymptotic=true, rel_tol=0.02, batch_size=50_000)
    means = [v.mean for v in vols]
    @test isapprox(sum(means), 1.0; atol=0.05)              # SANITY-CHECKED partition of unity
    @test count(>(0.0), means) == 8                         # snapshot: 8 non-singular regimes
end

@testset "Concurrent cold regime construction is single-flight" begin
    model = Bnc(
        N = reshape([1, 1, -1], 1, 3),
        x_sym = [:A, :L, :AL],
        q_sym = [:tA, :tL],
        K_sym = [:Kd1],
    )
    gate = Base.Event()
    tasks = [Threads.@spawn begin
        wait(gate)
        graph = get_regimes_graph!(model; full=true)
        (n_vertices(model), copy(model.vertices_perm), graph)
    end for _ in 1:8]
    notify(gate)
    results = fetch.(tasks)

    @test all(result -> result[1] == results[1][1], results)
    @test all(result -> result[2] == results[1][2], results)
    @test all(result -> result[3] === results[1][3], results)
    @test model._regimes_affine_ready
    @test model._regimes_build_complete
end

end # top-level testset
