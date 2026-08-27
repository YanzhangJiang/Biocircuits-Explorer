# =============================================================================
# Headless golden computation smoke.
#
# CONTRACT: CI runs this exact script under `--project=webapp_hpc` on Julia
# 1.10 and Julia 1.12. In that environment BindingAndCatalysis resolves to
# Bnc_julia_headless, whose 2-line shim sets BNC_HEADLESS=1 and includes the
# shared engine source — the same headless code path the Slurm data pipelines
# execute. Before this script existed, the HPC CI lane only proved the module
# loads (`using BindingAndCatalysis`) and never executed any computation.
#
# GOLDEN VALUES: the single-network characterization numbers frozen in
# Bnc_julia/test/runtests.jl testset "1. single (L + A <-> AL)". A silent
# change to regime enumeration or the qK->x mapping trips this smoke exactly
# as it trips the full golden suite.
# =============================================================================

using Test
using BindingAndCatalysis

@testset "headless runtime identity" begin
    @test BindingAndCatalysis._BNC_HEADLESS === true
    @test basename(dirname(dirname(pathof(BindingAndCatalysis)))) == "Bnc_julia_headless"
    loaded_package_names = Set(package_id.name for package_id in keys(Base.loaded_modules))
    for package_name in ("Makie", "GraphMakie", "ImageFiltering")
        @test package_name ∉ loaded_package_names
    end
    for binding in (:Makie, :GraphMakie, :ImageFiltering, :imfilter, :Kernel)
        @test !isdefined(BindingAndCatalysis, binding)
    end
end

@testset "headless golden smoke (single network)" begin
    model = Bnc(N = reshape([1 1 -1;], 1, 3),
                L = [1 0 1; 0 1 1],
                q_sym = Symbol.(["tA", "tL"]),
                K_sym = Symbol.(["Kd1"]))

    # Structural invariants (SANITY-CHECKED in the full suite: n = d + r).
    @test (model.n, model.d, model.r) == (3, 2, 1)

    # Regime enumeration must run in headless mode.
    find_all_vertices!(model)
    @test n_vertices(model) == 4
    @test length(get_vertices(model; asymptotic=true, return_idx=true)) == 4
    @test length(get_vertices(model; singular=false, return_idx=true)) == 3

    # Symmetric point tA = tL = Kd = 1 (golden snapshots from the full suite;
    # AL solves the x^2 form -> ~0.382).
    x_sym = qK2x(model, log10.([1.0, 1.0, 1.0]); input_logspace=true)
    @test isapprox(x_sym[1], 0.6180339887497848; rtol = 1e-12)
    @test isapprox(x_sym[3], 0.38196601124996904; rtol = 1e-12)
    # SANITY-CHECKED equilibrium relation A*L/AL = Kd = 1 at the solved point.
    @test isapprox(x_sym[1] * x_sym[2] / x_sym[3], 1.0; rtol = 1e-9)
    # SANITY-CHECKED conservation tA = A + AL = 1.
    @test isapprox(x_sym[1] + x_sym[3], 1.0; rtol = 1e-9)
end
