# Tests for the v0.3.0 phenotyper. Runs on a few tiny networks in seconds (after
# first-call compilation) — validates correctness, determinism and the v0.3.0
# review fixes (real threshold/plateau gates, floor-limited fold change,
# direction-aware operating range), not scale.
#
#   julia --project=webapp webapp/test/test_phenotype_pipeline.jl

using Test
using BindingAndCatalysis

const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline

# Smaller-but-still-deterministic policy for a fast smoke test.
const POLICY = PhenotyperPolicy(; K = 24, npoints = 81, bracket_npoints = 25)

@testset "phenotyper v0.4.0" begin
    mono, = build_model(["L + A <-> AL"], [1.0])                                            # monotone activation
    coop, = build_model(["A + L <-> AL", "AL + L <-> AL2"], [1.0, 1.0])                     # two-site / thresholded AL2
    hook, = build_model(["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"], [1.0,1.0,1.0])  # prozone/hook

    r_mono = phenotype(mono; input_sym = :tL, output_expr = "AL",
                       policy = POLICY, target_class = :monotone_activation)
    r_hook = phenotype(hook; input_sym = :tL, output_expr = "ALB",
                       policy = POLICY, target_class = :biphasic_peak)

    @testset "shape_support discriminates between networks" begin
        @test 0.0 <= r_mono.shape_support <= 1.0
        @test 0.0 <= r_hook.shape_support <= 1.0
        @test r_mono.shape_support >= 0.8          # monotone net is mostly monotone-activation
        @test r_hook.shape_support > 0.5           # hook robustly biphasic in ALB across the prior
        # cross-network discrimination: a monotone net is essentially never biphasic
        mono_as_biphasic = phenotype(mono; input_sym=:tL, output_expr="AL",
                                     policy=POLICY, target_class=:biphasic_peak).shape_support
        @test mono_as_biphasic < 0.1
    end

    @testset "determinism (same seed+policy ⇒ identical labels)" begin
        r2 = phenotype(mono; input_sym = :tL, output_expr = "AL",
                       policy = POLICY, target_class = :monotone_activation)
        @test r2.shape_support == r_mono.shape_support
        @test r2.stats[:rise_slope].median === r_mono.stats[:rise_slope].median
        @test r2.stats[:peak_prominence].q_alpha === r_mono.stats[:peak_prominence].q_alpha
    end

    @testset "provenance + version are reported" begin
        @test r_mono.phenotyper_version == POLICY.version
        @test r_mono.sampler == :halton
        @test haskey(r_mono.prior, "default_kd")        # serialized prior descriptor
    end

    @testset "thresholded_activation is NOT identical to monotone_activation" begin
        # A plain single-binding curve must not pass the threshold gate (no shoulder).
        r_thr_mono = phenotype(mono; input_sym = :tL, output_expr = "AL",
                               policy = POLICY, target_class = :thresholded_activation)
        @test r_thr_mono.shape_support < r_mono.shape_support
        # The two-site assembly (AL2) has a genuine shoulder → it should score
        # higher on the threshold gate than the plain binding does.
        r_thr_coop = phenotype(coop; input_sym = :tL, output_expr = "AL2",
                               policy = POLICY, target_class = :thresholded_activation)
        @test r_thr_coop.shape_support >= r_thr_mono.shape_support
    end

    @testset "bandpass_with_plateau is stricter than biphasic_peak" begin
        ss_peak = phenotype(hook; input_sym = :tL, output_expr = "ALB",
                            policy = POLICY, target_class = :biphasic_peak).shape_support
        ss_plat = phenotype(hook; input_sym = :tL, output_expr = "ALB",
                            policy = POLICY, target_class = :bandpass_with_plateau).shape_support
        @test ss_plat <= ss_peak    # plateau gate is a strict refinement of the peak gate
    end

    @testset "per-curve metrics: floor flag + direction-aware operating range" begin
        prof = phenotype_profile(mono; input_sym = :tL, output_expr = "AL", policy = POLICY)
        @test prof.dominant_shape in PhenotypePipeline._PROFILE_CLASSES
        @test haskey(prof.shape_fractions, :thresholded_activation)   # new class present
        @test haskey(prof.shape_fractions, :bandpass_with_plateau)
        @test isfinite(prof.stats[:input_operating_range_log10].median)
        @test prof.stats[:input_operating_range_log10].median > 0     # not the old degenerate 0

        # Repression operating range is now measurable (was always 0 before v0.3.0).
        comp, = build_model(["E + S <-> ES", "E + I <-> EI"], [1.0, 1.0])
        rep = phenotype_profile(comp; input_sym = :tI, output_expr = "ES", policy = POLICY)
        @test rep.stats[:input_operating_range_log10].median > 0
    end

    @testset "QMC vs MC samplers both deterministic" begin
        pmc = PhenotyperPolicy(; K = 24, npoints = 81, bracket_npoints = 25, sampler = :mc)
        a = phenotype(mono; input_sym=:tL, output_expr="AL", policy=pmc, target_class=:monotone_activation)
        b = phenotype(mono; input_sym=:tL, output_expr="AL", policy=pmc, target_class=:monotone_activation)
        @test a.shape_support == b.shape_support    # MC path deterministic under fixed seed
    end

    @testset "multimodal: ≥2 sign reversals, guarded against deadband jitter" begin
        # multimodal is in the vocabulary and the profile classes
        @test :multimodal in PhenotypePipeline._PROFILE_CLASSES
        @test haskey(PhenotypePipeline.PHENOTYPE_VOCAB_V0, :multimodal)

        # turning_analysis counts reversals + reports the min interior swing
        rho = [1.0, 1.0, -1.0, -1.0, 1.0, 1.0]
        ylog = [0.0, 0.5, 1.0, 0.6, 0.2, 0.7]
        ncs, swing = PhenotypePipeline.turning_analysis(ylog, rho, POLICY.rho_zero)
        @test ncs == 2
        @test swing ≈ 0.5 atol = 1e-9

        # an up-down-up curve with genuine swings → :multimodal (and the gate passes)
        m_osc = (; sign_seq = [1, -1, 1], n_sign_changes = 2, min_swing_log10 = 0.5)
        @test classify_shape([1, -1, 1], m_osc, POLICY) === :multimodal
        @test PhenotypePipeline.shape_gate(m_osc, :multimodal, POLICY)

        # the SAME sign sequence but a tiny middle swing is deadband jitter, NOT a
        # real oscillation → stays :complex and fails the gate (the no-fabrication guard)
        m_jit = (; sign_seq = [1, -1, 1], n_sign_changes = 2, min_swing_log10 = 0.01)
        @test classify_shape([1, -1, 1], m_jit, POLICY) === :complex
        @test !PhenotypePipeline.shape_gate(m_jit, :multimodal, POLICY)

        # a single peak must NOT be called multimodal
        m_peak = (; sign_seq = [1, -1], n_sign_changes = 1, min_swing_log10 = 0.5,
                  peak_prominence = 0.5, plateau_width_log10_input = 0.0)
        @test !PhenotypePipeline.shape_gate(m_peak, :multimodal, POLICY)
    end

    @testset "kd_profile mostly_weak honors fractional/outlier semantics" begin
        prior = reshape_prior(ParameterPrior(),
                              Dict("mode"=>"mostly_weak", "weak_fraction_min"=>0.75,
                                   "allow_strong_outliers"=>1))
        @test prior.kd_profile !== nothing
        @test prior.kd_profile.weak_fraction_min == 0.75
        @test prior.kd_profile.allow_strong_outliers == 1
        r = phenotype(hook; input_sym=:tL, output_expr="ALB", policy=POLICY,
                      prior=prior, target_class=:biphasic_peak)
        @test 0.0 <= r.shape_support <= 1.0
        @test haskey(r.prior, "kd_profile")          # recorded in provenance
    end
end
