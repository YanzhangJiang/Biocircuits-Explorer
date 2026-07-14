using Test
using BiocircuitsExplorerBackend

@testset "ROP shape replay keeps sampled evidence separate and complete" begin
    xs = collect(range(-4.0, 4.0; length=161))
    # Two smooth peaks over a lower central valley.
    ys = @. -2.0 + 1.4 * exp(-((xs + 2.0) / 0.65)^2) +
                    1.2 * exp(-((xs - 2.0) / 0.75)^2)
    result = BiocircuitsExplorerBackend.analyze_two_peak_curve(xs, ys, fill(true, length(xs));
        min_prominence_log10=0.5)
    @test result["schema_version"] == BiocircuitsExplorerBackend.ROP_SHAPE_REPLAY_VERSION
    @test result["complete"] === true
    @test result["pass"] === true
    @test length(result["peak_input_log10"]) == 2
    @test result["peak_separation_log10"] > 3.5
    @test result["left_half_prominence_width_log10"] > 0.0
    @test result["right_half_prominence_width_log10"] > 0.0
    @test result["central_half_prominence_interval_log10"] > 0.0

    partial = BiocircuitsExplorerBackend.analyze_two_peak_curve(
        xs, ys, [i == 40 ? false : true for i in eachindex(xs)])
    @test partial["status"] == "partial_solver_failure"
    @test partial["complete"] === false
    @test partial["pass"] === false

    monotone = BiocircuitsExplorerBackend.analyze_two_peak_curve(xs, xs, fill(true, length(xs)))
    @test monotone["status"] == "two_peaks_not_found"
    @test monotone["complete"] === true
    @test monotone["pass"] === false

    nonfinite = copy(ys)
    nonfinite[10] = NaN
    invalid = BiocircuitsExplorerBackend.analyze_two_peak_curve(
        xs, nonfinite, fill(true, length(xs)))
    @test invalid["status"] == "nonfinite_sample"
    @test invalid["complete"] === false
end
