module DesignabilityCellBudgetContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

@testset "Designability exact-cell preflight is bounded before materialization" begin
    choices = [collect(1:100), collect(1:100), collect(1:10)]
    products = Backend._designability_vertex_choice_products(choices)
    @test !(products isa AbstractArray)
    @test Base.IteratorSize(typeof(products)) isa Base.HasShape
    @test first(products) == (1, 1, 1)

    @test Backend._designability_bounded_product_count([2, 3, 4]) == 24
    @test Backend._designability_bounded_product_count([2, 0, 4]) == 0
    @test_throws Backend.SyncBudgetExceeded Backend._designability_bounded_product_count(
        [100, 100, 10]; limit=256)
    @test_throws Backend.SyncBudgetExceeded Backend._designability_bounded_product_count(
        [typemax(Int), 2])
    @test_throws Backend.SyncBudgetExceeded Backend._designability_add_cell_count(
        250, 7, 256)
    @test Backend._designability_add_cell_count(250, 6, 256) == 256

    source = read(joinpath(@__DIR__, "..", "src", "designability_feasible_regions.jl"), String)
    @test !occursin("collect(Iterators.product", source)
    preflight = findfirst("for pr in bf.path_records", source)
    solve = findfirst("for entry in eligible_paths", source)
    @test preflight !== nothing
    @test solve !== nothing
    @test first(preflight) < first(solve)

    screen_source = read(joinpath(@__DIR__, "..", "src", "designability.jl"), String)
    @test occursin(
        "max_cells = MAX_SYNC_DESIGNABILITY_FEASIBLE_CELLS",
        screen_source,
    )
    @test occursin(
        "Exact feasible-region cell enumeration exceeds",
        screen_source,
    )
end

end # module DesignabilityCellBudgetContract
