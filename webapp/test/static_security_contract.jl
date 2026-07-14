using Test

@testset "Node workspace static security boundary" begin
    static_assets = BiocircuitsExplorerBackend.StaticAssets
    node_headers = Dict(static_assets._static_security_headers("index-node.html"))
    ordinary_headers = Dict(static_assets._static_security_headers("index.html"))

    @test node_headers["X-Content-Type-Options"] == "nosniff"
    @test node_headers["Referrer-Policy"] == "no-referrer"
    @test haskey(node_headers, "Content-Security-Policy")
    @test occursin("script-src 'self' 'unsafe-eval'", node_headers["Content-Security-Policy"])
    @test !occursin("script-src 'self' 'unsafe-inline'", node_headers["Content-Security-Policy"])
    @test occursin("frame-ancestors 'none'", node_headers["Content-Security-Policy"])
    @test !haskey(ordinary_headers, "Content-Security-Policy")

    node_html = read(joinpath(@__DIR__, "..", "public", "index-node.html"), String)
    @test !occursin("document.write", node_html)
    @test !occursin("cdn.plot.ly", node_html)
    @test !occursin("cdnjs.cloudflare.com", node_html)
    @test occursin("vendor/plotly-2.27.0.min.js", node_html)
    @test occursin("vendor/jszip-3.10.1.min.js", node_html)
end
