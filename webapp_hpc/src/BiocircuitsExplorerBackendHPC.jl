module BiocircuitsExplorerBackendHPC
# Intentionally empty. webapp_hpc is used purely as a dependency ENVIRONMENT for the
# headless Phase-2 data-gen / benchmark scripts, which `include`
# webapp/src/reaction_parser.jl + webapp/src/latent_atlas/phenotype_pipeline.jl and
# `using BindingAndCatalysis` directly (they never `using` this package). This stub
# gives the project package a valid source file so `Pkg.precompile()` and
# PackageCompiler.create_sysimage succeed (previously the missing src/ broke both).
end
