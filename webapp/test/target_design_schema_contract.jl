using Test
using JSON3

# Reuse the repository's narrow JSON Schema evaluator, loaded by the preceding
# ROP schema contract. Exercise actual instances and dimension conditionals.
@testset "Design target schema accepts configurable dimensions" begin
    schema = BiocircuitsExplorerBackend._materialize(JSON3.read(read(joinpath(
        @__DIR__, "..", "..", "schemas", "design-target.schema.json"), String)))
    errors(value) = _rop_schema_errors(value, schema, schema, Dict())
    for (ni, no) in ((1, 1), (1, 2), (2, 1), (3, 3))
        value = BiocircuitsExplorerBackend.normalize_target_design_request(
            TargetDesignAPITests.design_request(input_count=ni, output_count=no))["target"]
        @test isempty(errors(value))
        for dimension in ("inputs", "outputs")
            bad = deepcopy(value)
            push!(bad["samples"][1][dimension], 0.4)
            @test !isempty(errors(bad))
            bad = deepcopy(value)
            bad["validation_samples"] = [deepcopy(first(value["samples"]))]
            push!(bad["validation_samples"][1][dimension], 0.4)
            @test !isempty(errors(bad))
        end
    end
    value = BiocircuitsExplorerBackend.normalize_target_design_request(
        TargetDesignAPITests.design_request())["target"]
    value["samples"][1]["inputs"] = [-1.0]
    @test !isempty(errors(value))
    delete!(value["samples"][1], "inputs")
    @test !isempty(errors(value))
end
