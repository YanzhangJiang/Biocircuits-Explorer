module BiocircuitsExplorerBackend

export main, julia_main, router
export AtlasSearchProfile, AtlasBehaviorConfig, AtlasEnumerationSpec, AtlasChangeExpansionSpec, AtlasQuerySpec, InverseDesignSpec, InverseRefinementSpec
export atlas_search_profile_binding_small_v0, atlas_behavior_config_default
export atlas_enumeration_spec_default, atlas_change_expansion_spec_default, atlas_query_spec_default, inverse_design_spec_default, inverse_refinement_spec_default
export atlas_library_default, is_atlas_library
export atlas_sqlite_default_path, atlas_sqlite_connect, atlas_sqlite_init!, atlas_sqlite_has_library
export atlas_sqlite_load_library, atlas_sqlite_save_library!, atlas_sqlite_summary
export atlas_sqlite_existing_ok_slice_ids, atlas_sqlite_merge_atlas!, atlas_sqlite_record_skip_only_event!, atlas_sqlite_append_atlas!
export canonical_program_profile, encode_program_blob, decode_program_blob, behavior_program_hash, program_exact_label, program_motif_label, program_features
export NetworkIR, DesignSpec, SpeciesDecl, ReactionDecl, ObservableDecl, ParameterDistribution, Provenance, IRValidationError
export NETWORK_IR_SCHEMA_VERSION, DESIGN_SPEC_SCHEMA_VERSION
export parse_network_ir, parse_design_spec, network_ir_to_dict, design_spec_to_dict
export network_ir_from_legacy, network_ir_to_legacy_inputs, design_spec_to_legacy_request
export network_ir_hash, design_spec_hash, is_network_ir, is_legacy_network_payload
export network_ir_to_sbml, sbml_to_network_ir
export enumerate_network_specs
export build_behavior_atlas, build_behavior_atlas_from_spec, looks_like_atlas_corpus
export build_atlas_library, build_atlas_library_from_spec
export merge_atlas_library, merge_atlas_library_from_spec
export query_behavior_atlas, query_behavior_atlas_from_spec
export run_inverse_design, run_inverse_design_from_spec
export compile_query, stable_hash
export canonicalize_network, emit_support_signature
export run_bounding_screen, run_exact_support_screen
export plan_delta_build, build_summary_delta, merge_atlas_delta
export retrieve_candidates, materialize_witnesses, refine_top_k
export record_negative, check_negative
export submit_biocircuits_job_from_spec, get_biocircuits_job, get_biocircuits_job_result, cancel_biocircuits_job
export run_biocircuits_job_payload, run_biocircuits_job_from_uri
export biocircuits_explorer_version, biocircuits_explorer_build_info
export RESULT_ARTIFACT_SCHEMA_VERSION, artifact_metadata, attach_artifact!, wrap_artifact
export DESIGNABILITY_SPEC_VERSION, normalize_designability_spec, design_screen_from_spec

using HTTP
using JSON3
using LinearAlgebra
using BindingAndCatalysis
using Polyhedra
using CDDLib
using Graphs
using SparseArrays
using Random
using Logging
using Dates
using Base64
using SHA
using DBInterface
using SQLite
import EzXML

# BindingAndCatalysis keeps this accessor internal, while the web backend uses
# it throughout SISO graph and trajectory serialization.
get_change_qK_idx(args...) = BindingAndCatalysis.get_change_qK_idx(args...)

include(joinpath(@__DIR__, "config.jl"))
include(joinpath(@__DIR__, "session_store.jl"))
include(joinpath(@__DIR__, "model_cache.jl"))
include(joinpath(@__DIR__, "debug_log.jl"))
include(joinpath(@__DIR__, "observability.jl"))
include(joinpath(@__DIR__, "serialization.jl"))
include(joinpath(@__DIR__, "reaction_parser.jl"))
include(joinpath(@__DIR__, "static_assets.jl"))
using .SessionStore: get_session, set_session
using .DebugLog: append_debug_log, with_debug_client_scope,
                  debug_client_id_from_request, install_debug_logger!
using .Observability: counter_inc!, gauge_set!, hist_observe!,
                       render_prometheus, log_request_json,
                       json_logs_enabled, iso_timestamp
using .Serialization: mat2vv, json_safe_value, json_safe_real, json_safe_profile,
                       json_response, error_response, read_json, is_request_error
using .ReactionParser: parse_term, parse_side, parse_reactions, parse_network_structure,
                       build_model, default_log_qK, fixed_qK_or_default
using .StaticAssets: static_dir, serve_static

# Shared canonicalization / content-identity primitives (raw-JSON access, the
# canonical-JSON hasher, and the graph-canonical network code). Included here —
# before atlas.jl/inverse_design.jl/ir.jl — so the IR/result substrate no longer
# depends backwards on those big files just to hash an artifact.
include(joinpath(@__DIR__, "canonicalization.jl"))

# These plain includes intentionally share this module's namespace. Their order
# is dependency order; routing stays last because it resolves every handler at
# load time. `webapp/test/backend_assembly_contract.jl` guards that structure.
include(joinpath(@__DIR__, "runtime_lifecycle.jl"))

include(joinpath(@__DIR__, "request_support.jl"))

include(joinpath(@__DIR__, "analysis_serializers.jl"))

include(joinpath(@__DIR__, "analysis_computation.jl"))

include(joinpath(@__DIR__, "cancellation.jl"))
include(joinpath(@__DIR__, "atlas.jl"))
include(joinpath(@__DIR__, "behavior_program_codec.jl"))
include(joinpath(@__DIR__, "atlas_sqlite.jl"))
include(joinpath(@__DIR__, "inverse_design.jl"))
include(joinpath(@__DIR__, "ir.jl"))
include(joinpath(@__DIR__, "sbml.jl"))
include(joinpath(@__DIR__, "version.jl"))
include(joinpath(@__DIR__, "result_artifact.jl"))
include(joinpath(@__DIR__, "auth.jl"))
include(joinpath(@__DIR__, "jobs.jl"))
# The Latent-Atlas SISO phenotyper — the SAME labeller that built the dose atlas. Exposed so the
# design agent verifies a candidate's dose-response shape CONSISTENTLY with the atlas labels
# (shape_support over the Kd prior Π), not via the different ROP-family view of behavior_families.
include(joinpath(@__DIR__, "latent_atlas", "phenotype_pipeline.jl"))
using .PhenotypePipeline: phenotype_profile, PhenotyperPolicy, ParameterPrior, LogUniform, PointMass

export verify_cognito_jwt

include(joinpath(@__DIR__, "service_handlers.jl"))

include(joinpath(@__DIR__, "model_runtime.jl"))

include(joinpath(@__DIR__, "model_handlers.jl"))

include(joinpath(@__DIR__, "parameter_placement.jl"))

include(joinpath(@__DIR__, "design_search.jl"))

include(joinpath(@__DIR__, "parameter_level.jl"))
include(joinpath(@__DIR__, "parameter_scan_handlers.jl"))

include(joinpath(@__DIR__, "rop_geometry_handlers.jl"))

include(joinpath(@__DIR__, "designability_feasible_regions.jl"))
include(joinpath(@__DIR__, "designability.jl"))
include(joinpath(@__DIR__, "routing.jl"))

end # module
