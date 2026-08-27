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
export ROP_SHAPE_OPTIMIZE_REQUEST_VERSION, ROP_SHAPE_OPTIMIZATION_VERSION
export ROP_SHAPE_REPLAY_VERSION, analyze_two_peak_curve, optimize_rop_shape_request
export RO_FIELD_REQUEST_VERSION, RO_FIELD_SCHEMA_VERSION
export ROFieldRequestError, NormalizedROFieldRequest
export normalize_ro_field_request, validate_ro_field_document!, validate_ro_field_payload!, produce_ro_field
export RO_FIELD_DIFFERENTIAL_ANALYSIS_VERSION, RO_FIELD_DIFFERENTIAL_REQUEST_VERSION
export analyze_ro_field_differential, validate_ro_field_differential_analysis!
export RO_FIELD_CHUNK_PLAN_SCHEMA_VERSION, RO_FIELD_WORK_UNIT_SCHEMA_VERSION, RO_FIELD_CHUNK_SCHEMA_VERSION, RO_FIELD_CHECKPOINT_SCHEMA_VERSION, RO_FIELD_DATASET_MANIFEST_SCHEMA_VERSION
export ROFieldChunkLimits, ROFieldChunkLimitExceeded, ROFieldChunkContractError, build_ro_field_chunk_plan, validate_ro_field_chunk_plan!, ro_field_chunk_plan_sha256, ro_field_plan_work_units, validate_ro_field_work_unit!, ro_field_work_unit_sha256, build_ro_field_chunk, validate_ro_field_chunk!, canonical_ro_field_chunk_bytes, ro_field_chunk_sha256, write_ro_field_chunk!, read_ro_field_chunk, build_ro_field_checkpoint, validate_ro_field_checkpoint!, resume_ro_field_work_units, build_ro_field_dataset_manifest, validate_ro_field_dataset_manifest!
export RO_FIELD_SLICE_SPEC_SCHEMA_VERSION, RO_FIELD_SLICE_SCHEMA_VERSION, RO_FIELD_SLICE_ALGORITHM_VERSION, ROFieldSliceLimits, ROFieldSliceLimitExceeded, ROFieldSliceContractError, build_ro_field_slice, validate_ro_field_slice!
export RO_FIELD_CAMPAIGN_MANIFEST_VERSION, RO_FIELD_CAMPAIGN_SHARD_RESULT_VERSION, RO_FIELD_CAMPAIGN_CORPUS_LOCK_VERSION, RO_FIELD_CAMPAIGN_QC_VERSION, ROFieldCampaignLimits, ROFieldCampaignLimitExceeded, build_ro_field_campaign_manifest, validate_ro_field_campaign_manifest!, run_ro_field_campaign_demo_shard, validate_ro_field_campaign_shard_result!, merge_ro_field_campaign_shards, validate_ro_field_campaign_corpus_lock!, audit_ro_field_campaign_corpus, validate_ro_field_campaign_qc!
export RO_FIELD_JOB_SPEC_VERSION, RO_FIELD_JOB_RESULT_VERSION, RO_FIELD_SPARSE_REQUEST_VERSION, RO_FIELD_SPARSE_JOB_SPEC_VERSION, RO_FIELD_SPARSE_PLAN_VERSION, RO_FIELD_SPARSE_JOB_RESULT_VERSION, RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION, RO_FIELD_SPARSE_NUMERICAL_POLICY_VERSION
export normalize_ro_field_job_spec, validate_ro_field_resume_parent!, compute_ro_field_job, validate_ro_field_job_result!, normalize_ro_field_sparse_job_spec, validate_ro_field_sparse_resume_parent!, validate_ro_field_sparse_job_result!
export RO_CELL_COMPLEX_MAGIC, RO_CELL_COMPLEX_CODEC_VERSION, RO_CELL_COMPLEX_IDENTITY_KIND, ROFieldIdentityError
export canonical_ro_field_data_bytes, ro_field_data_sha256, canonical_ro_field_document_bytes, ro_field_artifact_sha256, canonical_ro_cell_complex_payload, encode_ro_cell_complex_blob, decode_ro_cell_complex_blob, ro_cell_complex_hash
export RO_FIELD_SIGNATURE_SCHEMA_VERSION, RO_FIELD_SIGNATURE_CLASSIFIER_VERSION, RO_FIELD_SIGNATURE_SCOPE, RO_FIELD_SIGNATURE_PROVENANCE_CLAIM, ROFieldSignatureConfig, ROFieldSignatureLimitExceeded, classify_ro_cell_complex, ro_field_signature_identity_payload, validate_ro_field_signature!
export RO_FIELD_ATLAS_SCHEMA_VERSION, RO_FIELD_ATLAS_QUERY_SCHEMA_VERSION, RO_FIELD_ATLAS_QUERY_RESULT_SCHEMA_VERSION, ROFieldAtlasInput, ROFieldAtlasConfig, ROFieldAtlasLimitExceeded, ROFieldComponentFilter, ROFieldGradientFilter, ROFieldAtlasQuerySpec, build_ro_field_atlas, query_ro_field_atlas, atlas_sqlite_save_ro_field_artifact!, atlas_sqlite_load_ro_field_artifact, atlas_sqlite_query_ro_field_artifacts, atlas_sqlite_save_ro_field_signature!, atlas_sqlite_load_ro_field_signature, atlas_sqlite_query_ro_field_signatures

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
using .SessionStore: get_session, set_session, set_session_if_available
using .DebugLog: append_debug_log, with_debug_client_scope,
                  debug_client_id_from_request, install_debug_logger!
using .Observability: counter_inc!, gauge_set!, hist_observe!,
                       render_prometheus, log_request_json,
                       json_logs_enabled, iso_timestamp
using .Serialization: mat2vv, json_safe_value, json_safe_real, json_safe_profile,
                       json_response, error_response, read_json, is_request_error,
                       RequestBodyTooLarge
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
include(joinpath(@__DIR__, "sync_work_budget.jl"))
include(joinpath(@__DIR__, "path_work_budget.jl"))

include(joinpath(@__DIR__, "analysis_serializers.jl"))

include(joinpath(@__DIR__, "analysis_computation.jl"))

include(joinpath(@__DIR__, "cancellation.jl"))
include(joinpath(@__DIR__, "atlas.jl"))
include(joinpath(@__DIR__, "behavior_program_codec.jl"))
include(joinpath(@__DIR__, "ro_field_identity.jl"))
include(joinpath(@__DIR__, "ro_field_behavior.jl"))
include(joinpath(@__DIR__, "atlas_sqlite.jl"))
include(joinpath(@__DIR__, "ro_field_atlas.jl"))
include(joinpath(@__DIR__, "inverse_design.jl"))
include(joinpath(@__DIR__, "atlas_build_budget.jl"))
include(joinpath(@__DIR__, "atlas_corpus_budget.jl"))
include(joinpath(@__DIR__, "atlas_query_budget.jl"))
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
include(joinpath(@__DIR__, "ro_field_contract.jl"))
include(joinpath(@__DIR__, "ro_field_chunks.jl"))
include(joinpath(@__DIR__, "ro_field_slices.jl"))
include(joinpath(@__DIR__, "ro_field_campaign.jl"))
include(joinpath(@__DIR__, "ro_field_jobs.jl"))
include(joinpath(@__DIR__, "ro_field_sparse_jobs.jl"))
include(joinpath(@__DIR__, "ro_field_differential.jl"))
include(joinpath(@__DIR__, "ro_field_api.jl"))
include(joinpath(@__DIR__, "model_handlers.jl"))

include(joinpath(@__DIR__, "parameter_placement.jl"))

include(joinpath(@__DIR__, "rop_shape_replay.jl"))

include(joinpath(@__DIR__, "rop_shape_optimization.jl"))

include(joinpath(@__DIR__, "design_search.jl"))

include(joinpath(@__DIR__, "parameter_level.jl"))
include(joinpath(@__DIR__, "parameter_scan_handlers.jl"))

include(joinpath(@__DIR__, "rop_geometry_handlers.jl"))

include(joinpath(@__DIR__, "designability_feasible_regions.jl"))
include(joinpath(@__DIR__, "designability.jl"))
include(joinpath(@__DIR__, "rop_shape_api.jl"))
include(joinpath(@__DIR__, "routing.jl"))

end # module
