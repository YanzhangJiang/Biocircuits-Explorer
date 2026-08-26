using Test
using SHA
using BindingAndCatalysis

@testset "D=3 package assembly and exports" begin
    exported_names = Set(names(BindingAndCatalysis))
    for name in (
        :ROStratifiedField3DLimits,
        :RORegularExtensionIntegrabilityCertificate3D,
        :certify_ro_regular_extension_integrability_3d,
        :validate_ro_regular_extension_integrability_certificate_3d,
    )
        @test isdefined(BindingAndCatalysis, name)
        @test name in exported_names
    end
end

const _ROSF3_TEST = BindingAndCatalysis

struct _ROSF3TestCancelled <: Exception end

struct _ROSF3OutputProbe <: AbstractVector{Int}
    declared_length::Int
    reads::Base.RefValue{Int}
end

Base.size(probe::_ROSF3OutputProbe) = (probe.declared_length,)
Base.IndexStyle(::Type{_ROSF3OutputProbe}) = IndexLinear()
function Base.getindex(probe::_ROSF3OutputProbe, index::Int)
    probe.reads[] += 1
    error("unexpected output-index probe read at $(index)")
end

struct _ROSF3CountingOutputProbe <: AbstractVector{Int}
    declared_length::Int
    reads::Base.RefValue{Int}
end

Base.size(probe::_ROSF3CountingOutputProbe) = (probe.declared_length,)
Base.IndexStyle(::Type{_ROSF3CountingOutputProbe}) = IndexLinear()
function Base.getindex(probe::_ROSF3CountingOutputProbe, index::Int)
    probe.reads[] += 1
    return index
end

function _rosf3_test_label(regime_id, matrix, offset)
    return _ROSF3_TEST.ROAffineLabel3D(
        [regime_id], Matrix{Float64}(matrix), Float64.(offset))
end

function _rosf3_split_complex(;
    left_matrix=reshape([1.0, 2.0, 3.0], 1, 3),
    right_matrix=reshape([4.0, 2.0, 3.0], 1, 3),
    left_offset=[0.0],
    right_offset=[-1.5],
    reverse_specs=false,
)
    domain = _ROSF3_TEST.ROInputDomain3D(
        (1, 2, 3), (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    specs = [
        _ROSF3_TEST.ROCellSpec3D(
            "left", [1.0 0.0 0.0], [0.5], [1],
            [_rosf3_test_label(1, left_matrix, left_offset)]),
        _ROSF3_TEST.ROCellSpec3D(
            "right", [-1.0 0.0 0.0], [-0.5], [2],
            [_rosf3_test_label(2, right_matrix, right_offset)]),
    ]
    reverse_specs && reverse!(specs)
    return _ROSF3_TEST.build_ro_cell_complex_3d(domain, specs)
end

function _rosf3_abc_complex(; reverse_specs=false)
    scores = [
         1.0  1.0  1.0
         1.0 -1.0 -1.0
        -1.0  1.0 -1.0
        -1.0 -1.0  1.0
    ]
    specs = _ROSF3_TEST.ROCellSpec3D[]
    for cell_index in axes(scores, 1)
        others = [index for index in axes(scores, 1) if index != cell_index]
        A = reduce(vcat, [reshape(
            scores[other, :] - scores[cell_index, :], 1, 3)
            for other in others])
        push!(specs, _ROSF3_TEST.ROCellSpec3D(
            "regime-$(cell_index)", A, zeros(length(others)),
            [cell_index], [_rosf3_test_label(
                cell_index, reshape(scores[cell_index, :], 1, 3), [0.0])]))
    end
    reverse_specs && reverse!(specs)
    domain = _ROSF3_TEST.ROInputDomain3D(
        (1, 2, 3), (-2.0, -2.0, -2.0), (2.0, 2.0, 2.0), zeros(3))
    annotations = [_ROSF3_TEST.ROInterfaceAnnotation3D(
        ("regime-2", "regime-3"), :singular, :singular_regime_limit)]
    return _ROSF3_TEST.build_ro_cell_complex_3d(
        domain, specs; annotations=annotations)
end

function _rosf3_t_junction_complex()
    domain = _ROSF3_TEST.ROInputDomain3D(
        (1, 2, 3), (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    common_matrix = reshape([1.0, 2.0, 3.0], 1, 3)
    specs = [
        _ROSF3_TEST.ROCellSpec3D(
            "bottom", [0.0 0.0 1.0], [0.5], [1],
            [_rosf3_test_label(1, common_matrix, [0.0])]),
        _ROSF3_TEST.ROCellSpec3D(
            "top-left", [0.0 0.0 -1.0; 1.0 0.0 0.0],
            [-0.5, 0.5], [2],
            [_rosf3_test_label(2, common_matrix, [0.0])]),
        _ROSF3_TEST.ROCellSpec3D(
            "top-right", [0.0 0.0 -1.0; -1.0 0.0 0.0],
            [-0.5, -0.5], [3],
            [_rosf3_test_label(3, common_matrix, [0.0])]),
    ]
    return _ROSF3_TEST.build_ro_cell_complex_3d(domain, specs)
end

function _rosf3_ambiguous_complex()
    domain = _ROSF3_TEST.ROInputDomain3D(
        (1, 2, 3), (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    specs = [
        _ROSF3_TEST.ROCellSpec3D(
            "same-a", [1.0 0.0 0.0], [1.0], [1],
            [_rosf3_test_label(1, reshape([1.0, 0.0, 0.0], 1, 3),
                [0.0])]),
        _ROSF3_TEST.ROCellSpec3D(
            "same-b", [1.0 0.0 0.0], [1.0], [2],
            [_rosf3_test_label(2, reshape([0.0, 1.0, 0.0], 1, 3),
                [0.0])]),
    ]
    return _ROSF3_TEST.build_ro_cell_complex_3d(domain, specs)
end

@testset "D=3 exact-dyadic global regular-extension certificate" begin
    raw_outputs = [7]
    complex = _rosf3_split_complex()
    certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, raw_outputs)
    raw_outputs[1] = 99

    @test certificate.schema_version ==
        _ROSF3_TEST.RO_REGULAR_EXTENSION_INTEGRABILITY_3D_VERSION
    @test certificate.evidence_scope ==
        _ROSF3_TEST.RO_REGULAR_EXTENSION_INTEGRABILITY_3D_SCOPE
    @test certificate.source_complex_identity == complex.canonical_identity
    @test certificate.output_indices == [7]
    @test certificate.output_count == 1
    @test certificate.status == :regular_cell_extension_integrable
    @test certificate.gradient_integrability_status ==
        :exact_dyadic_global_integrable
    @test certificate.provided_potential_status == :exact_dyadic_continuous
    @test isempty(certificate.reasons)
    @test certificate.regular_limit_only
    @test !certificate.includes_singular_branch
    @test certificate.coordinate_semantics ==
        _ROSF3_TEST.RO_REGULAR_EXTENSION_INTEGRABILITY_3D_COORDINATES
    @test certificate.domain_topology == :complete_contractible_box
    @test certificate.checked_cell_count == 2
    @test certificate.checked_internal_facet_count == 1
    @test certificate.checked_exact_basis_point_count == 3
    @test certificate.checked_tangential_equation_count == 2
    @test certificate.checked_offset_equation_count == 1
    @test certificate.checked_cycle_equation_count == 0
    @test certificate.dual_graph_component_count == 1
    @test certificate.dual_graph_edge_count == 1
    @test certificate.dual_graph_cycle_rank == 0
    @test certificate.tangential_obstruction_count == 0
    @test certificate.cycle_obstruction_count == 0
    @test certificate.provided_offset_obstruction_count == 0
    @test isempty(certificate.witnesses)
    @test !certificate.witnesses_truncated
    @test !certificate.arbitrary_real_certified
    @test !certificate.higher_dimension_certified
    @test !certificate.holed_domain_cohomology_certified
    @test !certificate.chemistry_extraction_certified
    @test certificate.canonical_identity == "sha256:" * bytes2hex(
        SHA.sha256(codeunits(certificate.canonical_payload)))
    @test _ROSF3_TEST.
        validate_ro_regular_extension_integrability_certificate_3d(
            complex, certificate)

    whole_domain = _ROSF3_TEST.ROInputDomain3D(
        (1, 2, 3), (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    whole_spec = _ROSF3_TEST.ROCellSpec3D(
        "whole", [1.0 0.0 0.0], [1.0], [1],
        [_rosf3_test_label(1, reshape([2.0, -1.0, 3.0], 1, 3), [4.0])])
    whole = _ROSF3_TEST.build_ro_cell_complex_3d(
        whole_domain, [whole_spec])
    whole_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            whole, [7])
    @test whole_certificate.status == :regular_cell_extension_integrable
    @test whole_certificate.checked_cell_count == 1
    @test whole_certificate.checked_internal_facet_count == 0
    @test whole_certificate.dual_graph_component_count == 1
    @test whole_certificate.dual_graph_cycle_rank == 0
    @test whole_certificate.checked_cycle_equation_count == 0

    reordered = _rosf3_split_complex(reverse_specs=true)
    reordered_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            reordered, [7])
    @test reordered.canonical_identity == complex.canonical_identity
    @test reordered_certificate.canonical_identity ==
        certificate.canonical_identity

    relabelled_output =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [8])
    @test relabelled_output.status == :regular_cell_extension_integrable
    @test relabelled_output.canonical_identity != certificate.canonical_identity

    @test_throws MethodError _ROSF3_TEST.ROIntegrabilityObstruction3D(
        :exact_tangential_mismatch, [1], [1, 2], 1, 7, "1//1")
    @test_throws MethodError _ROSF3_TEST.RORegularExtensionIntegrabilityCertificate3D()
end

@testset "D=3 exact obstructions do not admit tolerance repair" begin
    one_ulp_offset = nextfloat(-1.5)
    offset_complex = _rosf3_split_complex(
        right_offset=[one_ulp_offset])
    offset_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            offset_complex, [7])
    @test offset_certificate.status ==
        :regular_cell_extension_not_integrable
    @test offset_certificate.gradient_integrability_status ==
        :exact_dyadic_global_integrable
    @test offset_certificate.provided_potential_status ==
        :exact_dyadic_discontinuous
    @test offset_certificate.tangential_obstruction_count == 0
    @test offset_certificate.provided_offset_obstruction_count == 1
    @test :exact_provided_offset_obstruction in offset_certificate.reasons
    @test length(offset_certificate.witnesses) == 1
    offset_witness = only(offset_certificate.witnesses)
    @test offset_witness.kind == :exact_provided_offset_mismatch
    @test offset_witness.output_position == 1
    @test offset_witness.output_index == 7
    @test offset_witness.facet_ids == [1]
    @test offset_witness.cell_ids == [1, 2]
    @test offset_witness.exact_residual != "0//1"
    @test _ROSF3_TEST.
        validate_ro_regular_extension_integrability_certificate_3d(
            offset_complex, offset_certificate)

    tangent_complex = _rosf3_split_complex(
        right_matrix=reshape([4.0, 3.0, 3.0], 1, 3))
    tangent_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            tangent_complex, [7])
    @test tangent_certificate.status ==
        :regular_cell_extension_not_integrable
    @test tangent_certificate.gradient_integrability_status ==
        :exact_dyadic_tangential_obstruction
    @test tangent_certificate.provided_potential_status ==
        :exact_dyadic_discontinuous
    @test tangent_certificate.tangential_obstruction_count >= 1
    @test tangent_certificate.provided_offset_obstruction_count == 0
    @test :exact_tangential_obstruction in tangent_certificate.reasons
    @test any(witness ->
        witness.kind === :exact_tangential_mismatch,
        tangent_certificate.witnesses)

    left_mimo = [1.0 2.0 3.0; 0.0 0.0 0.0]
    right_mimo = [4.0 2.0 3.0; 0.0 1.0 1.0]
    mimo = _rosf3_split_complex(
        left_matrix=left_mimo,
        right_matrix=right_mimo,
        left_offset=[0.0, 0.0],
        right_offset=[-1.5, 0.0])
    mimo_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            mimo, [7, 11])
    @test mimo_certificate.output_indices == [7, 11]
    @test mimo_certificate.checked_tangential_equation_count == 4
    @test mimo_certificate.status ==
        :regular_cell_extension_not_integrable
    @test all(witness -> witness.output_index == 11,
        filter(witness -> witness.kind === :exact_tangential_mismatch,
            mimo_certificate.witnesses))

    truncated = _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
        mimo, [7, 11];
        limits=_ROSF3_TEST.ROStratifiedField3DLimits(max_witnesses=1))
    @test length(truncated.witnesses) == 1
    @test truncated.witnesses_truncated
    @test truncated.tangential_obstruction_count >= 1
end

@testset "D=3 exact dual cycles and multiway incidence" begin
    abc = _rosf3_abc_complex()
    certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(abc, [3])
    @test certificate.status == :regular_cell_extension_integrable
    @test certificate.checked_cell_count == 4
    @test certificate.checked_internal_facet_count == 6
    @test certificate.dual_graph_component_count == 1
    @test certificate.dual_graph_cycle_rank == 3
    @test certificate.checked_cycle_equation_count == 3
    @test certificate.cycle_obstruction_count == 0
    @test certificate.singular_stratum_count == 1
    @test certificate.regular_limit_only
    @test !certificate.includes_singular_branch
    @test _ROSF3_TEST.
        validate_ro_regular_extension_integrability_certificate_3d(
            abc, certificate)

    reordered = _rosf3_abc_complex(reverse_specs=true)
    reordered_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            reordered, [3])
    @test reordered.canonical_identity == abc.canonical_identity
    @test reordered_certificate.canonical_identity ==
        certificate.canonical_identity

    t_junction = _rosf3_t_junction_complex()
    t_certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            t_junction, [5])
    @test t_certificate.status == :regular_cell_extension_integrable
    @test t_certificate.checked_cell_count == 3
    @test t_certificate.checked_internal_facet_count == 3
    @test t_certificate.dual_graph_cycle_rank == 1
    @test t_certificate.checked_cycle_equation_count == 1

    zero = _ROSF3_TEST._RO3Exact(0)
    one = _ROSF3_TEST._RO3Exact(1)
    point0 = (zero, zero, zero)
    point1 = (zero, one, zero)
    point2 = (zero, zero, one)
    bases = _ROSF3_TEST._ROSF3FacetBasis[
        _ROSF3_TEST._ROSF3FacetBasis(10, (1, 2),
            (point0, point1, point2)),
        _ROSF3_TEST._ROSF3FacetBasis(20, (2, 3),
            (point0, point1, point2)),
        _ROSF3_TEST._ROSF3FacetBasis(30, (1, 3),
            (point0, point1, point2)),
    ]
    topology = _ROSF3_TEST._rosf3_dual_topology(3, bases)
    @test topology.component_count == 1
    @test count(topology.tree_edge) == 2
    @test length(topology.cycle_edges) == 1
    _, consistent = _ROSF3_TEST._rosf3_solve_dual_offsets(
        topology, bases, _ROSF3_TEST._RO3Exact[0, 1, 1])
    @test isempty(consistent)
    _, inconsistent = _ROSF3_TEST._rosf3_solve_dual_offsets(
        topology, bases, _ROSF3_TEST._RO3Exact[0, 0, 1])
    @test length(inconsistent) == 1
    edge_index, residual = only(inconsistent)
    @test edge_index in topology.cycle_edges
    @test residual == one
    facet_ids, cell_ids = _ROSF3_TEST._rosf3_fundamental_cycle(
        topology, bases, edge_index)
    @test facet_ids == [10, 20, 30]
    @test cell_ids == [1, 2, 3]
end

@testset "D=3 ambiguity remains unknown and validators fail closed" begin
    ambiguous = _rosf3_ambiguous_complex()
    certificate =
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            ambiguous, [7])
    @test certificate.status == :unknown
    @test certificate.gradient_integrability_status == :unknown
    @test certificate.provided_potential_status == :unknown
    @test :geometry_not_publishable in certificate.reasons
    @test :ambiguous_complex in certificate.reasons
    @test :set_valued_cell in certificate.reasons
    @test :invalid_cell_label_count in certificate.reasons
    @test certificate.regular_limit_only
    @test !certificate.includes_singular_branch
    @test _ROSF3_TEST.
        validate_ro_regular_extension_integrability_certificate_3d(
            ambiguous, certificate)

    complex = _rosf3_split_complex()
    stale = _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
        complex, [7])
    stale.output_indices[1] = 8
    @test_throws _ROSF3_TEST.ROStratifiedField3DValidationError begin
        _ROSF3_TEST.
            validate_ro_regular_extension_integrability_certificate_3d(
                complex, stale)
    end

    tampered_complex = _rosf3_split_complex()
    valid = _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
        tampered_complex, [7])
    tampered_complex.cells[1].labels[1].reaction_order_matrix[1, 1] += 1.0
    @test_throws _ROSF3_TEST.ROCellComplex3DClosureError begin
        _ROSF3_TEST.
            validate_ro_regular_extension_integrability_certificate_3d(
                tampered_complex, valid)
    end
end

@testset "D=3 output identity, limits, and cancellation" begin
    complex = _rosf3_split_complex()
    @test_throws ArgumentError begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, Int[])
    end
    @test_throws ArgumentError begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [true])
    end
    @test_throws ArgumentError begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [7, 7])
    end
    @test_throws DimensionMismatch begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [7, 8];
            limits=_ROSF3_TEST.ROStratifiedField3DLimits(
                max_geometry_exact_bit_work=1))
    end

    reads = Ref(0)
    oversized = _ROSF3OutputProbe(4_097, reads)
    @test_throws _ROSF3_TEST.ROStratifiedField3DLimitExceeded begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, oversized)
    end
    @test reads[] == 0

    early_reads = Ref(0)
    second_check = Ref(0)
    @test_throws _ROSF3TestCancelled begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex,
            _ROSF3CountingOutputProbe(100_000, early_reads);
            limits=_ROSF3_TEST.ROStratifiedField3DLimits(
                max_outputs=100_000),
            cancel_check=() -> begin
                second_check[] += 1
                second_check[] == 2 && throw(_ROSF3TestCancelled())
            end,
        )
    end
    @test second_check[] == 2
    @test early_reads[] <= 1

    for (limits, phase) in (
        (_ROSF3_TEST.ROStratifiedField3DLimits(max_cells=1), :cells),
        (_ROSF3_TEST.ROStratifiedField3DLimits(max_exact_basis_points=2),
            :exact_basis_points),
        (_ROSF3_TEST.ROStratifiedField3DLimits(max_exact_equations=3),
            :exact_equations),
        (_ROSF3_TEST.ROStratifiedField3DLimits(
            max_geometry_exact_bit_work=1), :geometry_exact_bit_work),
        (_ROSF3_TEST.ROStratifiedField3DLimits(max_exact_bit_work=1),
            :exact_bit_work),
        (_ROSF3_TEST.ROStratifiedField3DLimits(max_identity_bytes=1),
            :identity_reservation),
    )
        error = try
            _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
                complex, [7]; limits=limits)
            nothing
        catch caught
            caught
        end
        @test error isa _ROSF3_TEST.ROStratifiedField3DLimitExceeded
        @test error.phase == phase
    end

    abc_error = try
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            _rosf3_abc_complex(), [7];
            limits=_ROSF3_TEST.ROStratifiedField3DLimits(
                max_internal_facets=5))
        nothing
    catch caught
        caught
    end
    @test abc_error isa _ROSF3_TEST.ROStratifiedField3DLimitExceeded
    @test abc_error.phase == :internal_facets

    mimo = _rosf3_split_complex(
        left_matrix=[1.0 2.0 3.0; 0.0 0.0 0.0],
        right_matrix=[4.0 2.0 3.0; 0.0 0.0 0.0],
        left_offset=[0.0, 0.0], right_offset=[-1.5, 0.0])
    output_limit_error = try
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            mimo, [7, 8];
            limits=_ROSF3_TEST.ROStratifiedField3DLimits(max_outputs=1))
        nothing
    catch caught
        caught
    end
    @test output_limit_error isa
        _ROSF3_TEST.ROStratifiedField3DLimitExceeded
    @test output_limit_error.phase == :outputs

    @test_throws ArgumentError begin
        _ROSF3_TEST.ROStratifiedField3DLimits(max_cells=0)
    end
    @test_throws ArgumentError begin
        _ROSF3_TEST.ROStratifiedField3DLimits(max_cells=true)
    end

    entry_checks = Ref(0)
    @test_throws _ROSF3TestCancelled begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [7]; cancel_check=() -> begin
                entry_checks[] += 1
                throw(_ROSF3TestCancelled())
            end)
    end
    @test entry_checks[] == 1

    delayed_checks = Ref(0)
    @test_throws _ROSF3TestCancelled begin
        _ROSF3_TEST.certify_ro_regular_extension_integrability_3d(
            complex, [7]; cancel_check=() -> begin
                delayed_checks[] += 1
                delayed_checks[] == 100 && throw(_ROSF3TestCancelled())
            end)
    end
    @test delayed_checks[] == 100
end
