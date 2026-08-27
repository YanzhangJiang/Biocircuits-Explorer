module RORegularRootCensusContract

using Test
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function static_cubic_system(;
    limits=RORegularSheetLimits(),
    state_unit="concentration",
)
    # f(x,u) = -(x-1)(x-2)(x-3); u is a neutral control coordinate.
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=[state_unit],
        control_names=["u"],
        control_units=["concentration"],
        equations=[[
            term(-1.0, [3], [0]),
            term(6.0, [2], [0]),
            term(-11.0, [1], [0]),
            term(6.0, [0], [0]),
        ]],
        limits=limits,
    )
end

function static_cubic_patch(system, root)
    stable = root == 1.0 || root == 3.0
    return certify_ro_regular_sheet_patch(
        system;
        control_lower=[1.0],
        control_upper=[2.0],
        control_reference=[1.5],
        state_reference=[root],
        predictor_slope=[0.0;;],
        remainder_lower=[-0.25],
        remainder_upper=[0.25],
        preconditioner=[stable ? -0.5 : 1.0;;],
    )
end

function cubic_population()
    system = static_cubic_system()
    patches = [static_cubic_patch(system, root) for root in (1.0, 2.0, 3.0)]
    return system, patches
end

const CUBIC_BREAKS = [[
    -1.5,
    -1.25,
    -0.75,
    -0.5,
    -0.25,
    0.25,
    0.5,
    0.75,
    1.25,
    1.5,
]]

function cubic_census(system, patches; kwargs...)
    return certify_ro_complete_regular_root_census(
        system,
        patches;
        control_lower=[1.0],
        control_upper=[2.0],
        control_reference=[1.5],
        state_reference=[2.0],
        predictor_slope=[0.0;;],
        remainder_axis_breaks=CUBIC_BREAKS,
        kwargs...,
    )
end

function translated_two_input_system(; limits=RORegularSheetLimits())
    # f = -(y+y^3), y=x-u-2v.  Its only real root is x=u+2v.
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["concentration"],
        control_names=["u", "v"],
        control_units=["concentration", "concentration"],
        equations=[[
            term(-1.0, [3], [0, 0]),
            term(3.0, [2], [1, 0]),
            term(6.0, [2], [0, 1]),
            term(-3.0, [1], [2, 0]),
            term(-12.0, [1], [1, 1]),
            term(-12.0, [1], [0, 2]),
            term(1.0, [0], [3, 0]),
            term(6.0, [0], [2, 1]),
            term(12.0, [0], [1, 2]),
            term(8.0, [0], [0, 3]),
            term(-1.0, [1], [0, 0]),
            term(1.0, [0], [1, 0]),
            term(2.0, [0], [0, 1]),
        ]],
        limits=limits,
    )
end

function translated_two_input_fixture()
    system = translated_two_input_system()
    patch = certify_ro_regular_sheet_patch(
        system;
        control_lower=[1.0, 1.0],
        control_upper=[1.25, 1.125],
        control_reference=[1.125, 1.0625],
        state_reference=[3.25],
        predictor_slope=[1.0 2.0],
        remainder_lower=[-0.125],
        remainder_upper=[0.125],
        preconditioner=[-1.0;;],
    )
    census = certify_ro_complete_regular_root_census(
        system,
        [patch];
        control_lower=[1.0, 1.0],
        control_upper=[1.25, 1.125],
        control_reference=[1.125, 1.0625],
        state_reference=[3.25],
        predictor_slope=[1.0 2.0],
        remainder_axis_breaks=[[-0.5, -0.125, 0.125, 0.5]],
    )
    return system, patch, census
end

function affine_two_state_two_input_fixture()
    # F = (x-u, y-v).  In the common affine coordinate the residual is exactly
    # delta, so the central tensor cell contains the sole regular sheet and
    # every other cell is excluded by one of the two residual components.
    system = ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["u", "v"],
        control_units=["concentration", "concentration"],
        equations=[
            [
                term(1.0, [1, 0], [0, 0]),
                term(-1.0, [0, 0], [1, 0]),
            ],
            [
                term(1.0, [0, 1], [0, 0]),
                term(-1.0, [0, 0], [0, 1]),
            ],
        ],
    )
    patch = certify_ro_regular_sheet_patch(
        system;
        control_lower=[1.0, 1.0],
        control_upper=[1.125, 1.125],
        control_reference=[1.0, 1.0],
        state_reference=[1.0, 1.0],
        predictor_slope=[1.0 0.0; 0.0 1.0],
        remainder_lower=[-0.125, -0.125],
        remainder_upper=[0.125, 0.125],
        preconditioner=[1.0 0.0; 0.0 1.0],
    )
    census = certify_ro_complete_regular_root_census(
        system,
        [patch];
        control_lower=[1.0, 1.0],
        control_upper=[1.125, 1.125],
        control_reference=[1.0, 1.0],
        state_reference=[1.0, 1.0],
        predictor_slope=[1.0 0.0; 0.0 1.0],
        remainder_axis_breaks=[
            [-0.5, -0.125, 0.125, 0.5],
            [-0.5, -0.125, 0.125, 0.5],
        ],
    )
    return system, patch, census
end

function raw_census_with_cells(census, cells)
    raw = Any[getfield(census, field) for
        field in fieldnames(typeof(census))]
    fields = fieldnames(typeof(census))
    raw[findfirst(==(:cells), fields)] = cells
    pop!(raw)
    return BindingAndCatalysis.ROCompleteRegularRootCensus(
        BindingAndCatalysis._RORC_VALIDATED_TOKEN,
        raw...,
    )
end

@testset "P8s1a complete regular-root census contract" begin
    @testset "complete three-sheet census and empty fold set" begin
        system, patches = cubic_population()
        census = cubic_census(system, patches)
        @test census.version == RO_COMPLETE_REGULAR_ROOT_CENSUS_VERSION
        @test census.version ==
            "bne-ro-complete-regular-root-census/v1.0.0"
        @test census.evidence_scope ==
            RO_COMPLETE_REGULAR_ROOT_CENSUS_SCOPE
        @test census.system_declaration_sha256 == system.declaration_sha256
        @test census.root_count_per_control == 3
        @test census.regular_sheet_count == 3
        @test census.partition_cell_count == 9
        @test census.unique_root_cell_count == 3
        @test census.root_free_cell_count == 6
        @test length(census.cells) == 9
        @test census.root_population_complete_inside_declared_domain
        @test census.regular_sheet_population_complete_inside_declared_domain
        @test census.continuation_complete_inside_declared_control_box
        @test census.fold_event_set_complete_inside_declared_domain
        @test census.fold_event_count == 0
        @test census.all_roots_regular_inside_declared_domain
        @test !census.roots_outside_declared_domain_excluded
        @test !census.stable_root_population_complete
        @test !census.hopf_event_set_complete
        @test !census.global_continuation_certified
        @test !census.true_hysteresis_certified
        @test census.source_replay_interval_operation_count ==
            sum(patch.exact_operation_count for patch in patches)
        @test census.analysis_interval_operation_count > 0
        @test occursin(r"^[0-9a-f]{64}$", census.certificate_sha256)

        classifications = [cell.classification for cell in census.cells]
        @test classifications == [
            :root_free_by_residual_exclusion,
            :unique_regular_root,
            :root_free_by_residual_exclusion,
            :root_free_by_residual_exclusion,
            :unique_regular_root,
            :root_free_by_residual_exclusion,
            :root_free_by_residual_exclusion,
            :unique_regular_root,
            :root_free_by_residual_exclusion,
        ]
        @test [cell.grid_index for cell in census.cells] ==
            [(index,) for index in 1:9]
        unique_cells = [cell for cell in census.cells if
            cell.classification == :unique_regular_root]
        @test Set(cell.patch_certificate_sha256 for cell in unique_cells) ==
            Set(patch.certificate_sha256 for patch in patches)
        @test all(cell.excluding_equation_index == 0 for cell in unique_cells)
        @test all(all(interval -> interval.lower <= 0 <= interval.upper,
            cell.residual_enclosure) for cell in unique_cells)
        free_cells = [cell for cell in census.cells if
            cell.classification == :root_free_by_residual_exclusion]
        @test all(cell.excluding_equation_index == 1 for cell in free_cells)
        @test all(cell.patch_certificate_sha256 == "" for cell in free_cells)
        @test all(occursin(r"^[0-9a-f]{64}$", cell.evidence_sha256)
            for cell in census.cells)

        @test validate_ro_complete_regular_root_census(
            system, patches, census)
        replayed = replay_ro_complete_regular_root_census(
            system, reverse(patches), census)
        @test replayed.certificate_sha256 == census.certificate_sha256
        reordered = cubic_census(system, reverse(patches))
        @test reordered.certificate_sha256 == census.certificate_sha256
        @test census.patch_certificate_sha256s == Tuple(sort!([
            patch.certificate_sha256 for patch in patches]))
    end

    @testset "nonlinear two-input translated sheet stays complete" begin
        system, patch, census = translated_two_input_fixture()
        @test census.root_count_per_control == 1
        @test census.partition_cell_count == 3
        @test census.unique_root_cell_count == 1
        @test census.root_free_cell_count == 2
        @test census.predictor_slope[1, 1] == 1
        @test census.predictor_slope[1, 2] == 2
        @test census.control_box == (
            ROExactInterval(1.0, 1.25),
            ROExactInterval(1.0, 1.125),
        )
        root_cell = only(cell for cell in census.cells if
            cell.classification == :unique_regular_root)
        @test root_cell.grid_index == (2,)
        @test root_cell.patch_certificate_sha256 == patch.certificate_sha256
        @test root_cell.remainder_box ==
            (ROExactInterval(-0.125, 0.125),)
        @test validate_ro_complete_regular_root_census(
            system, [patch], census)
    end

    @testset "two-state two-input tensor cover chooses either equation" begin
        system, patch, census = affine_two_state_two_input_fixture()
        @test census.root_count_per_control == 1
        @test census.partition_cell_count == 9
        @test census.unique_root_cell_count == 1
        @test census.root_free_cell_count == 8
        @test all(length(cell.remainder_box) == 2 &&
            length(cell.residual_enclosure) == 2 for cell in census.cells)
        root_cell = only(cell for cell in census.cells if
            cell.classification == :unique_regular_root)
        @test root_cell.grid_index == (2, 2)
        @test root_cell.patch_certificate_sha256 == patch.certificate_sha256
        @test Dict(cell.grid_index => cell.excluding_equation_index
            for cell in census.cells) == Dict(
                (1, 1) => 1,
                (2, 1) => 2,
                (3, 1) => 1,
                (1, 2) => 1,
                (2, 2) => 0,
                (3, 2) => 1,
                (1, 3) => 1,
                (2, 3) => 2,
                (3, 3) => 1,
            )
        @test validate_ro_complete_regular_root_census(
            system, [patch], census)
    end

    @testset "zero-root census is a complete bounded negative result" begin
        system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["u"],
            control_units=["1"],
            equations=[[
                term(1.0, [1], [0]),
                term(-1.0, [0], [1]),
            ]],
        )
        census = certify_ro_complete_regular_root_census(
            system,
            RORegularSheetPatchCertificate[];
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[8.5],
            predictor_slope=[0.0;;],
            remainder_axis_breaks=[[-0.5, 0.5]],
        )
        @test census.root_count_per_control == 0
        @test census.regular_sheet_count == 0
        @test census.partition_cell_count == 1
        @test census.root_free_cell_count == 1
        @test census.root_population_complete_inside_declared_domain
        @test census.fold_event_set_complete_inside_declared_domain
        @test !census.roots_outside_declared_domain_excluded
        @test validate_ro_complete_regular_root_census(
            system, RORegularSheetPatchCertificate[], census)
    end

    @testset "partition and source mismatches fail closed" begin
        system, patches = cubic_population()
        unresolved = try
            certify_ro_complete_regular_root_census(
                system,
                patches;
                control_lower=[1.0],
                control_upper=[2.0],
                control_reference=[1.5],
                state_reference=[2.0],
                predictor_slope=[0.0;;],
                remainder_axis_breaks=[[-1.5, 1.5]],
            )
            nothing
        catch caught
            caught
        end
        @test unresolved isa RORegularRootCensusRejected
        @test unresolved.reason == :patch_tube_not_a_partition_cell

        missing = try
            cubic_census(system, patches[1:2])
            nothing
        catch caught
            caught
        end
        @test missing isa RORegularRootCensusRejected
        @test missing.reason == :unresolved_partition_cell

        misaligned_patch = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[1.0],
            predictor_slope=[0.0;;],
            remainder_lower=[-0.125],
            remainder_upper=[0.125],
            preconditioner=[-0.5;;],
        )
        alignment = try
            cubic_census(system, [misaligned_patch, patches[2], patches[3]])
            nothing
        catch caught
            caught
        end
        @test alignment isa RORegularRootCensusRejected
        @test alignment.reason == :patch_tube_not_a_partition_cell

        narrow_patch = certify_ro_regular_sheet_patch(
            system;
            control_lower=[1.0],
            control_upper=[1.5],
            control_reference=[1.25],
            state_reference=[1.0],
            predictor_slope=[0.0;;],
            remainder_lower=[-0.25],
            remainder_upper=[0.25],
            preconditioner=[-0.5;;],
        )
        control_mismatch = try
            cubic_census(system, [narrow_patch, patches[2], patches[3]])
            nothing
        catch caught
            caught
        end
        @test control_mismatch isa RORegularRootCensusRejected
        @test control_mismatch.reason == :patch_control_box_mismatch

        @test_throws ArgumentError cubic_census(
            system, [patches; patches[1]])
        @test_throws ArgumentError certify_ro_complete_regular_root_census(
            system,
            patches;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[2.0],
            predictor_slope=[0.0;;],
            remainder_axis_breaks=[[-1.5, -0.25, -0.5, 1.5]],
        )
        @test_throws ArgumentError certify_ro_complete_regular_root_census(
            system,
            patches;
            control_lower=[1.0],
            control_upper=[2.0],
            control_reference=[1.5],
            state_reference=[2.0],
            predictor_slope=[0.0;;],
            remainder_axis_breaks=[Any[-1, 1.0]],
        )

        other = static_cubic_system(state_unit="other")
        foreign = static_cubic_patch(other, 1.0)
        @test_throws ArgumentError cubic_census(
            system, [foreign, patches[2], patches[3]])
    end

    @testset "identity, raw forgery, resources, and cancellation" begin
        system, patches = cubic_population()
        census = cubic_census(system, patches)
        raw = Any[getfield(census, field) for
            field in fieldnames(typeof(census))]
        @test_throws MethodError ROCompleteRegularRootCensus(raw...)
        outside_index = findfirst(
            ==(:roots_outside_declared_domain_excluded),
            fieldnames(typeof(census)),
        )
        forged = copy(raw)
        forged[outside_index] = true
        pop!(forged)
        @test_throws ArgumentError BindingAndCatalysis.ROCompleteRegularRootCensus(
            BindingAndCatalysis._RORC_VALIDATED_TOKEN,
            forged...,
        )

        first_cell = census.cells[1]
        wrong_box_cell = BindingAndCatalysis._rorc_make_cell(
            first_cell.linear_index,
            first_cell.grid_index,
            (ROExactInterval(-1.5, -1.0),),
            first_cell.classification,
            first_cell.excluding_equation_index,
            first_cell.residual_enclosure,
            first_cell.patch_certificate_sha256,
        )
        wrong_box_cells = (wrong_box_cell, census.cells[2:end]...)
        wrong_box_error = try
            raw_census_with_cells(census, wrong_box_cells)
            nothing
        catch caught
            caught
        end
        @test wrong_box_error isa ArgumentError
        @test occursin("cell bounds do not match their axis",
            sprint(showerror, wrong_box_error))

        wrong_residual_cell = BindingAndCatalysis._rorc_make_cell(
            first_cell.linear_index,
            first_cell.grid_index,
            first_cell.remainder_box,
            first_cell.classification,
            first_cell.excluding_equation_index,
            (
                first_cell.residual_enclosure...,
                ROExactInterval(1.0, 2.0),
            ),
            first_cell.patch_certificate_sha256,
        )
        wrong_residual_cells = (
            wrong_residual_cell,
            census.cells[2:end]...,
        )
        @test_throws DimensionMismatch raw_census_with_cells(
            census, wrong_residual_cells)
        @test_throws ArgumentError replay_ro_complete_regular_root_census(
            system, patches[1:2], census)

        @test_throws RORegularRootCensusLimitExceeded cubic_census(
            system,
            patches;
            limits=RORegularRootCensusLimits(max_cells=8),
        )
        @test_throws RORegularRootCensusLimitExceeded cubic_census(
            system,
            patches;
            limits=RORegularRootCensusLimits(max_patches=2),
        )
        source_cap = sum(patch.exact_operation_count for patch in patches) - 1
        source_error = try
            cubic_census(
                system,
                patches;
                limits=RORegularRootCensusLimits(
                    max_source_replay_interval_operations=source_cap),
            )
            nothing
        catch caught
            caught
        end
        @test source_error isa RORegularRootCensusLimitExceeded
        @test source_error.phase == :source_replay_interval_operations
        @test_throws RORegularRootCensusLimitExceeded cubic_census(
            system,
            patches;
            limits=RORegularRootCensusLimits(
                max_analysis_interval_operations=10),
        )
        @test_throws RORegularRootCensusLimitExceeded cubic_census(
            system,
            patches;
            limits=RORegularRootCensusLimits(
                max_axis_breakpoints_per_state=4),
        )

        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 3 && throw(InterruptException())
        end
        @test_throws InterruptException cubic_census(
            system, patches; cancel_check=cancel)
        @test calls[] == 3
    end
end

end # module
