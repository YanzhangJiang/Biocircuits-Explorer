module ROFoldEventCensusContract

using Test
using BindingAndCatalysis

term(coefficient, state_exponents, control_exponents) =
    ROPolynomialTerm(coefficient, state_exponents, control_exponents)

function scalar_fold_system(; limits=RORegularSheetLimits())
    # F(x, lambda) = (x-1)^2 - (lambda-1).
    return ROPolynomialEquilibriumSystem(
        state_names=["x"],
        state_units=["concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[[
            term(1.0, [2], [0]),
            term(-2.0, [1], [0]),
            term(2.0, [0], [0]),
            term(-1.0, [0], [1]),
        ]],
        limits=limits,
    )
end

const SCALAR_FOLD_AXES = [
    [0.5, 0.75, 1.25, 1.5],
    [0.5, 0.75, 1.25, 1.5],
]

const SCALAR_FOLD_PRECONDITIONER = [0.0 0.5; -1.0 0.0]

function scalar_fold_census(system=scalar_fold_system(); kwargs...)
    return certify_ro_complete_simple_fold_event_census(
        system;
        event_axis_breaks=SCALAR_FOLD_AXES,
        event_grid_indices=[(2, 2)],
        event_preconditioners=[SCALAR_FOLD_PRECONDITIONER],
        kwargs...,
    )
end

function two_state_fold_system(; limits=RORegularSheetLimits())
    # F1=(x-1)^2-(lambda-1), F2=y-2.  The only singular equilibrium in
    # the declared domain is the simple fold (x,y,lambda)=(1,2,1).
    return ROPolynomialEquilibriumSystem(
        state_names=["x", "y"],
        state_units=["concentration", "concentration"],
        control_names=["lambda"],
        control_units=["concentration"],
        equations=[
            [
                term(1.0, [2, 0], [0]),
                term(-2.0, [1, 0], [0]),
                term(2.0, [0, 0], [0]),
                term(-1.0, [0, 0], [1]),
            ],
            [
                term(1.0, [0, 1], [0]),
                term(-2.0, [0, 0], [0]),
            ],
        ],
        limits=limits,
    )
end

function two_state_fold_census(system=two_state_fold_system(); kwargs...)
    return certify_ro_complete_simple_fold_event_census(
        system;
        event_axis_breaks=[
            [0.5, 0.75, 1.25, 1.5],
            [1.5, 1.75, 2.25, 2.5],
            [0.5, 0.75, 1.25, 1.5],
        ],
        event_grid_indices=[(2, 2, 2)],
        event_preconditioners=[
            [0.0 0.0 0.5; 0.0 1.0 0.0; -1.0 0.0 0.0],
        ],
        kwargs...,
    )
end

function raw_census_with_flag(census, field::Symbol, value)
    fields = fieldnames(typeof(census))
    raw = Any[getfield(census, name) for name in fields]
    raw[findfirst(==(field), fields)] = value
    return BindingAndCatalysis.ROCompleteSimpleFoldEventCensus(
        BindingAndCatalysis._ROFE_VALIDATED_TOKEN,
        raw...,
    )
end

@testset "P8s1b0 complete simple-fold event census contract" begin
    @testset "one scalar fold is isolated and the complete event set is bound" begin
        system = scalar_fold_system()
        census = scalar_fold_census(system)
        @test census.version == RO_SIMPLE_FOLD_EVENT_CENSUS_VERSION
        @test census.version == "bne-ro-simple-fold-event-census/v1.0.0"
        @test census.evidence_scope == RO_SIMPLE_FOLD_EVENT_CENSUS_SCOPE
        @test census.system_declaration_sha256 == system.declaration_sha256
        @test census.augmented_variable_names == ("x", "lambda")
        @test census.augmented_variable_units ==
            ("concentration", "concentration")
        @test census.partition_cell_count == 9
        @test census.fold_event_count == 1
        @test census.unique_event_cell_count == 1
        @test census.fold_free_cell_count == 8
        @test census.fold_event_set_complete_inside_declared_domain
        @test census.all_fold_events_simple_inside_declared_domain
        @test census.singular_equilibrium_set_complete_inside_declared_domain
        @test !census.root_population_complete_inside_declared_domain
        @test !census.adjacent_regular_sheet_incidence_certified
        @test !census.stable_root_population_complete
        @test !census.hopf_event_set_complete
        @test !census.global_continuation_certified
        @test !census.true_hysteresis_certified
        @test occursin(r"^[0-9a-f]{64}$", census.certificate_sha256)

        event = only(census.events)
        @test event.grid_index == (2, 2)
        @test event.event_box == (
            ROExactInterval(0.75, 1.25),
            ROExactInterval(0.75, 1.25),
        )
        @test event.center == (1, 1)
        @test event.contraction_beta == 1 // 2
        @test event.unique_augmented_root_inside_event_box
        @test event.state_jacobian_corank_one_certified
        @test event.control_transversality_certified
        @test event.quadratic_nondegeneracy_certified
        @test event.simple_fold_certified
        @test event.fold_point_strictly_inside_event_box
        @test all(interval -> interval.lower > 0, event.event_box)
        @test all(interval -> interval.lower <= 0 <= interval.upper,
            event.krawczyk_offset_image)
        @test occursin(r"^[0-9a-f]{64}$", event.certificate_sha256)

        classifications = Dict(cell.grid_index => cell.classification
            for cell in census.cells)
        @test classifications[(2, 2)] == :unique_simple_fold_event
        @test count(==(:fold_free_by_augmented_residual_exclusion),
            values(classifications)) == 8
        excluding = Dict(cell.grid_index =>
            cell.excluding_augmented_equation_index for cell in census.cells)
        @test excluding == Dict(
            (1, 1) => 2,
            (2, 1) => 1,
            (3, 1) => 2,
            (1, 2) => 2,
            (2, 2) => 0,
            (3, 2) => 2,
            (1, 3) => 2,
            (2, 3) => 1,
            (3, 3) => 2,
        )
        event_cell = census.cells[5]
        @test event_cell.event_certificate_sha256 == event.certificate_sha256
        @test all(interval -> interval.lower <= 0 <= interval.upper,
            event_cell.augmented_residual_enclosure)

        @test validate_ro_complete_simple_fold_event_census(system, census)
        replayed = replay_ro_complete_simple_fold_event_census(system, census)
        @test replayed.certificate_sha256 == census.certificate_sha256
        @test replayed.events[1].certificate_sha256 == event.certificate_sha256
    end

    @testset "two-state determinant and three-axis cover are exact" begin
        system = two_state_fold_system()
        census = two_state_fold_census(system)
        @test census.augmented_variable_names == ("x", "y", "lambda")
        @test census.partition_cell_count == 27
        @test census.fold_event_count == 1
        @test census.fold_free_cell_count == 26
        event = only(census.events)
        @test event.grid_index == (2, 2, 2)
        @test event.center == (1, 2, 1)
        @test length(event.augmented_residual_enclosure) == 3
        @test size(event.augmented_jacobian_enclosure) == (3, 3)
        @test event.contraction_beta == 1 // 2
        @test all(length(cell.augmented_residual_enclosure) == 3
            for cell in census.cells)
        @test Set(cell.excluding_augmented_equation_index for cell in
            census.cells if
            cell.classification == :fold_free_by_augmented_residual_exclusion) ==
            Set((1, 2, 3))
        @test validate_ro_complete_simple_fold_event_census(system, census)
    end

    @testset "a regular-root domain can certify an empty fold-event set" begin
        system = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["lambda"],
            control_units=["1"],
            equations=[[
                term(1.0, [1], [0]),
                term(-1.0, [0], [1]),
            ]],
        )
        census = certify_ro_complete_simple_fold_event_census(
            system;
            event_axis_breaks=[[1.0, 2.0], [1.0, 2.0]],
            event_grid_indices=Tuple[],
            event_preconditioners=Matrix{Float64}[],
        )
        @test census.fold_event_count == 0
        @test census.partition_cell_count == 1
        @test census.fold_free_cell_count == 1
        @test census.fold_event_set_complete_inside_declared_domain
        @test census.all_fold_events_simple_inside_declared_domain
        @test !census.root_population_complete_inside_declared_domain
        @test validate_ro_complete_simple_fold_event_census(system, census)
    end

    @testset "degenerate and incomplete event covers fail closed" begin
        cusp = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["lambda"],
            control_units=["1"],
            equations=[[
                term(1.0, [3], [0]),
                term(-3.0, [2], [0]),
                term(3.0, [1], [0]),
                term(-1.0, [0], [1]),
            ]],
        )
        degenerate = try
            certify_ro_complete_simple_fold_event_census(
                cusp;
                event_axis_breaks=SCALAR_FOLD_AXES,
                event_grid_indices=[(2, 2)],
                event_preconditioners=[SCALAR_FOLD_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test degenerate isa ROFoldEventCensusRejected
        @test degenerate.reason == :augmented_contraction_not_proven

        boundary_event = try
            certify_ro_complete_simple_fold_event_census(
                scalar_fold_system();
                event_axis_breaks=[
                    [0.75, 1.0, 1.25],
                    [0.75, 1.25],
                ],
                event_grid_indices=[(1, 1)],
                event_preconditioners=[SCALAR_FOLD_PRECONDITIONER],
            )
            nothing
        catch caught
            caught
        end
        @test boundary_event isa ROFoldEventCensusRejected
        @test boundary_event.reason ==
            :augmented_krawczyk_inclusion_not_proven

        system = scalar_fold_system()
        unresolved = try
            certify_ro_complete_simple_fold_event_census(
                system;
                event_axis_breaks=SCALAR_FOLD_AXES,
                event_grid_indices=Tuple[],
                event_preconditioners=Matrix{Float64}[],
            )
            nothing
        catch caught
            caught
        end
        @test unresolved isa ROFoldEventCensusRejected
        @test unresolved.reason == :unresolved_event_partition_cell

        @test_throws ArgumentError certify_ro_complete_simple_fold_event_census(
            system;
            event_axis_breaks=SCALAR_FOLD_AXES,
            event_grid_indices=[(2, 2), (2, 2)],
            event_preconditioners=[
                SCALAR_FOLD_PRECONDITIONER,
                SCALAR_FOLD_PRECONDITIONER,
            ],
        )
        @test_throws ArgumentError certify_ro_complete_simple_fold_event_census(
            system;
            event_axis_breaks=[Any[0.5, 1], [0.5, 1.5]],
            event_grid_indices=Tuple[],
            event_preconditioners=Matrix{Float64}[],
        )

        two_controls = ROPolynomialEquilibriumSystem(
            state_names=["x"],
            state_units=["1"],
            control_names=["lambda", "mu"],
            control_units=["1", "1"],
            equations=[[
                term(1.0, [1], [0, 0]),
                term(-1.0, [0], [1, 0]),
            ]],
        )
        @test_throws ArgumentError certify_ro_complete_simple_fold_event_census(
            two_controls;
            event_axis_breaks=[[1.0, 2.0], [1.0, 2.0]],
            event_grid_indices=Tuple[],
            event_preconditioners=Matrix{Float64}[],
        )
    end

    @testset "identity, resources, replay, and cancellation remain bounded" begin
        system = scalar_fold_system()
        census = scalar_fold_census(system)
        raw = Any[getfield(census, field) for
            field in fieldnames(typeof(census))]
        @test_throws MethodError ROCompleteSimpleFoldEventCensus(raw...)
        @test_throws ArgumentError raw_census_with_flag(
            census, :adjacent_regular_sheet_incidence_certified, true)

        foreign = scalar_fold_system(limits=RORegularSheetLimits(
            max_interval_operations=1_999_999))
        @test_throws ArgumentError replay_ro_complete_simple_fold_event_census(
            foreign, census)

        @test_throws ROFoldEventCensusLimitExceeded scalar_fold_census(
            system;
            limits=ROFoldEventCensusLimits(max_cells=8),
        )
        @test_throws ROFoldEventCensusLimitExceeded scalar_fold_census(
            system;
            limits=ROFoldEventCensusLimits(max_events=0),
        )
        @test_throws ROFoldEventCensusLimitExceeded scalar_fold_census(
            system;
            limits=ROFoldEventCensusLimits(
                max_axis_breakpoints_per_variable=3),
        )
        @test_throws ROFoldEventCensusLimitExceeded scalar_fold_census(
            system;
            limits=ROFoldEventCensusLimits(max_interval_operations=10),
        )

        calls = Ref(0)
        cancel = () -> begin
            calls[] += 1
            calls[] >= 3 && throw(InterruptException())
        end
        @test_throws InterruptException scalar_fold_census(
            system; cancel_check=cancel)
        @test calls[] == 3
    end
end

end # module
