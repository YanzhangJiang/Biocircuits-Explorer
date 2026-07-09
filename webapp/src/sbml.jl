# ─── SBML interoperability (subset codec) ─────────────────────────────────────
#
# Bridges NetworkIR <-> SBML Level 3 Core for the binding-network subset we
# model: species, reversible reactions, and per-reaction dissociation
# constants (Kd). This is intentionally NOT a full SBML implementation —
# arbitrary kinetic laws, MathML rate expressions, units, events, rules, and
# function definitions are out of scope and surfaced as import warnings.
#
# Loaded as a flat include in BiocircuitsExplorerBackend after ir.jl, so it
# uses NetworkIR/SpeciesDecl/ReactionDecl and the already-imported
# parse_term. EzXML is referenced qualified (`EzXML.foo`) to avoid dragging
# its generic exports (root, nodename, …) into the module namespace.

const SBML_NS_L3V2 = "http://www.sbml.org/sbml/level3/version2/core"
const _SBML_ARROW_RE = r"<->|<=>|↔"
const _SBML_SID_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"

# ── XML text escaping (export) ──
function _xml_escape(s)
    str = string(s)
    str = replace(str, "&" => "&amp;")
    str = replace(str, "<" => "&lt;")
    str = replace(str, ">" => "&gt;")
    str = replace(str, "\"" => "&quot;")
    str = replace(str, "'" => "&apos;")
    return str
end

# SBML SId must match [A-Za-z_][A-Za-z0-9_]*. Formula species produced by the
# reaction parser already satisfy this; free-form labels and imported metadata
# are sanitized before they are emitted as identifiers.
function _sanitize_sid(s; fallback = "id")
    str = string(s)
    isempty(str) && return fallback
    cleaned = replace(str, r"[^A-Za-z0-9_]" => "_")
    occursin(r"^[A-Za-z_]", cleaned) || (cleaned = "_" * cleaned)
    return cleaned
end

# Keep SBML's machine identifier and optional display name namespaced inside
# NetworkIR metadata.  NetworkIR formulas must use the identifier (species
# references in SBML point to `id`, never `name`), while the human-readable
# name is preserved for a lossless export/import cycle.
function _sbml_identity_metadata(id::AbstractString, name::AbstractString)
    return Dict{String, Any}(
        "sbml" => Dict{String, Any}("id" => String(id), "name" => String(name)),
    )
end

function _sbml_identity(metadata, fallback_id::AbstractString, fallback_name::AbstractString)
    sbml = metadata isa AbstractDict ? get(metadata, "sbml", nothing) : nothing
    raw_id = sbml isa AbstractDict ? get(sbml, "id", fallback_id) : fallback_id
    raw_name = sbml isa AbstractDict ? get(sbml, "name", fallback_name) : fallback_name
    candidate = raw_id isa AbstractString ? String(raw_id) : String(fallback_id)
    sid = occursin(_SBML_SID_RE, candidate) ? candidate :
          _sanitize_sid(candidate; fallback = _sanitize_sid(fallback_id))
    name = raw_name isa AbstractString ? String(raw_name) : String(fallback_name)
    return sid, name
end
function _claim_unique_sid!(seen::Set{String}, sid::String, kind::AbstractString)
    sid in seen && throw(ArgumentError("Duplicate SBML $kind id after sanitization: $sid"))
    push!(seen, sid)
    return sid
end

# ── Formula <-> sides ──

# Parse one side ("A + 2 B") into ordered (species, stoichiometry) pairs,
# preserving author order for deterministic round-trips.
function _parse_side_ordered(side::AbstractString)
    terms = Tuple{String, Int}[]
    for part in split(side, "+")
        isempty(strip(part)) && continue
        sym, coeff = parse_term(part)   # from ReactionParser, imported flat
        push!(terms, (String(sym), coeff))
    end
    return terms
end

function _split_formula(formula::AbstractString)
    m = match(_SBML_ARROW_RE, formula)
    m === nothing && throw(ArgumentError("Reaction formula missing '<->': $formula"))
    left, right = split(formula, m.match; limit = 2)
    return _parse_side_ordered(left), _parse_side_ordered(right)
end

function _side_to_string(terms::Vector{Tuple{String, Int}})
    isempty(terms) && return ""
    parts = [c == 1 ? sp : "$c $sp" for (sp, c) in terms]
    return join(parts, " + ")
end

# ── Export: NetworkIR -> SBML string ──

"""
    network_ir_to_sbml(ir::NetworkIR) -> String

Render a NetworkIR as an SBML Level 3 Version 2 Core document. Every species
referenced by a reaction is declared (union of `ir.species` and formula
species). Each reaction's `kd` is emitted both as a position-stable global
parameter (`Kd_r1`, `Kd_r2`, …) and inside the reaction's kineticLaw, so standard tools and
our own importer can both recover it. SBML `id`/`name` pairs previously stored
under component `metadata["sbml"]` and model `extensions["sbml"]` are restored.
"""
function network_ir_to_sbml(ir::NetworkIR)
    # Gather every species: declared ones first (preserving their order and
    # initial totals), then any extra species that only appear in formulas.
    initial_by_name = Dict{String, Union{Nothing, Float64}}()
    metadata_by_name = Dict{String, Dict{String, Any}}()
    ordered_species = String[]
    seen = Set{String}()
    for sp in ir.species
        if !(sp.name in seen)
            push!(ordered_species, sp.name); push!(seen, sp.name)
        end
        initial_by_name[sp.name] = sp.initial_total
        metadata_by_name[sp.name] = sp.metadata
    end

    reaction_sides = Vector{Tuple{Vector{Tuple{String,Int}}, Vector{Tuple{String,Int}}}}()
    for rx in ir.reactions
        lhs, rhs = _split_formula(rx.formula)
        push!(reaction_sides, (lhs, rhs))
        for (sp, _) in vcat(lhs, rhs)
            if !(sp in seen)
                push!(ordered_species, sp); push!(seen, sp)
            end
        end
    end

    # Formula symbols are NetworkIR identifiers. Map them back to preserved
    # SBML SIds for speciesReference attributes, checking that sanitization did
    # not collapse two distinct symbols onto one id.
    species_id_by_name = Dict{String, String}()
    species_display_by_name = Dict{String, String}()
    used_species_ids = Set{String}()
    for name in ordered_species
        sid, display_name = _sbml_identity(
            get(metadata_by_name, name, Dict{String, Any}()), name, name)
        _claim_unique_sid!(used_species_ids, sid, "species")
        species_id_by_name[name] = sid
        species_display_by_name[name] = display_name
    end

    reaction_ids = String[]
    reaction_names = String[]
    used_reaction_ids = Set{String}()
    for (i, rx) in enumerate(ir.reactions)
        rid, rname = _sbml_identity(rx.metadata, "r$(i)", "")
        push!(reaction_ids, _claim_unique_sid!(used_reaction_ids, rid, "reaction"))
        push!(reaction_names, rname)
    end

    io = IOBuffer()
    println(io, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
    println(io, "<sbml xmlns=\"$SBML_NS_L3V2\" level=\"3\" version=\"2\">")
    preserved_model_identity = get(ir.extensions, "sbml", nothing)
    model_metadata = preserved_model_identity isa AbstractDict ?
        Dict{String, Any}("sbml" => preserved_model_identity) : Dict{String, Any}()
    model_id, model_name = _sbml_identity(
        model_metadata,
        _sanitize_sid(ir.label; fallback = "biocircuits_model"), ir.label)
    label_attr = isempty(model_name) ? "" : " name=\"$(_xml_escape(model_name))\""
    println(io, "  <model id=\"$model_id\"$label_attr>")

    # Single well-mixed compartment.
    println(io, "    <listOfCompartments>")
    println(io, "      <compartment id=\"default\" name=\"default\" spatialDimensions=\"3\" size=\"1\" constant=\"true\"/>")
    println(io, "    </listOfCompartments>")

    # Species.
    println(io, "    <listOfSpecies>")
    for name in ordered_species
        init = get(initial_by_name, name, nothing)
        init_attr = init === nothing ? "" : " initialConcentration=\"$(init)\""
        sid = species_id_by_name[name]
        display_name = species_display_by_name[name]
        name_attr = isempty(display_name) ? "" : " name=\"$(_xml_escape(display_name))\""
        println(io, "      <species id=\"$sid\"$name_attr compartment=\"default\"$init_attr " *
                    "hasOnlySubstanceUnits=\"false\" boundaryCondition=\"false\" constant=\"false\"/>")
    end
    println(io, "    </listOfSpecies>")

    # Parameters: per-reaction Kd, plus any point-valued parameter
    # distributions. Non-point distributions can't be expressed in core SBML;
    # they're noted in the model <notes> instead.
    println(io, "    <listOfParameters>")
    for (i, rx) in enumerate(ir.reactions)
        println(io, "      <parameter id=\"Kd_r$(i)\" name=\"Kd for r$(i)\" value=\"$(rx.kd)\" constant=\"true\"/>")
    end
    nonpoint_notes = String[]
    for pd in ir.parameter_distributions
        if pd.kind == :point && pd.value !== nothing
            pid, pname = _sbml_identity(pd.metadata, _sanitize_sid(pd.symbol), "")
            pname_attr = isempty(pname) ? "" : " name=\"$(_xml_escape(pname))\""
            println(io, "      <parameter id=\"$pid\"$pname_attr value=\"$(pd.value)\" constant=\"true\"/>")
        else
            push!(nonpoint_notes, "$(pd.symbol): $(pd.kind)")
        end
    end
    println(io, "    </listOfParameters>")

    # Reactions.
    println(io, "    <listOfReactions>")
    for (i, rx) in enumerate(ir.reactions)
        lhs, rhs = reaction_sides[i]
        rev = rx.reversible ? "true" : "false"
        reaction_name_attr = isempty(reaction_names[i]) ? "" :
            " name=\"$(_xml_escape(reaction_names[i]))\""
        println(io, "      <reaction id=\"$(reaction_ids[i])\"$reaction_name_attr reversible=\"$rev\" compartment=\"default\">")
        if !isempty(lhs)
            println(io, "        <listOfReactants>")
            for (sp, c) in lhs
                println(io, "          <speciesReference species=\"$(species_id_by_name[sp])\" stoichiometry=\"$(c)\" constant=\"true\"/>")
            end
            println(io, "        </listOfReactants>")
        end
        if !isempty(rhs)
            println(io, "        <listOfProducts>")
            for (sp, c) in rhs
                println(io, "          <speciesReference species=\"$(species_id_by_name[sp])\" stoichiometry=\"$(c)\" constant=\"true\"/>")
            end
            println(io, "        </listOfProducts>")
        end
        # Degenerate but well-formed kinetic law that references the Kd
        # parameter, so the value is recoverable on import and visible to
        # tools without us having to commit to a specific rate law.
        println(io, "        <kineticLaw>")
        println(io, "          <math xmlns=\"http://www.w3.org/1998/Math/MathML\">")
        println(io, "            <ci> Kd_r$(i) </ci>")
        println(io, "          </math>")
        println(io, "        </kineticLaw>")
        println(io, "      </reaction>")
    end
    println(io, "    </listOfReactions>")

    if !isempty(nonpoint_notes)
        println(io, "    <notes>")
        println(io, "      <body xmlns=\"http://www.w3.org/1999/xhtml\">")
        println(io, "        <p>Non-point parameter distributions (not representable in core SBML): " *
                    _xml_escape(join(nonpoint_notes, "; ")) * "</p>")
        println(io, "      </body>")
        println(io, "    </notes>")
    end

    println(io, "  </model>")
    println(io, "</sbml>")
    return String(take!(io))
end

# ── Import: SBML string -> (NetworkIR, warnings) ──

_sbml_local_name(n) = last(split(EzXML.nodename(n), ':'))
_sbml_attr(n, key, default = "") = haskey(n, key) ? n[key] : default

function _sbml_child(node, name)
    for c in EzXML.eachelement(node)
        _sbml_local_name(c) == name && return c
    end
    return nothing
end

function _sbml_children(node, name)
    out = EzXML.Node[]
    for c in EzXML.eachelement(node)
        _sbml_local_name(c) == name && push!(out, c)
    end
    return out
end

# Pull a numeric attribute, returning nothing if absent/unparseable.
function _sbml_num(node, key)
    haskey(node, key) || return nothing
    return tryparse(Float64, node[key])
end

"""
    sbml_to_network_ir(xml::AbstractString) -> (NetworkIR, Vector{String})

Parse the binding-network subset of an SBML document into a NetworkIR and a
list of human-readable warnings for anything that could not be represented
(extra compartments, modifiers, missing/ambiguous Kd, unsupported SBML
constructs). Throws `ArgumentError` if the XML is malformed or has no model.
"""
function sbml_to_network_ir(xml::AbstractString)
    warnings = String[]
    doc = try
        EzXML.parsexml(xml)
    catch err
        throw(ArgumentError("Invalid SBML/XML: $(sprint(showerror, err))"))
    end

    sbml_root = doc.root
    _sbml_local_name(sbml_root) == "sbml" ||
        throw(ArgumentError("Root element is <$(_sbml_local_name(sbml_root))>, expected <sbml>"))

    model = _sbml_child(sbml_root, "model")
    model === nothing && throw(ArgumentError("SBML document has no <model>"))

    model_id = _sbml_attr(model, "id", "")
    model_name = _sbml_attr(model, "name", "")
    label = isempty(model_name) ? model_id : model_name

    # Compartments: we collapse everything into one well-mixed compartment.
    comp_list = _sbml_child(model, "listOfCompartments")
    if comp_list !== nothing
        ncomp = length(_sbml_children(comp_list, "compartment"))
        ncomp > 1 && push!(warnings,
            "$ncomp compartments collapsed into one well-mixed compartment (spatial structure dropped).")
    end

    # Global parameters: id -> value.
    global_params = Dict{String, Float64}()
    param_list = _sbml_child(model, "listOfParameters")
    species_decls = SpeciesDecl[]
    param_dists = ParameterDistribution[]
    if param_list !== nothing
        for p in _sbml_children(param_list, "parameter")
            id = _sbml_attr(p, "id", "")
            v = _sbml_num(p, "value")
            if !isempty(id) && v !== nothing
                global_params[id] = v
                # Surface non-Kd parameters as point distributions so users
                # keep their nominal values; Kd_* params are folded into
                # reactions below instead.
                if !occursin(r"^Kd_r\d+$", id)
                    name = _sbml_attr(p, "name", "")
                    push!(param_dists, ParameterDistribution(
                        symbol = id,
                        kind = :point,
                        value = v,
                        metadata = _sbml_identity_metadata(id, name),
                    ))
                end
            end
        end
    end

    # Species.
    sp_list = _sbml_child(model, "listOfSpecies")
    if sp_list !== nothing
        for s in _sbml_children(sp_list, "species")
            id = _sbml_attr(s, "id", "")
            isempty(id) && continue
            name = _sbml_attr(s, "name", "")
            init = _sbml_num(s, "initialConcentration")
            if init === nothing
                amt = _sbml_num(s, "initialAmount")
                amt !== nothing && (init = amt)
            end
            push!(species_decls, SpeciesDecl(
                name = id,
                initial_total = init,
                metadata = _sbml_identity_metadata(id, name),
            ))
        end
    end

    # Reactions.
    reaction_decls = ReactionDecl[]
    rx_list = _sbml_child(model, "listOfReactions")
    if rx_list !== nothing
        for (i, rx) in enumerate(_sbml_children(rx_list, "reaction"))
            rid = _sbml_attr(rx, "id", "r$i")
            rname = _sbml_attr(rx, "name", "")
            reversible = lowercase(_sbml_attr(rx, "reversible", "true")) != "false"

            reactants = _collect_species_refs(rx, "listOfReactants")
            products = _collect_species_refs(rx, "listOfProducts")

            modifiers = _sbml_child(rx, "listOfModifiers")
            modifiers !== nothing && !isempty(_sbml_children(modifiers, "modifierSpeciesReference")) &&
                push!(warnings, "Reaction $rid: modifier species ignored (not part of the binding model).")

            if isempty(reactants) || isempty(products)
                push!(warnings, "Reaction $rid skipped: needs both reactants and products to form a binding reaction.")
                continue
            end

            formula = _side_to_string(reactants) * " <-> " * _side_to_string(products)

            kd, kd_note = _extract_kd(rx, rid, global_params)
            kd_note !== nothing && push!(warnings, kd_note)

            push!(reaction_decls, ReactionDecl(
                formula = formula,
                kd = kd,
                reversible = reversible,
                metadata = _sbml_identity_metadata(rid, rname),
            ))
        end
    end

    # Unsupported top-level constructs → warnings (we ignore them).
    for (lname, human) in (("listOfRules", "rules"),
                           ("listOfEvents", "events"),
                           ("listOfFunctionDefinitions", "function definitions"),
                           ("listOfConstraints", "constraints"))
        node = _sbml_child(model, lname)
        node !== nothing && !isempty(EzXML.nodename.(collect(EzXML.eachelement(node)))) &&
            push!(warnings, "SBML $human are not supported and were ignored.")
    end

    ir = NetworkIR(
        label = label,
        species = species_decls,
        reactions = reaction_decls,
        parameter_distributions = param_dists,
        provenance = Provenance(source = "sbml_import"),
        extensions = Dict{String, Any}(
            "sbml" => Dict{String, Any}("id" => model_id, "name" => model_name),
        ),
    )
    return ir, warnings
end

function _collect_species_refs(reaction, list_name)
    out = Tuple{String, Int}[]
    list = _sbml_child(reaction, list_name)
    list === nothing && return out
    for ref in _sbml_children(list, "speciesReference")
        sp = _sbml_attr(ref, "species", "")
        isempty(sp) && continue
        stoich = _sbml_num(ref, "stoichiometry")
        coeff = stoich === nothing ? 1 : max(1, round(Int, stoich))
        push!(out, (sp, coeff))
    end
    return out
end

# Recover a reaction's Kd: prefer a global parameter referenced from the
# kineticLaw whose id looks like a dissociation constant, then a kineticLaw
# local parameter, then the conventional Kd_<rid> global parameter; else
# default to 1.0 and report it.
function _extract_kd(reaction, rid, global_params)
    klaw = _sbml_child(reaction, "kineticLaw")
    kd_like(id) = occursin(r"^(kd|k_d|keq)"i, id) || occursin(r"^Kd_"i, id)

    if klaw !== nothing
        # <ci> references inside the math, mapped to global parameters.
        math = _sbml_child(klaw, "math")
        if math !== nothing
            for ci in _all_descendants(math, "ci")
                ref = strip(EzXML.nodecontent(ci))
                if haskey(global_params, ref) && kd_like(ref)
                    return global_params[ref], nothing
                end
            end
        end
        # Local parameters.
        for ln in ("listOfLocalParameters", "listOfParameters")
            lp_list = _sbml_child(klaw, ln)
            lp_list === nothing && continue
            for lp in EzXML.eachelement(lp_list)
                id = _sbml_attr(lp, "id", "")
                v = _sbml_num(lp, "value")
                v !== nothing && kd_like(id) && return v, nothing
            end
        end
    end

    conventional = "Kd_$(rid)"
    haskey(global_params, conventional) && return global_params[conventional], nothing
    haskey(global_params, "Kd_r") && return global_params["Kd_r"], nothing

    return 1.0, "Reaction $rid: no dissociation constant found in SBML; defaulted Kd = 1.0."
end

function _all_descendants(node, name)
    out = EzXML.Node[]
    for c in EzXML.eachelement(node)
        _sbml_local_name(c) == name && push!(out, c)
        append!(out, _all_descendants(c, name))
    end
    return out
end
