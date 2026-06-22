export find_catalysis_regimes!, get_catalysis_network, get_catalysis_regime, get_catalysis_regimes, get_catalysis_regimes_dict
export get_PΠ, get_CΠ, get_P_pos_neg, get_P0_pos_neg
export get_C_k, get_C_C0_xk, get_C0_xk, get_C_xk, get_C_C0_nullity_xk

@inline function _require_catalysis_network(args...)
    model = get_catalysis_network(args...)
    isnothing(model) && error("Catalysis network not found in the model.")
    return model
end







get_binding_network(rgm::CatalysisRegime) = get_binding_network(rgm.network)
get_binding_network(rgm::BncRegime) = get_binding_network(rgm.bind_rgm)
function get_binding_network(model::CatalysisData)
    if isnothing(model.bn)
        @warn "Binding Network not found in the model"
        return nothing
    end
    return model.bn
end




get_catalysis_network(model::CatalysisData) = model
get_catalysis_network(rgm::BindRegime) = get_catalysis_network(rgm.network)
get_catalysis_network(rgm::CatalysisRegime) = rgm.network
get_catalysis_network(rgm::BncRegime) = get_catalysis_network(rgm.catalysis_rgm)
function get_catalysis_network(model::Bnc)
    if isnothing(model.catalysis)
        @warn "Catalysis Network not found in the model"
        return nothing
    end
    return model.catalysis
end



@inline _catalysis_regimes(model::CatalysisData) = getfield(model, :CatalysisRegimes)
@inline _catalysis_regimes_built(model::CatalysisData) = !isnothing(_catalysis_regimes(model))



@inline function _catalysis_regimes_data(model::CatalysisData)
    regimes = _catalysis_regimes(model)
    return isnothing(regimes) ? CatalysisRegime[] : regimes.vertices_data
end




@inline function _catalysis_regimes_perm_dict(model::CatalysisData)
    regimes = _catalysis_regimes(model)
    return isnothing(regimes) ? Dict{Vector{Int},Int}() : regimes.vertices_perm_dict
end



@inline _catalysis_regimes_perm(model::CatalysisData) = getfield.(_catalysis_regimes_data(model), :perm)
@inline _catalysis_regimes_asymptotic_flag(model::CatalysisData) = getfield.(_catalysis_regimes_data(model), :is_asymptotic)
@inline _catalysis_regimes_initialized(model::CatalysisData) = BitVector(.!isnothing.(getfield.(_catalysis_regimes_data(model), :P_pos_neg)))
function Base.getproperty(model::CatalysisData, sym::Symbol)
    if sym === :vertices_perm
        return _catalysis_regimes_perm(model)
    elseif sym === :vertices_perm_dict
        return _catalysis_regimes_perm_dict(model)
    elseif sym === :vertices_asymptotic_flag
        return _catalysis_regimes_asymptotic_flag(model)
    elseif sym === :vertices_data
        return _catalysis_regimes_data(model)
    elseif sym === :_vertices_is_initialized
        return _catalysis_regimes_initialized(model)
    end
    return getfield(model, sym)
end
function Base.propertynames(model::CatalysisData, private::Bool=false)
    names = Symbol[fieldnames(typeof(model))...,
        :vertices_perm,
        :vertices_perm_dict,
        :vertices_asymptotic_flag,
        :vertices_data,
        :_vertices_is_initialized,
    ]
    return private ? Tuple(unique(names)) : Tuple(sym for sym in unique(names) if !startswith(String(sym), "_"))
end







find_all_regimes!(model::CatalysisData) = find_catalysis_regimes!(model)
find_catalysis_regimes!(model::Bnc) = find_catalysis_regimes!(_require_catalysis_network(model))

function find_catalysis_regimes!(model::CatalysisData)
    if _catalysis_regimes_built(model)
        return nothing
    end

    @info "---------------------Start finding all vertices--------------------"
    all_vertices, is_asymptotic = _enumerate_all_regimes(model._S_helper)

    n_vertices = length(all_vertices)
    n_asym_rgms = sum(is_asymptotic)
    @info "Finished, with $(n_vertices) catalysis vertices found and $(n_asym_rgms) asymptotic vertices."

    @info "3.Building Regimes..."
    model.CatalysisRegimes = let
        regimes = _build_catalysis_regimes(model, all_vertices, is_asymptotic)
        vertices_perm_dict = Dict(perm => idx for (idx, perm) in enumerate(all_vertices))
        Regimes(vertices_perm_dict, regimes)
    end
    return nothing
end

@inline function _build_catalysis_regimes(model::CatalysisData, all_vertices, is_asymptotic)
    n_vertices = length(all_vertices)
    regimes = Vector{CatalysisRegime}(undef, n_vertices)
    for i in 1:n_vertices
        regimes[i] = CatalysisRegime(
            network = model,
            perm = all_vertices[i],
            idx = i,
            is_asymptotic = is_asymptotic[i],
        )
    end
    return regimes
end


function _initialize_regime!(vtx::CatalysisRegime)
    if !isnothing(vtx.P_pos_neg)
        return vtx
    end

    model = _require_catalysis_network(vtx)
    perm = vtx.perm

    P_pos_neg, P0_pos_neg = _calc_P_P0(perm, model._S_helper)
    C, C0 = _calc_C_C0(perm, model._S_helper)
    P = P_pos_neg[1:model.r_v, :] - P_pos_neg[model.r_v+1:end, :]
    P0 = P0_pos_neg[1:model.r_v] - P0_pos_neg[model.r_v+1:end]

    vtx.P_pos_neg = P_pos_neg
    vtx.P0_pos_neg = P0_pos_neg
    vtx.P = P
    vtx.P0 = P0
    vtx.C = C
    vtx.C0 = C0
    vtx.CΠ = C * model.Π
    vtx.PΠ = P * model.Π
    return vtx
end


get_regimes_dict(model::CatalysisData) = begin
    find_catalysis_regimes!(model)
    get_regimes_dict(model.CatalysisRegimes)
end
get_catalysis_regimes_dict(model::AbstractBnc) = get_regimes_dict(_require_catalysis_network(model))


function get_idx(model::CatalysisData, idx::T; check::Bool=false) where T<:Integer
    if check
        find_catalysis_regimes!(model)
        @assert idx >= 1 && idx <= n_regimes(model) "The given catalysis index is out of range."
    end
    return idx
end
function get_idx(model::CatalysisData, perm::AbstractVector{<:Integer}; check::Bool=false)
    dict = get_catalysis_regimes_dict(model)
    if check
        @assert haskey(dict, perm) "The given catalysis perm is not in the model."
    end
    return dict[perm]
end
get_idx(model::AbstractBnc, rgm::CatalysisRegime; kwargs...) = get_idx(_require_catalysis_network(model), rgm; kwargs...)
get_idx(model::CatalysisData, rgm::CatalysisRegime; kwargs...) = get_idx(rgm)
get_idx(rgm::CatalysisRegime) = rgm.idx


function get_perm(model::CatalysisData, perm::AbstractVector{<:Integer}; check::Bool=false)
    if check
        @assert haskey(get_catalysis_regimes_dict(model), perm) "The given catalysis perm is not in the model."
    end
    return Vector{Int}(perm)
end
get_perm(model::CatalysisData, idx::Integer; kwargs...) = (find_catalysis_regimes!(model); model.vertices_data[idx].perm)
get_perm(rgm::CatalysisRegime) = rgm.perm
get_perm(model::AbstractBnc, rgm::CatalysisRegime; kwargs...) = get_perm(rgm)


function get_catalysis_regime(model::AbstractBnc, perm_or_idx; check::Bool=false, kwargs...)::CatalysisRegime
    return get_catalysis_regime(_require_catalysis_network(model), perm_or_idx; check=check, kwargs...)
end
function get_catalysis_regime(model::CatalysisData, perm_or_idx; check::Bool=false, kwargs...)::CatalysisRegime
    find_catalysis_regimes!(model)
    idx = get_idx(model, perm_or_idx; check=check)
    return get_catalysis_regime(model.vertices_data[idx]; kwargs...)
end
function get_catalysis_regime(rgm::CatalysisRegime; kwargs...)::CatalysisRegime
    return _initialize_regime!(rgm)
end

get_regime(model::CatalysisData, perm_or_idx; kwargs...) = get_catalysis_regime(model, perm_or_idx; kwargs...)
get_regime(rgm::CatalysisRegime; kwargs...) = get_catalysis_regime(rgm; kwargs...)


function get_regimes(model::CatalysisData; return_idx::Bool=false, asymptotic::Union{Bool,Nothing}=nothing)
    find_catalysis_regimes!(model)
    idxs = collect(eachindex(model.vertices_data))
    if !isnothing(asymptotic)
        filter!(i -> model.vertices_data[i].is_asymptotic == asymptotic, idxs)
    end
    return return_idx ? idxs : getfield.(model.vertices_data[idxs], :perm)
end

get_indices(model::CatalysisData; kwargs...) = get_regimes(model; return_idx=true, kwargs...)
get_perms(model::CatalysisData; kwargs...) = get_regimes(model; return_idx=false, kwargs...)
get_indices(rgms::AbstractVector{<:CatalysisRegime}) = getfield.(rgms, :idx)
get_perms(rgms::AbstractVector{<:CatalysisRegime}) = getfield.(rgms, :perm)

n_regimes(model::CatalysisData) = (find_catalysis_regimes!(model); length(model.vertices_data))

have_perm(model::CatalysisData, perm::AbstractVector) = haskey(get_catalysis_regimes_dict(model), Vector{Int}(perm))
have_perm(model::CatalysisData, idx::Integer) = (find_catalysis_regimes!(model); idx >= 1 && idx <= n_regimes(model))
have_perm(model::CatalysisData, rgm::CatalysisRegime) = have_perm(model, get_perm(rgm))

is_asymptotic(model::CatalysisData, perm_or_idx) = begin
    find_catalysis_regimes!(model)
    model.vertices_data[get_idx(model, perm_or_idx)].is_asymptotic
end::Bool
is_asymptotic(rgm::CatalysisRegime) = rgm.is_asymptotic

get_nullity(::CatalysisRegime) = 0
is_singular(::CatalysisRegime) = false


get_catalysis_regimes(model::AbstractBnc; kwargs...) = get_regimes(_require_catalysis_network(model); kwargs...)
get_catalysis_regimes(model::CatalysisData; kwargs...) = get_regimes(model; kwargs...)


function get_P_pos_neg(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.P_pos_neg
end
function get_P0_pos_neg(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.P0_pos_neg
end
get_P0_pos_neg(model::CatalysisData, perm_or_idx; kwargs...) = get_P0_pos_neg(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_P(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.P
end
get_P(model::CatalysisData, perm_or_idx; kwargs...) = get_P(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_P0(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.P0
end
get_P0(model::CatalysisData, perm_or_idx; kwargs...) = get_P0(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_PΠ(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.PΠ
end
get_PΠ(model::CatalysisData, perm_or_idx; kwargs...) = get_PΠ(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.C
end
get_C(model::CatalysisData, perm_or_idx; kwargs...) = get_C(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C0(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.C0
end
get_C0(model::CatalysisData, perm_or_idx; kwargs...) = get_C0(get_catalysis_regime(model, perm_or_idx; kwargs...))

get_C_k(rgm::CatalysisRegime) = get_C(rgm)
get_C_k(model::CatalysisData, perm_or_idx; kwargs...) = get_C_k(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_CΠ(rgm::CatalysisRegime)
    _initialize_regime!(rgm)
    return rgm.CΠ
end
get_CΠ(model::CatalysisData, perm_or_idx; kwargs...) = get_CΠ(get_catalysis_regime(model, perm_or_idx; kwargs...))

get_C_x(rgm::CatalysisRegime) = get_CΠ(rgm)
get_C_x(model::CatalysisData, perm_or_idx; kwargs...) = get_C_x(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C_xk(rgm::CatalysisRegime)
    return hcat(get_CΠ(rgm), get_C_k(rgm))
end
get_C_xk(model::CatalysisData, perm_or_idx; kwargs...) = get_C_xk(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_P_xk(rgm::CatalysisRegime)
    return hcat(get_PΠ(rgm), get_P(rgm))
end
get_P_xk(model::CatalysisData, perm_or_idx; kwargs...) = get_P_xk(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_P_P0(rgm::CatalysisRegime)
    return get_P(rgm), get_P0(rgm)
end
get_P_P0(model::CatalysisData, perm_or_idx; kwargs...) = get_P_P0(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C_C0(rgm::CatalysisRegime)
    return get_C(rgm), get_C0(rgm)
end
get_C_C0(model::CatalysisData, perm_or_idx; kwargs...) = get_C_C0(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C_C0_xk(rgm::CatalysisRegime)
    C = vcat(get_P_xk(rgm), get_C_xk(rgm))
    C0 = vcat(get_P0(rgm), get_C0(rgm))
    return C, C0
end
get_C_C0_xk(model::CatalysisData, perm_or_idx; kwargs...) = get_C_C0_xk(get_catalysis_regime(model, perm_or_idx; kwargs...))
get_C0_xk(rgm::CatalysisRegime) = get_C_C0_xk(rgm)[2]
get_C0_xk(model::CatalysisData, perm_or_idx; kwargs...) = get_C0_xk(get_catalysis_regime(model, perm_or_idx; kwargs...))

function get_C_C0_nullity_xk(rgm::CatalysisRegime)
    C, C0 = get_C_C0_xk(rgm)
    return C, C0, size(get_P(rgm), 1)
end
get_C_C0_nullity_xk(model::CatalysisData, perm_or_idx; kwargs...) = get_C_C0_nullity_xk(get_catalysis_regime(model, perm_or_idx; kwargs...))

function summary_regime(rgm::CatalysisRegime)
    rgm = get_catalysis_regime(rgm)
    println("idx=$(rgm.idx), perm=$(rgm.perm), asymptotic=$(rgm.is_asymptotic)")
    println("Steady-state equalities and dominance inequalities in (x, k):")
    display.(show_condition_xk(rgm; log_space=false))
    return nothing
end

summary(rgm::CatalysisRegime) = summary_regime(rgm)

@inline function _regime_display_dominant_mode(rgm::CatalysisRegime)
    return "perm=$(get_perm(rgm))"
end

function Base.show(io::IO, rgm::CatalysisRegime)
    print(
        io,
        "CatalysisRegime(",
        _regime_display_dominant_mode(rgm),
        ", nullity=",
        get_nullity(rgm),
        ", asymptotic=",
        is_asymptotic(rgm),
        ")",
    )
end

function Base.show(io::IO, ::MIME"text/plain", rgm::CatalysisRegime)
    println(io, "CatalysisRegime")
    println(io, "  dominant mode: ", _regime_display_dominant_mode(rgm))
    println(io, "  nullity: ", get_nullity(rgm))
    print(io, "  asymptotic: ", is_asymptotic(rgm))
end
