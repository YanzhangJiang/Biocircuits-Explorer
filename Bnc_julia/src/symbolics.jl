#----------------------------------------------------------Symbolics calculation fucntions-----------------------------------------------------------

"""
    x_sym(args...) -> Vector{Num}

Return species symbols for a binding network.
"""
x_sym(args...)=get_binding_network(args...).x_sym
"""
    q_sym(args...) -> Vector{Num}

Return total concentration symbols for a binding network.
"""
q_sym(args...)=get_binding_network(args...).q_sym
"""
    K_sym(args...) -> Vector{Num}

Return binding constant symbols for a binding network.
"""
K_sym(args...)=get_binding_network(args...).K_sym
"""
    qK_sym(args...) -> Vector{Num}

Return concatenated `[q; K]` symbols for a binding network.
"""
qK_sym(args...)= [q_sym(args...); K_sym(args...)]

"""
    q_sym(grh::SISOPaths, args...) -> Vector{Num}

Return q symbols for a SISO path, excluding the varying coordinate.
"""
q_sym(grh::SISOPaths,args...)= begin
    bn = get_binding_network(grh)
    q_sym = if grh.change_qK_idx <= bn.d
        deleteat!(copy(bn.q_sym), grh.change_qK_idx)
    else
        bn.q_sym
    end
    return q_sym
end
"""
    K_sym(grh::SISOPaths, args...) -> Vector{Num}

Return K symbols for a SISO path, excluding the varying coordinate.
"""
K_sym(grh::SISOPaths,args...)= begin
    bn = get_binding_network(grh)
    K_sym = if grh.change_qK_idx > bn.d
        deleteat!(copy(bn.K_sym), grh.change_qK_idx - bn.d)
    else
        bn.K_sym
    end
    return K_sym
end

"""
    ∂logqK_∂logx_sym(bnc::Bnc; show_x_space=false) -> Matrix{Num}

Symbolically compute `∂log(qK)/∂log(x)`.
"""
function ∂logqK_∂logx_sym(Bnc::Bnc; show_x_space::Bool=false)::Matrix{Num}

    if show_x_space
        q = Bnc.L * Bnc.x_sym
    else
        q = Bnc.q_sym
    end

    return [
        transpose(Bnc.x_sym) .* Bnc.L ./ q
        Bnc.N
    ]
end
"""
    logder_qK_x_sym(args...; kwargs...) -> Matrix{Num}

Alias for `∂logqK_∂logx_sym`.
"""
logder_qK_x_sym(args...;kwargs...) = ∂logqK_∂logx_sym(args...;kwargs...)

"""
    ∂logx_∂logqK_sym(bnc::Bnc; show_x_space=false) -> Matrix{Num}

Symbolically compute `∂log(x)/∂log(qK)`.
"""
function ∂logx_∂logqK_sym(Bnc::Bnc;show_x_space::Bool=false)::Matrix{Num}
    # Calculate the symbolic derivative of log(qK) with respect to log(x)
    # This function is used for symbolic calculations, not numerical ones.
    return inv(∂logqK_∂logx_sym(Bnc; show_x_space=show_x_space)).|> Symbolics.simplify
end
"""
    logder_x_qK_sym(args...; kwargs...) -> Matrix{Num}

Alias for `∂logx_∂logqK_sym`.
"""
logder_x_qK_sym(args...;kwargs...) = ∂logx_∂logqK_sym(args...;kwargs...)


#---------------------------------------------------------
#   Below are regimes associtaed symbolic functions
#---------------------------------------------------------

"""
    show_condition_poly(C, C0, nullity=0; syms, log_space=true, asymptotic=false) -> Vector

Return symbolic inequality/equality conditions for polyhedral constraints.
"""
function show_condition_poly(C::AbstractMatrix{<:Real},
                        C0::AbstractVector{<:Real},
                        nullity::Integer = 0;

                        syms::AbstractVector{Num},
                        log_space::Bool = true,
                        asymptotic::Bool = false
)

    # Helper: generate symbolic expression per row
    make_expr(Crow, C0v) = if log_space
        expr = Crow * log10.(syms)
        asymptotic ? expr : expr .+ C0v
    else
        asymptotic ?
            handle_log_weighted_sum(Crow, syms) :
            handle_log_weighted_sum(Crow, syms, C0v)
    end

    # Helper: generate symbolic comparison
    make_cond(expr, op) = begin
        if log_space
            op == :eq ? (expr .~ 0) : (expr .> 0)
        else
            expr .|> x -> begin
                num, den = numerator(x), denominator(x)
                op == :eq ? (num ~ den) : (num > den)
            end
        end
    end

    # Handle two cases: nullity == 0 vs >0
    if nullity == 0
        expr = make_expr(C, C0)
        conds = make_cond(expr, :uneq)
        return conds
    else
        eq_expr   = make_expr(C[1:nullity, :], C0[1:nullity])
        uneq_expr = make_expr(C[nullity+1:end, :], C0[nullity+1:end])

        eq   = make_cond(eq_expr, :eq)
        uneq = make_cond(uneq_expr, :uneq)

        return vcat(eq, uneq) 
    end
end
"""
    show_condition_poly(poly::Polyhedron; kwargs...) -> Vector

Convenience wrapper for polyhedron constraints.
"""
show_condition_poly(poly::Polyhedron; kwargs...)=show_condition_poly(get_C_C0_nullity(poly)...; kwargs...)
"""
    show_condition_poly(C_qK::AbstractVector, C0_qK::Real, args...; kwargs...) -> Any

Return a single condition for a 1-row constraint.
"""
show_condition_poly(C_qK::AbstractVector{<:Real},C0_qK::Real,args...;kwargs...)=show_condition_poly(C_qK', [C0_qK], args...; kwargs...)[1]


"""
    show_condition_x(args...; kwargs...) -> Vector

Show symbolic conditions in x space.
"""
show_condition_x(args...; kwargs...)= show_condition_poly(get_C_C0_x(args...)...; syms=x_sym(args...), kwargs...)
"""
    show_condition_qK(args...; kwargs...) -> Vector

Show symbolic conditions in qK space.
"""
show_condition_qK(args...; kwargs...)= show_condition_poly(get_C_C0_nullity_qK(args...)...; syms=qK_sym(args...), kwargs...)
"""
    show_condition(args...; kwargs...) -> Vector

Alias for `show_condition_qK`.
"""
show_condition(args...; kwargs...)= show_condition_qK(args...; kwargs...)



"""
    show_condition_path(bnc::Bnc, path, change_qK; kwargs...) -> Vector

Show symbolic conditions for a regime path.
"""
function show_condition_path(Bnc::Bnc, path::AbstractVector{<:Integer}, change_qK; kwargs...)
    # we couldn't name it as "show_condition" as "path" will be confused with perms
    # directly calculate the polyhedron for the path, may not useful.
    poly = _calc_polyhedra_for_path(Bnc, path,change_qK)
    syms = copy(qK_sym(Bnc)) |> x->deleteat!(x,locate_sym_qK(Bnc, change_qK)) 
    show_condition_poly(poly; syms=syms, kwargs...)
end

"""
    show_condition_path(grh::SISOPaths, pth_idx; kwargs...) -> Vector

Show symbolic conditions for a SISO path.
"""
function show_condition_path(grh::SISOPaths, pth_idx; kwargs...)
    poly = get_polyhedron(grh, pth_idx)
    syms = qK_sym(grh)     
    show_condition_poly(poly; syms=syms, kwargs...)
end





"""
    show_expression_mapping(C, C0, y, x; log_space=true, asymptotic=false) -> Vector{Equation}

Return symbolic expressions for mappings of the form `log(y) = C log(x) + C0`.
"""
function show_expression_mapping(C::AbstractMatrix{<:Real}, C0::AbstractVector{<:Real}, y, x; log_space::Bool=true,asymptotic::Bool=false)::Vector{Equation}
    if log_space
        expr =  asymptotic ?   log10.(y) .~ C * log10.(x) : log10.(y) .~ C * log10.(x) .+ C0
    else
        expr =  asymptotic ? y .~ handle_log_weighted_sum(C, x) : y .~ handle_log_weighted_sum(C, x,C0)
    end
    return expr 
end
"""
    show_expression_mapping(C::AbstractVector, C0::Real, args...; kwargs...) -> Equation

Return a single mapping expression for a 1-row constraint.
"""
show_expression_mapping(C::AbstractVector{<:Real}, C0::Real, args...;kwargs...)=show_expression_mapping(C', [C0], args...;kwargs...)[1]

"""
    show_expression_x(args...; kwargs...) -> Vector{Equation}

Show symbolic expressions for x as a function of qK.
"""
show_expression_x(args...;kwargs...)= begin
    bn = get_binding_network(args...)
    y = x_sym(bn)
    x = qK_sym(bn)
    show_expression_mapping(get_H_H0(args...)..., y,x; kwargs...)
end

"""
    show_expression_qK(args...; kwargs...) -> Vector{Equation}

Show symbolic expressions for qK as a function of x.
"""
show_expression_qK(args...;kwargs...)= begin
    bn = get_binding_network(args...)
    y = qK_sym(bn)
    x = x_sym(bn)
    show_expression_mapping(get_M_M0(args...)..., y,x; kwargs...)
end


"""
    show_dominant_condition(args...; log_space=false, kwargs...) -> Vector{Equation}

Show dominant conservation conditions for a vertex.
"""
show_dominant_condition(args...;log_space=false, kwargs...)= begin
    bn = get_binding_network(args...)
    y = q_sym(bn)
    x = x_sym(bn)
    show_expression_mapping(get_P_P0(args...)..., y,x; log_space=log_space,kwargs...)
end
"""
    show_conservation(bnc::Bnc) -> Vector{Equation}

Return conservation equations `q = Lx`.
"""
show_conservation(Bnc::Bnc)=Bnc.q_sym .~ Bnc._L_sparse * Bnc.x_sym
"""
    show_equilibrium(bnc::Bnc; log_space=true) -> Vector{Equation}

Return equilibrium equations relating `K` and `x`.
"""
show_equilibrium(Bnc::Bnc;log_space::Bool=true) = show_expression_mapping(Bnc.N, zeros(Int,Bnc.r), Bnc.K_sym, Bnc.x_sym; log_space=log_space)










"""
    handle_log_weighted_sum(A, x, b=nothing) -> Vector{Num}

Convert `A*log10(x) + b` into a multiplicative form `x^A * 10^b`.
"""
function handle_log_weighted_sum(A::AbstractMatrix{<:Real}, x , b::Union{Nothing,AbstractVector{<:Real}}=nothing)::Vector{Num}
    rows = size(A,1)
    rst = Vector{Num}(undef, rows)
    b = isnothing(b) ? zeros(Int, rows) : b
    for i in 1:rows
        rst[i] = x .^ A[i,:] |> prod |> (x-> x*10^b[i])
    end
    return rst
end


"""
    sym_direction(bnc::Bnc, dir) -> String

Create a symbolic representation of a qK direction vector.
"""
function sym_direction(Bnc::Bnc,dir)::String
    rst = ""
    for i in 1:Bnc.d
        if dir[i] > 1e-6
            rst *= "+"*repr(Bnc.q_sym[i])*" "
        elseif dir[i] < -1e-6
            rst *= "-"*repr(Bnc.q_sym[i])*" "
        end
    end
    rst*="; "
    for j in 1:Bnc.r
        if dir[j+Bnc.d] > 1e-6
            rst *= "+"*repr(Bnc.K_sym[j])*" "
        elseif dir[j+Bnc.d] < -1e-6
            rst *= "-"*repr(Bnc.K_sym[j])*" "
        end
    end
    return rst
end


"""
    sym_direction(dir; syms) -> String

Create a symbolic representation of a direction vector using provided symbols.
"""
function sym_direction(dir ;syms::AbstractArray{Num})::String
    rst = ""
    for i in eachindex(dir)
        if dir[i] > 1e-6
            rst *= "+"*repr(Bnc.q_sym[i])*" "
        elseif dir[i] < -1e-6
            rst *= "-"*repr(Bnc.q_sym[i])*" "
        end
    end
    return rst
end



"""
    _fmt_elem(x; digits=3) -> String

Format a single path element for display.
"""
@inline function _fmt_elem(x; digits::Int=3)::String
    if x isa Real
        if isnan(x)
            return "NaN"
        end
        xr = round(Float64(x); digits=digits)
        # “接近整数就显示整数”，避免 try/catch
        if isfinite(xr) && isapprox(xr, round(xr); atol=10.0^(-digits), rtol=0)
            return string(Int(round(xr)))
        else
            return string(xr)
        end
    else
        return repr(x)
    end
end

"""
    format_arrow(path; prefix="", digits=3) -> String

Format a path vector as an arrow-separated string.
"""
function format_arrow(path::AbstractVector; prefix::AbstractString="", digits::Int=3)::String
    isempty(path) && return ""
    parts = Vector{String}(undef, length(path))
    @inbounds for i in eachindex(path)
        parts[i] = prefix * _fmt_elem(path[i]; digits=digits)
    end
    return join(parts, " → ")
end



"""
A normalized row for printing:
- id:     group/path id (any displayable object)
- path:   the actual path vector Could be either regime path or Reaction order path
- volume: nothing | Float64 | Tuple(value, err)
"""
struct PathRow{I,P,V}
    id::I
    path::P
    volume::V
end

"""
    _normalize_rows(paths; ids=nothing, volumes=nothing) -> Vector{PathRow}

Normalize paths, ids, and volumes into `PathRow` entries.
"""
function _normalize_rows(paths::AbstractVector{<:AbstractVector}; ids=nothing, volumes=nothing)
    n = length(paths)
    ids === nothing && (ids = collect(1:n))
    volumes === nothing && (volumes = fill(nothing, n))

    @assert length(ids) == n "ids length must match paths length"
    @assert length(volumes) == n "volumes length must match paths length"

    rows = Vector{PathRow}(undef, n)
    @inbounds for i in 1:n
        rows[i] = PathRow(ids[i], paths[i], volumes[i])
    end
    return rows
end



"""
    print_paths(rows; prefix="", digits=3, io=stdout) -> nothing

Print rows of paths in aligned columns.
"""
function print_paths(rows::AbstractVector{<:PathRow};
    prefix::AbstractString="",
    digits::Int=3,
    io::IO=stdout,
)
    isempty(rows) && return nothing

    # 动态列宽：比写死 15/30 更不容易“错位”
    id_strs    = [repr(r.id) for r in rows]
    path_strs  = [format_arrow(r.path; prefix=prefix, digits=digits) for r in rows]

    id_width   = max(8, maximum(length.(id_strs)))     # 至少 8
    path_width = max(10, maximum(length.(path_strs)))  # 至少 10

    for (r, id_s, path_s) in zip(rows, id_strs, path_strs)
        if r.volume === nothing
            Printf.@printf(io, "Path %-*s  %-*s\n", id_width, id_s, path_width, path_s)
        else#if  r.volume isa Volume
            @assert typeof(r.volume) <: Volume 
            v = r.volume.mean
            e = sqrt(r.volume.var)
            Printf.@printf(io, "Path %-*s  %-*s  Volume: %.4f ± %.4f\n", id_width, id_s, path_width, path_s, v, e)
        end
    end
    return nothing
end
# 6.1 直接给 paths



"""
    print_paths(paths; volumes=nothing, ids=nothing, kwargs...) -> nothing

Convenience wrapper that builds `PathRow` entries.
"""
print_paths(paths::AbstractVector{<:AbstractVector}; volumes=nothing, ids=nothing, kwargs...) =
    print_paths(_normalize_rows(paths; volumes=volumes, ids=ids); kwargs...)


"""
    print_path(path; id=nothing, volume=nothing, kwargs...) -> nothing

Print a single path entry.
"""
print_path(path::AbstractVector; id = nothing, volume = nothing, kwargs...) =
    print_paths(
        _normalize_rows(
            [path]; 
            ids = id === nothing ? nothing : [id], 
            volumes = volume === nothing ? nothing : [volume]
        ); kwargs...)




"""
    solve_sym_expr(a, b, x, idx; log_space=true) -> Equation

Solve `a'x + b = 0` for the variable at `idx` symbolically.
"""
function solve_sym_expr(a::AbstractVector{<:Real}, b::Real, x, idx; log_space::Bool=true)
    a = copy(collect(a))
    x = copy(x)
    ai = popat!(a, idx)
    target_x = popat!(x, idx)
    @assert abs(ai) > 1e-10 "Cannot solve for the variable at index $idx since its coefficient is zero." 
    a ./= -ai
    b /= -ai

    target = log_space ? log10(target_x) : target_x
    expr = log_space ? a' * log10.(x) .+ b : handle_log_weighted_sum(a', x, [b])[1]
    return target ~ expr
end

"""
    show_interface(bnc::Bnc, from, to; lhs_idx=nothing, kwargs...) -> Any

Display the interface expression between two regimes.
"""
function show_interface(Bnc::Bnc, from,to;  lhs_idx::Union{Nothing,Integer}=nothing, kwargs...)
    C, C0 = get_interface(Bnc,from,to) # C' log qK + C0 =0
    if isnothing(lhs_idx)
        return show_condition_poly(C, C0, 1 ;syms =  qK_sym(Bnc) ,kwargs...)
    else
        return solve_sym_expr(C,C0, qK_sym(Bnc), lhs_idx;kwargs...)
    end
end





"""
    show_expression_path(grh::SISOPaths, pth; observe_x=nothing, kwargs...) -> nothing

Display piecewise symbolic expressions along a SISO path.
"""
function show_expression_path(grh::SISOPaths, pth; observe_x=nothing, kwargs...)
    pth_idx = get_idx(grh, pth)
    bn = get_binding_network(grh)

    observe_x_idx = isnothing(observe_x) ? (1:bn.n) : locate_sym_x.(Ref(bn), observe_x)
    change_qK_idx = grh.change_qK_idx

    xsym = x_sym(bn)[observe_x_idx]
    qKsym = qK_sym(bn)
    change_sym = qKsym[change_qK_idx]

    continuous, upward, downward = :→, :↑, :↓
    vars = @variables $continuous, $upward, $downward

    H_H0, rgm_interface = get_expression_path(grh, pth_idx; observe_x = observe_x_idx)

    expr_sym = let 
        exprs = Vector{Any}(undef, length(H_H0))
        for (i, (H_row, H0_val)) in enumerate(H_H0)

            exprs[i] = if isnothing(H0_val)
                        #1. singular regime, expression is just H_row * log(qK)
                            let
                                a = Vector{Num}(undef, size(H_row,1)) # number of x observed
                                for j in eachindex(a)
                                    a[j] = if abs(H_row[j]) < 1e-6
                                                vars[1]
                                        else  
                                            H_row[j] > 0 ? vars[2] : vars[3]
                                        end
                                end
                                a
                            end
                    else
                        # 2. regular regime, expression is H_row * log(qK) + H0_val
                            show_expression_mapping(H_row, H0_val, xsym, qKsym  ; kwargs...)
                    end
        end
        exprs
    end


    interface = rgm_interface .|> x -> solve_sym_expr(x..., qKsym, change_qK_idx; kwargs...)
    
    for i in eachindex(expr_sym)
        if i == 1
            display(change_sym < interface[1].rhs)
        elseif i == length(expr_sym)
            display(change_sym > interface[end].rhs)
        else
            display((change_sym > interface[i-1].rhs) & (change_sym < interface[i].rhs))
        end

        display(expr_sym[i])
    end


    return nothing
end



















"""
    show_expression_path(model::Bnc, rgm_path, change_qK_idx, observe_x_idx; log_space=false) -> (Vector, Vector)

Return symbolic expressions and interface locations along a regime path.
"""
function show_expression_path(model::Bnc, rgm_path, change_qK_idx, observe_x_idx;log_space::Bool=false)::Tuple{Vector,Vector}
    change_qK_idx = locate_sym([model.q_sym;model.K_sym],change_qK_idx)
    observe_x_idx = locate_sym(model.x_sym, observe_x_idx)
    have_volume_mask = _get_vertices_mask(model, rgm_path; singular=false)
    idx = findall(have_volume_mask)
    exprs = map(idx) do id
        show_expression_x(model, rgm_path[id];log_space=log_space)[observe_x_idx].rhs
    end
    edges = map(@view idx[1:end-1]) do i
        rgm_from = rgm_path[i]
        rgm_to   = rgm_path[i+1]
        edge = show_interface(model, rgm_from, rgm_to; lhs_idx=change_qK_idx, log_space=log_space).rhs
        return edge
    end
    return (exprs, edges)
end

"""
    show_expression_path(grh::SISOPaths, pth_idx, observe_x; kwargs...) -> (Vector, Vector)

Convenience wrapper for `show_expression_path` with a SISO path index.
"""
show_expression_path(grh::SISOPaths, pth_idx, observe_x; kwargs...)=show_expression_path(get_binding_network(grh), grh.rgm_paths[pth_idx], grh.change_qK_idx, observe_x; kwargs...)
