export judge_dstable

# using SparseArrays
# using LinearAlgebra
import JuMP
import MathOptInterface as MOI
import Arpack
import Clarabel

# 可选：
# import MosekTools
# import MathOptChordalDecomposition as MOCD
# import SCS

@inline tri_index(i::Int, j::Int) = (j * (j - 1)) ÷ 2 + i  # 要求 i <= j

function _default_optimizer_factory(time_limit::Real)
    return JuMP.optimizer_with_attributes(
        Clarabel.Optimizer,
        "verbose" => false,
        "time_limit" => Float64(time_limit),
    )
end

function _build_model(optimizer_factory)
    model = JuMP.Model(optimizer_factory)
    try
        set_silent(model)
    catch
        # 某些优化器不支持 MOI.Silent()，忽略即可
    end
    return model
end

"""
    _obvious_not_hurwitz(A; ...)

单边测试：
- 返回 `true`     : 已证实 A 不是 Hurwitz
- 返回 `:unknown` : 无法确定
"""
function _obvious_not_hurwitz(
    A::SparseMatrixCSC{Float64, Int};
    dense_exact_threshold::Int = 256,
    spectral_tol::Float64 = 1e-8,
    eigs_tol::Float64 = 1e-8,
    eigs_maxiter::Int = 2000,
)
    n = size(A, 1)

    # 小规模：直接精确（数值上）算全部特征值
    if n <= dense_exact_threshold
        α = maximum(real.(eigvals(Matrix(A))))
        return α >= -spectral_tol
    end

    # 大规模：ARPACK 做单边筛查
    # 若 Ritz 对满足 Re(λ̂) - residual > 0，则可保守判定存在 RHP 特征值
    try
        λ, V, nconv, _, _, _ = Arpack.eigs(
            A;
            nev = 1,
            which = :LR,      # largest real part
            tol = eigs_tol,
            maxiter = eigs_maxiter,
            ritzvec = true,
        )
        if nconv >= 1
            λ1 = λ[1]
            v1 = V[:, 1]
            r  = norm(A * v1 - λ1 * v1) / max(norm(v1), eps(Float64))
            if real(λ1) - r > spectral_tol
                return true
            end
        end
    catch
        # 留给后续 SDP
    end

    return :unknown
end

"""
构造 -(A'P + P*A) - tI 的上三角向量化，
其中 P = diag(signs .* x).
如果 signs === nothing，则表示全正号。
"""
function _neg_lyap_triangle(
    A::SparseMatrixCSC{Float64, Int},
    x,
    t;
    signs::Union{Nothing, AbstractVector{<:Real}} = nothing,
)
    n = size(A, 1)
    tri = [JuMP.AffExpr(0.0) for _ in 1:(n * (n + 1) ÷ 2)]

    # 对角：-t I
    for i in 1:n
        JuMP.add_to_expression!(tri[tri_index(i, i)], -1.0, t)
    end

    rows = rowvals(A)
    vals = nonzeros(A)

    # 对每个非零 a_{row,col}：
    # 上三角位置 (min(row,col), max(row,col))
    # 接收 a_{row,col} * p_row 的贡献；对角项会自动翻倍
    for col in 1:n
        for ptr in nzrange(A, col)
            row = rows[ptr]
            a   = vals[ptr]

            i = min(row, col)
            j = max(row, col)
            idx = tri_index(i, j)

            s = isnothing(signs) ? 1.0 : Float64(signs[row])
            coeff = -(row == col ? 2.0 : 1.0) * a * s
            JuMP.add_to_expression!(tri[idx], coeff, x[row])
        end
    end

    return tri
end

"""
求解
    max t
    s.t. -(A'P + P*A) - tI ⪰ 0,
         P = diag(signs .* x),
         x_i >= p_floor,
         sum(x) = 1.
返回最优 t；若求解失败则返回 -Inf。
"""
function _signed_diag_lyap_margin(
    A::SparseMatrixCSC{Float64, Int};
    optimizer_factory,
    p_floor::Float64 = 1e-8,
    signs::Union{Nothing, AbstractVector{<:Real}} = nothing,
)
    n = size(A, 1)
    n * p_floor < 1 || throw(ArgumentError("需要满足 n * p_floor < 1"))

    model = _build_model(optimizer_factory)

    JuMP.@variable(model, x[1:n] >= p_floor)
    JuMP.@variable(model, t >= 0.0)
    JuMP.@constraint(model, sum(x) == 1.0)

    tri = _neg_lyap_triangle(A, x, t; signs = signs)
    JuMP.@constraint(model, tri in MOI.PositiveSemidefiniteConeTriangle(n))
    JuMP.@objective(model, Max, t)

    JuMP.optimize!(model)

    st = JuMP.termination_status(model)
    if st == MOI.OPTIMAL || st == MOI.ALMOST_OPTIMAL
        return JuMP.value(t)
    end
    return -Inf
end

"""
判据 B 的可选备份：
只枚举“恰好一个负号”的模式。
找到即可给出强 0 证书。
"""
function _strong_zero_certificate_singletons(
    A::SparseMatrixCSC{Float64, Int};
    optimizer_factory,
    p_floor::Float64 = 1e-8,
    margin_tol::Float64 = 1e-7,
    max_patterns::Int = 16,
)
    n = size(A, 1)
    n == 0 && return false
    max_patterns <= 0 && return false

    # 启发式：先试对角元较大的位置
    ord = sortperm(diag(A); rev = true)
    kmax = min(n, max_patterns)

    for kk in 1:kmax
        k = ord[kk]
        signs = ones(Float64, n)
        signs[k] = -1.0

        t = _signed_diag_lyap_margin(
            A;
            optimizer_factory = optimizer_factory,
            p_floor = p_floor,
            signs = signs,
        )
        if isfinite(t) && t > margin_tol
            return true
        end
    end

    return false
end

"""
    judge_dstable(A; kwargs...) -> Int

输出：
- 1  : 证实 D-stable（通过 diagonal stability）
- 0  : 证实一定不 D-stable
- -1 : 无法判断

默认是“性能优先”：
1) 先做单边的非 Hurwitz 筛查；
2) 再做 diagonal stability SDP；
3) 默认不跑判据 B 的符号枚举；
   若要启用，把 try_strong_zero=true。
"""
function judge_dstable(
    Ain::AbstractMatrix{<:Real};
    optimizer_factory = nothing,
    time_limit::Float64 = 20.0,
    dense_exact_threshold::Int = 256,
    spectral_tol::Float64 = 1e-8,
    eigs_tol::Float64 = 1e-8,
    eigs_maxiter::Int = 2000,
    p_floor::Float64 = 1e-8,
    margin_tol::Float64 = 1e-7,
    try_strong_zero::Bool = false,
    strong_zero_patterns::Int = 16,
)::Int
    n, m = size(Ain)
    n == m || throw(ArgumentError("A 必须是方阵"))

    if n == 0
        return 1
    end

    A = sparse(Float64.(Ain))
    all(isfinite, nonzeros(A)) || throw(ArgumentError("A 含 NaN/Inf"))

    # 归一化，改善数值条件；不改变稳定性符号
    scale = max(opnorm(A, Inf), 1.0)
    A = A / scale

    if optimizer_factory === nothing
        optimizer_factory = _default_optimizer_factory(time_limit)
    end

    # Step 0: 便宜的单边 0 证书
    rhp = _obvious_not_hurwitz(
        A;
        dense_exact_threshold = dense_exact_threshold,
        spectral_tol = spectral_tol,
        eigs_tol = eigs_tol,
        eigs_maxiter = eigs_maxiter,
    )
    if rhp === true
        return 0
    end

    # Step 1: diagonal stability => D-stable
    tpos = _signed_diag_lyap_margin(
        A;
        optimizer_factory = optimizer_factory,
        p_floor = p_floor,
        signs = nothing,
    )
    if isfinite(tpos) && tpos > margin_tol
        return 1
    end

    # Step 2: 可选的强 0 证书备份（只在谱筛查未明确时有意义）
    if try_strong_zero && rhp === :unknown
        if _strong_zero_certificate_singletons(
            A;
            optimizer_factory = optimizer_factory,
            p_floor = p_floor,
            margin_tol = margin_tol,
            max_patterns = strong_zero_patterns,
        )
            return 0
        end
    end

    return -1
end
