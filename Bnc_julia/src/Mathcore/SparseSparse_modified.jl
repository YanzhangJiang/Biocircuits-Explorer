# Modified from  https://github.com/sloisel/SparseSparse.jl/blob/main/src/SparseSparse.jl


struct luFac{Tv,Ti<:Integer} 
    L::Union{Missing,SparseMatrixCSC{Tv,Ti}}
    U::Union{Missing,SparseMatrixCSC{Tv,Ti}}
    p::Union{Missing,Vector{Ti}}
    q::Union{Missing,Vector{Ti}}
end

function luFac(F::SparseArrays.UMFPACK.UmfpackLU{Tv,Ti}) where {Tv,Ti<:Integer}
    @assert Tv==Float64 || Tv==ComplexF64
    @assert issuccess(F)
    R = spdiagm(0=>1 ./F.Rs[F.p])
    p = (F.p==1:length(F.p)) ? missing : F.p
    q = (F.q==1:length(F.q)) ? missing : invperm(F.q)
    return luFac((R*F.L),(F.U),p,q)
end

function luFac(A::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti<:Integer}
    return luFac(lu(A; check=false))
end


function transitiveclosure(L::SparseMatrixCSC{Tv,Ti},Jlen,CJ,mark;countonly=false) where {Tv,Ti<:Integer}
    b = 0
    c = Jlen
    for i=1:Jlen
        mark[CJ[i]] = true
    end
    cp = L.colptr
    rv = L.rowval
    while b<c
        b+=1
        i = CJ[b]
        p = cp[i]
        q = cp[i+1]-1
        for i=p:q
            j = rv[i]
            if !mark[j]
                c+=1
                mark[j]=true
                CJ[c]=j
            end
        end
    end
    for i=1:c
        mark[CJ[i]] = false
    end
    if countonly
        return c
    end
    sort!(view(CJ,1:c))
    return c
end

function solvevec(L::SparseMatrixCSC{Tv,Ti},lowertriangular,x::Vector{Tv},J,CJ,mark) where {Tv,Ti<:Integer}
    @assert size(L,2)==length(x)
    m = length(J)
    for i=1:m
        CJ[i] = J[i]
    end
    c = transitiveclosure(L,m,CJ,mark)
    cp = L.colptr
    rv = L.rowval
    nz = L.nzval
    if lowertriangular
        (a,b,dir) = (1,c,1)
    else
        (a,b,dir) = (c,1,-1)
    end
    for i=a:dir:b
        j = CJ[i]
        p = cp[j]
        q = cp[j+1]-1
        if lowertriangular
            @assert rv[p]==j
            x[j] /= nz[p]
            p+=1
        else
            @assert rv[q]==j
            x[j] /= nz[q]
            q-=1
        end
        alpha = x[j]
        for k=p:q
            x[rv[k]] -= alpha*nz[k]
        end
    end
    return c
end
function solvemat(L::SparseMatrixCSC{Tv,Ti},B::SparseMatrixCSC{Tv,Ti};lowertriangular=true) where {Tv,Ti<:Integer}
    @assert size(L,2)==size(B,1)
    cp = B.colptr
    rv = B.rowval
    nz = B.nzval
    CP = Vector{Ti}(undef,B.n+1)
    CP[1] = 1
    CJ = Vector{Ti}(undef,L.n)
    mark = falses(L.n)
    for i in 1:B.n
        p = cp[i]-1
        q = cp[i+1]-1
        Jlen = q-p
        for j=1:Jlen
            CJ[j]=rv[j+p]
        end
        CP[i+1]=CP[i]+transitiveclosure(L,Jlen,CJ,mark,countonly=true)
    end
    N = CP[end]-1
    RV = Vector{Ti}(undef,N)
    NZ = Vector{Tv}(undef,N)
    x = zeros(Tv,L.n)
    for i = 1:B.n
        p = cp[i]
        q = cp[i+1]-1
        J = view(rv,p:q)
        p -= 1
        for j=1:length(J)
            x[J[j]] = nz[j+p]
        end
        d = solvevec(L,lowertriangular,x,J,CJ,mark)
        c = CP[i]-1
        for j=1:d
            k = CJ[j]
            RV[c+j] = k
            NZ[c+j] = x[k]
            x[CJ[j]] = 0
        end
    end
    SparseMatrixCSC{Tv,Ti}(B.m,B.n,CP,RV,NZ)
end

@enum SolveMode lower=1 upper=2 detect=3

"""
    function solve(L::SparseMatrixCSC{Tv,Ti},B::SparseMatrixCSC{Tv,Ti};solvemode=detect,numthreads=min(B.n,nthreads())) where {Tv,Ti<:Integer}

Solve `L*X=B` for the unknown `X`, where `L` and `B` are sparse matrices. `L` should be either lower or upper triangular. If `numthreads>1` then multithreading is used. `solvemode` should be either `lower`, `upper` or `detect`.
"""
function solve(L::SparseMatrixCSC{Tv,Ti},B::SparseMatrixCSC{Tv,Ti};solvemode=detect,numthreads=min(B.n, Threads.nthreads())) where {Tv,Ti<:Integer}
    if solvemode==detect
        if istril(L)
            solvemode=lower
        elseif istriu(L)
            solvemode=upper
        else
            error("`solve` can only be used on lower or upper triangular matrices")
        end
    end
    if numthreads==1
        return solvemat(L,B;lowertriangular=(solvemode==lower))
    end
    dk = B.n/numthreads
    X = Array{SparseMatrixCSC{Tv,Ti}}(undef,numthreads)
    Threads.@threads for j=1:numthreads
        a = (j==1) ? 1 : (Int(floor(j*dk)+1))
        b = (j==numthreads) ? B.n : Int(floor((j+1)*dk))
        X[j] = solvemat(L,B[:,a:b];lowertriangular=(solvemode==lower))
    end
    return hcat(X...)
end



"""
    function Base.:\\(A::luFac, B::SparseMatrixCSC)

Solve the problem `A*X=B` for the unknown `X`, where `A` is a luFac object and `B` is sparse.
"""
function Base.:\(A::luFac{Tv,Ti}, B::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti<:Integer}
    if !ismissing(A.p) B = B[A.p,:]                     end
    if !ismissing(A.L) B = solve(A.L,B;solvemode=lower) end
    if !ismissing(A.U) B = solve(A.U,B;solvemode=upper) end
    if !ismissing(A.q) B = B[A.q,:]                     end
    return B
end
Base.:\(A::luFac{Tv,Ti}, B::SparseVector{Tv,Ti}) where {Tv,Ti<:Integer} = SparseVector(A\SparseMatrixCSC(B))
Base.:\(A::SparseMatrixCSC{Tv,Ti}, B::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti<:Integer} = luFac(A)\B
Base.:\(A::SparseMatrixCSC{Tv,Ti}, B::SparseVector{Tv,Ti}) where {Tv,Ti<:Integer} = luFac(A)\B
Base.transpose(A::luFac) = luFac(ismissing(A.U) ? missing : sparse(transpose(A.U)),
                                                 ismissing(A.L) ? missing : sparse(transpose(A.L)),
                                                 ismissing(A.q) ? missing : invperm(A.q),
                                                 ismissing(A.p) ? missing : invperm(A.p))
Base.:/(A::SparseMatrixCSC{Tv,Ti}, B::luFac{Tv,Ti}) where {Tv,Ti<:Integer} = sparse(transpose(transpose(B)\sparse(transpose(A))))
Base.:/(A::SparseMatrixCSC{Tv,Ti}, B::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti<:Integer} = A/luFac(B)
Base.inv(A::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti<:Integer} = A\spdiagm(0=>ones(Tv,size(A,1)))
