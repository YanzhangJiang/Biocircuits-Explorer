struct LocalJobCancelled <: Exception
    job_id::String
end

Base.showerror(io::IO, err::LocalJobCancelled) =
    print(io, "Local job ", err.job_id, " was cancelled")

struct LocalJobCancelToken
    job_id::String
    requested::Threads.Atomic{Bool}
end

LocalJobCancelToken(job_id::AbstractString) =
    LocalJobCancelToken(String(job_id), Threads.Atomic{Bool}(false))

function _request_cancel!(token::LocalJobCancelToken)
    Threads.atomic_xchg!(token.requested, true)
    return nothing
end

function _check_cancelled(token::LocalJobCancelToken)
    token.requested[] && throw(LocalJobCancelled(token.job_id))
    return nothing
end

_no_cancel_check() = nothing
