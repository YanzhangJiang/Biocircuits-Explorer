module Serialization

using HTTP
using JSON3

mat2vv(M::AbstractMatrix) = [collect(M[i,:]) for i in 1:size(M,1)]

json_safe_real(x::Real) = if isnan(x)
    "NaN"
elseif isinf(x)
    signbit(x) ? "-Inf" : "Inf"
else
    Float64(x)
end

json_safe_profile(profile::AbstractVector{<:Real}) = [json_safe_real(x) for x in profile]
json_safe_profile(profile::AbstractVector{<:AbstractVector{<:Real}}) =
    [json_safe_profile(coords) for coords in profile]

json_safe_value(x::Real) = json_safe_real(x)
json_safe_value(x::AbstractString) = x
json_safe_value(x::Symbol) = String(x)
json_safe_value(x::Nothing) = nothing
json_safe_value(x::Bool) = x
json_safe_value(x) = x

function json_safe_value(data::AbstractDict)
    sanitized = Dict{Any, Any}()
    for (key, value) in data
        safe_key = key isa Symbol ? String(key) : key
        sanitized[safe_key] = json_safe_value(value)
    end
    return sanitized
end

json_safe_value(data::Tuple)          = [json_safe_value(item) for item in data]
json_safe_value(data::NamedTuple)     = json_safe_value(Dict(pairs(data)))
json_safe_value(data::AbstractVector) = [json_safe_value(item) for item in data]
json_safe_value(data::AbstractSet)    = [json_safe_value(item) for item in collect(data)]

json_response(data; status::Integer = 200) = HTTP.Response(status,
    ["Content-Type" => "application/json"],
    JSON3.write(json_safe_value(data)))

error_response(msg; status::Integer = 400) =
    json_response(Dict("error" => msg); status = status)

function read_json(req)
    try
        return JSON3.read(String(req.body))
    catch e
        throw(ArgumentError("Invalid JSON: $(sprint(showerror, e))"))
    end
end

is_request_error(err) = err isa ArgumentError ||
                        err isa DomainError  ||
                        err isa BoundsError  ||
                        err isa KeyError

end # module
