using HTTP
using JSON3
using BiocircuitsExplorerBackend

function request(method, path, payload=nothing)
    headers = ["Content-Type" => "application/json"]
    body = payload === nothing ? UInt8[] : Vector{UInt8}(codeunits(JSON3.write(payload)))
    return HTTP.Request(method, path, headers, body)
end

BiocircuitsExplorerBackend.router(HTTP.Request("GET", "/"))

build_payload = Dict(
    "reactions" => ["E + S <-> C_ES", "E + P <-> C_EP"],
    "kd" => [1e-3, 1e-3],
    "session_id" => "precompile",
)

build_resp = BiocircuitsExplorerBackend.router(request("POST", "/api/build_model", build_payload))
build_json = JSON3.read(String(build_resp.body))

session_id = String(build_json["session_id"])

BiocircuitsExplorerBackend.router(request("POST", "/api/find_vertices", Dict("session_id" => session_id)))
