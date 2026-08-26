export find_bounds

@inline function _regime_laplacian_nonzero(
    north::Integer,
    south::Integer,
    west::Integer,
    east::Integer,
    center::Integer,
)
    # Widen all label arithmetic so a large integer regime identifier cannot
    # overflow and silently change the boundary predicate.
    return big(north) + big(south) + big(west) + big(east) !=
        4 * big(center)
end

"""
    find_bounds(lattice) -> BitMatrix

Compute the nonzero support of the two-dimensional five-point Laplacian with
replicated boundary values. This is the numerical regime-boundary operation
historically used by the plotting layer, but it is deliberately independent of
ImageFiltering so headless engine and Web runtimes have identical behavior.
`lattice` must contain integer regime identifiers and use one-based axes.
"""
function find_bounds(lattice::AbstractMatrix{T}) where {T<:Integer}
    Base.require_one_based_indexing(lattice)
    edge_map = falses(size(lattice))
    isempty(lattice) && return edge_map

    rows = axes(lattice, 1)
    columns = axes(lattice, 2)
    first_row, last_row = first(rows), last(rows)
    first_column, last_column = first(columns), last(columns)

    @inbounds for (output_column, column) in enumerate(columns)
        west_column = column == first_column ? column : column - 1
        east_column = column == last_column ? column : column + 1
        for (output_row, row) in enumerate(rows)
            north_row = row == first_row ? row : row - 1
            south_row = row == last_row ? row : row + 1
            edge_map[output_row, output_column] = _regime_laplacian_nonzero(
                lattice[north_row, column],
                lattice[south_row, column],
                lattice[row, west_column],
                lattice[row, east_column],
                lattice[row, column],
            )
        end
    end
    return edge_map
end
