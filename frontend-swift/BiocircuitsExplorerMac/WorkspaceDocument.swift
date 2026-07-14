import Foundation

nonisolated struct WorkspaceDocument: Codable, Equatable, Sendable {
    static let currentVersion = 1

    // Keep this set aligned with NODE_TYPES in
    // webapp/public/js/node-types/index.js. The native decoder must reject a
    // document that the embedded workspace cannot restore.
    private static let supportedNodeTypes: Set<String> = [
        "ai-import",
        "atlas-builder",
        "atlas-inverse-result",
        "atlas-query-config",
        "atlas-query-result",
        "atlas-spec",
        "design-spec-config",
        "design-target",
        "fret-heatmap",
        "fret-params",
        "fret-result",
        "markdown-note",
        "model-builder",
        "model-summary",
        "network-id-definition",
        "parameter-scan-1d",
        "parameter-scan-2d",
        "placer-params",
        "placer-result",
        "qk-poly-result",
        "reaction-network",
        "regime-graph",
        "rop-cloud",
        "rop-cloud-params",
        "rop-cloud-result",
        "rop-poly-params",
        "rop-poly-result",
        "rop-polyhedron",
        "rop-shape-edit-config",
        "rop-shape-result",
        "sbml-export",
        "sbml-import",
        "scan-1d-params",
        "scan-1d-result",
        "scan-2d-params",
        "scan-2d-result",
        "siso-analysis",
        "siso-params",
        "siso-result",
        "vertices-table",
    ]

    private static let minimumCanvasScale = 0.005
    private static let maximumCanvasScale = 3.0
    private static let maximumCanvasPan = 1_000_000_000.0

    private static let defaultCanvas: [String: JSONValue] = [
        "panX": .number(0),
        "panY": .number(0),
        "scale": .number(1),
    ]

    private var storage: [String: JSONValue]

    init(rawObject: [String: JSONValue] = [:]) {
        storage = Self.normalizedTrustedObject(rawObject)
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let object = try container.decode([String: JSONValue].self)
        storage = try Self.normalizedDecodedObject(object)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(storage)
    }

    var rawObject: [String: JSONValue] {
        storage
    }

    var version: Int {
        storage["version"]?.intValue ?? Self.currentVersion
    }

    func validatedForPersistence() throws -> WorkspaceDocument {
        guard version >= 1 else {
            throw WorkspaceDocumentError.invalidVersion(version)
        }

        guard version <= Self.currentVersion else {
            throw WorkspaceDocumentError.unsupportedVersion(version, supportedVersion: Self.currentVersion)
        }

        return self
    }

    static func starter(named _: String) -> WorkspaceDocument {
        WorkspaceDocument(rawObject: [
            "version": .number(Double(currentVersion)),
            "timestamp": .string(ISO8601DateFormatter().string(from: Date())),
            "canvas": .object(defaultCanvas),
            "nodes": .array([]),
            "connections": .array([]),
        ])
    }

    private static func normalizedTrustedObject(_ rawObject: [String: JSONValue]) -> [String: JSONValue] {
        var normalized = rawObject

        if normalized["version"]?.intValue == nil {
            normalized["version"] = .number(Double(currentVersion))
        }

        if let canvas = normalized["canvas"]?.objectValue {
            normalized["canvas"] = .object(defaultCanvas.merging(canvas) { _, newValue in newValue })
        } else {
            normalized["canvas"] = .object(defaultCanvas)
        }

        if normalized["nodes"]?.arrayValue == nil {
            normalized["nodes"] = .array([])
        }

        if normalized["connections"]?.arrayValue == nil {
            normalized["connections"] = .array([])
        }

        return normalized
    }

    private static func normalizedDecodedObject(
        _ rawObject: [String: JSONValue]
    ) throws -> [String: JSONValue] {
        var normalized = rawObject

        if let suppliedVersion = normalized["version"], suppliedVersion != .null {
            guard suppliedVersion.intValue != nil else {
                throw WorkspaceDocumentError.invalidField("version", expected: "an integer")
            }
        } else {
            normalized["version"] = .number(Double(currentVersion))
        }

        let sourceCanvas: [String: JSONValue]
        if let suppliedCanvas = normalized["canvas"], suppliedCanvas != .null {
            guard let canvas = suppliedCanvas.objectValue else {
                throw WorkspaceDocumentError.invalidField("canvas", expected: "an object")
            }
            sourceCanvas = canvas
        } else {
            sourceCanvas = [:]
        }

        var canvas = sourceCanvas
        let panX = try finiteNumber(sourceCanvas["panX"], path: "canvas.panX", fallback: 0)
        let panY = try finiteNumber(sourceCanvas["panY"], path: "canvas.panY", fallback: 0)
        let scale = try finiteNumber(sourceCanvas["scale"], path: "canvas.scale", fallback: 1)
        guard scale >= minimumCanvasScale, scale <= maximumCanvasScale else {
            throw WorkspaceDocumentError.invalidField(
                "canvas.scale",
                expected: "between \(minimumCanvasScale) and \(maximumCanvasScale)"
            )
        }
        guard abs(panX) <= maximumCanvasPan, abs(panY) <= maximumCanvasPan else {
            throw WorkspaceDocumentError.invalidField(
                "canvas.panX/canvas.panY",
                expected: "between -\(maximumCanvasPan) and \(maximumCanvasPan)"
            )
        }
        canvas["panX"] = .number(panX)
        canvas["panY"] = .number(panY)
        canvas["scale"] = .number(scale)
        normalized["canvas"] = .object(canvas)

        guard let suppliedNodes = normalized["nodes"]?.arrayValue else {
            throw WorkspaceDocumentError.invalidField("nodes", expected: "an array")
        }
        normalized["nodes"] = .array(try normalizedNodes(suppliedNodes))

        if let suppliedConnections = normalized["connections"], suppliedConnections != .null {
            guard suppliedConnections.arrayValue != nil else {
                throw WorkspaceDocumentError.invalidField("connections", expected: "an array")
            }
        } else {
            normalized["connections"] = .array([])
        }

        return normalized
    }

    private static func normalizedNodes(_ values: [JSONValue]) throws -> [JSONValue] {
        var seenNodeIDs: Set<String> = []

        return try values.enumerated().map { index, value in
            let path = "nodes[\(index)]"
            guard case let .object(sourceNode) = value else {
                throw WorkspaceDocumentError.invalidField(path, expected: "an object")
            }

            guard case let .string(id)? = sourceNode["id"],
                  !id.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            else {
                throw WorkspaceDocumentError.invalidField("\(path).id", expected: "a non-empty string")
            }
            guard seenNodeIDs.insert(id).inserted else {
                throw WorkspaceDocumentError.invalidField("\(path).id", expected: "a unique node ID")
            }

            guard case let .string(type)? = sourceNode["type"], supportedNodeTypes.contains(type) else {
                throw WorkspaceDocumentError.invalidField("\(path).type", expected: "a supported node type")
            }

            var node = sourceNode
            if let data = sourceNode["data"], data != .null {
                guard data.objectValue != nil else {
                    throw WorkspaceDocumentError.invalidField("\(path).data", expected: "an object")
                }
            } else {
                node["data"] = .object([:])
            }

            node["x"] = .number(try finiteNumber(sourceNode["x"], path: "\(path).x", fallback: 0))
            node["y"] = .number(try finiteNumber(sourceNode["y"], path: "\(path).y", fallback: 0))

            for dimension in ["width", "height"] {
                guard let suppliedDimension = sourceNode[dimension], suppliedDimension != .null else {
                    continue
                }
                let number = try finiteNumber(
                    suppliedDimension,
                    path: "\(path).\(dimension)",
                    fallback: 0
                )
                guard number >= 0 else {
                    throw WorkspaceDocumentError.invalidField(
                        "\(path).\(dimension)",
                        expected: "a non-negative finite number"
                    )
                }
                if number == 0 {
                    node.removeValue(forKey: dimension)
                } else {
                    node[dimension] = .number(number)
                }
            }

            return .object(node)
        }
    }

    private static func finiteNumber(
        _ value: JSONValue?,
        path: String,
        fallback: Double
    ) throws -> Double {
        guard let value, value != .null else {
            return fallback
        }
        guard case let .number(number) = value, number.isFinite else {
            throw WorkspaceDocumentError.invalidField(path, expected: "a finite number")
        }
        return number
    }
}

extension WorkspaceDocument {
    enum WorkspaceDocumentError: LocalizedError, Equatable {
        case invalidVersion(Int)
        case unsupportedVersion(Int, supportedVersion: Int)
        case invalidField(String, expected: String)

        var errorDescription: String? {
            switch self {
            case let .invalidVersion(version):
                return "Workspace version \(version) is invalid."
            case let .unsupportedVersion(version, supportedVersion):
                return "Workspace version \(version) is newer than this app supports (\(supportedVersion))."
            case let .invalidField(field, expected):
                return "Workspace field '\(field)' must be \(expected)."
            }
        }
    }
}

nonisolated enum JSONValue: Codable, Equatable, Sendable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case object([String: JSONValue])
    case array([JSONValue])
    case null

    var intValue: Int? {
        guard case let .number(value) = self else {
            return nil
        }

        return Int(exactly: value)
    }

    var objectValue: [String: JSONValue]? {
        guard case let .object(value) = self else {
            return nil
        }

        return value
    }

    var arrayValue: [JSONValue]? {
        guard case let .array(value) = self else {
            return nil
        }

        return value
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([String: JSONValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([JSONValue].self) {
            self = .array(value)
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value.")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()

        switch self {
        case let .string(value):
            try container.encode(value)
        case let .number(value):
            try container.encode(value)
        case let .bool(value):
            try container.encode(value)
        case let .object(value):
            try container.encode(value)
        case let .array(value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}
