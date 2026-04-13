import Foundation

struct WorkspaceDocument: Codable, Equatable {
    static let currentVersion = 1

    private static let defaultCanvas: [String: JSONValue] = [
        "panX": .number(0),
        "panY": .number(0),
        "scale": .number(1),
    ]

    private var storage: [String: JSONValue]

    init(rawObject: [String: JSONValue] = [:]) {
        storage = Self.normalized(rawObject)
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let object = try container.decode([String: JSONValue].self)
        storage = Self.normalized(object)
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

    private static func normalized(_ rawObject: [String: JSONValue]) -> [String: JSONValue] {
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
}

extension WorkspaceDocument {
    enum WorkspaceDocumentError: LocalizedError, Equatable {
        case invalidVersion(Int)
        case unsupportedVersion(Int, supportedVersion: Int)

        var errorDescription: String? {
            switch self {
            case let .invalidVersion(version):
                return "Workspace version \(version) is invalid."
            case let .unsupportedVersion(version, supportedVersion):
                return "Workspace version \(version) is newer than this app supports (\(supportedVersion))."
            }
        }
    }
}

enum JSONValue: Codable, Equatable {
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
