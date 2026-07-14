import Foundation

nonisolated struct WorkspaceDocument: Codable, Equatable, Sendable {
    static let currentVersion = 2
    static let currentSchemaVersion = "bne-workspace/v2.0.0"

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

    private struct LegacyMigration: Sendable {
        let configType: String
        let resultType: String
        let resultKeys: Set<String>
    }

    private struct PortContract: Sendable {
        let inputs: [String: String]
        let outputs: [String: String]

        init(inputs: [String: String] = [:], outputs: [String: String] = [:]) {
            self.inputs = inputs
            self.outputs = outputs
        }
    }

    private static let legacyNodeMigrations: [String: LegacyMigration] = [
        "siso-analysis": LegacyMigration(
            configType: "siso-params",
            resultType: "siso-result",
            resultKeys: [
                "behaviorData", "overlayTrajectoryData", "selectedPath",
                "sisoPlotMode", "trajectoryData",
            ]
        ),
        "parameter-scan-1d": LegacyMigration(
            configType: "scan-1d-params",
            resultType: "scan-1d-result",
            resultKeys: ["scan1DResult", "scan1DResultMeta"]
        ),
        "parameter-scan-2d": LegacyMigration(
            configType: "scan-2d-params",
            resultType: "scan-2d-result",
            resultKeys: ["scan2DResult", "scan2DResultMeta"]
        ),
        "rop-cloud": LegacyMigration(
            configType: "rop-cloud-params",
            resultType: "rop-cloud-result",
            resultKeys: ["ropCloudData", "ropCloudPreset", "ropCloudRanges"]
        ),
        "fret-heatmap": LegacyMigration(
            configType: "fret-params",
            resultType: "fret-result",
            resultKeys: ["fretHeatmapData"]
        ),
        "rop-polyhedron": LegacyMigration(
            configType: "rop-poly-params",
            resultType: "rop-poly-result",
            resultKeys: ["fitInnerPoints", "ropPlotData"]
        ),
    ]

    private static let activeV2NodeTypes = supportedNodeTypes.subtracting(
        legacyNodeMigrations.keys
    )

    private static let commonResultKeys: Set<String> = [
        "artifact", "certificate_grade", "evidence", "evidence_grade",
        "lifecycle", "provenance", "warnings",
    ]

    private static let derivedDataKeys: [String: Set<String>] = [
        "model-builder": ["modelContext"],
        "siso-result": ["behaviorData", "overlayTrajectoryData", "selectedPath", "trajectoryData"],
        "qk-poly-result": ["polyhedronPayload", "selection"],
        "scan-1d-result": ["scan1DResult"],
        "scan-2d-result": ["scan2DResult"],
        "placer-result": ["placerResult"],
        "design-target": ["config"],
        "rop-cloud-result": ["ropCloudData"],
        "fret-result": ["fretHeatmapData"],
        "rop-poly-result": ["ropPlotData"],
        "rop-shape-result": ["ropShapeResult"],
        "atlas-builder": ["atlasData"],
        "atlas-query-result": ["queryData"],
        "atlas-inverse-result": ["inverseDesignData"],
        "model-summary": ["summaryData"],
        "vertices-table": ["verticesData"],
        "regime-graph": ["graphData"],
    ]

    private static let runtimeFieldNames: Set<String> = [
        "abortController", "backendSessionId", "backend_session_id",
        "executionTicket", "executionToken", "execution_ticket", "execution_token",
        "ownerEpoch", "ownerToken", "owner_epoch", "owner_token",
        "pollTimer", "poll_timer", "requestToken", "request_token",
        "runtimeSessionId", "runtime_session_id", "sessionId", "session_id",
        "timerId", "timer_id", "workspaceEpoch", "workspace_epoch",
    ]

    private static let portContracts: [String: PortContract] = [
        "markdown-note": PortContract(),
        "ai-import": PortContract(),
        "reaction-network": PortContract(outputs: ["reactions": "NetworkIR"]),
        "network-id-definition": PortContract(outputs: [
            "reactions": "NetworkIR", "atlas-network": "AtlasNetwork",
        ]),
        "model-builder": PortContract(
            inputs: ["reactions": "NetworkIR"], outputs: ["model": "ModelArtifact"]
        ),
        "atlas-builder": PortContract(
            inputs: ["atlas-spec": "AtlasSpec"], outputs: ["atlas": "AtlasArtifact"]
        ),
        "siso-params": PortContract(
            inputs: ["model": "ModelArtifact"], outputs: ["params": "SISOConfig"]
        ),
        "siso-result": PortContract(
            inputs: ["params": "SISOConfig"], outputs: ["result": "PathResult"]
        ),
        "qk-poly-result": PortContract(inputs: ["result": "PathResult"]),
        "scan-1d-params": PortContract(
            inputs: ["model": "ModelArtifact"], outputs: ["params": "Scan1DConfig"]
        ),
        "scan-2d-params": PortContract(
            inputs: ["model": "ModelArtifact"], outputs: ["params": "Scan2DConfig"]
        ),
        "scan-1d-result": PortContract(inputs: ["params": "Scan1DConfig"]),
        "scan-2d-result": PortContract(inputs: ["params": "Scan2DConfig"]),
        "placer-params": PortContract(
            inputs: ["model": "ModelArtifact"],
            outputs: ["params": "ParameterPlacerConfig"]
        ),
        "placer-result": PortContract(inputs: ["params": "ParameterPlacerConfig"]),
        "design-spec-config": PortContract(outputs: [
            "designability-spec": "DesignabilitySpec",
        ]),
        "design-target": PortContract(
            inputs: ["designability-spec": "DesignabilitySpec"],
            outputs: [
                "reactions": "NetworkIR",
                "rop-shape-reference": "ROPShapeReferenceArtifact",
            ]
        ),
        "rop-cloud-params": PortContract(
            inputs: ["reactions": "NetworkIR", "model": "ModelArtifact"],
            outputs: ["params": "ROPCloudConfig"]
        ),
        "rop-cloud-result": PortContract(inputs: ["params": "ROPCloudConfig"]),
        "fret-params": PortContract(
            inputs: ["model": "ModelArtifact"], outputs: ["params": "FRETConfig"]
        ),
        "fret-result": PortContract(inputs: ["params": "FRETConfig"]),
        "rop-poly-params": PortContract(
            inputs: ["model": "ModelArtifact"],
            outputs: ["params": "ROPPolyhedronConfig"]
        ),
        "rop-poly-result": PortContract(inputs: ["params": "ROPPolyhedronConfig"]),
        "rop-shape-edit-config": PortContract(
            inputs: ["rop-shape-reference": "ROPShapeReferenceArtifact"],
            outputs: ["rop-shape-request": "ROPShapeRequestArtifact"]
        ),
        "rop-shape-result": PortContract(
            inputs: ["rop-shape-request": "ROPShapeRequestArtifact"],
            outputs: ["rop-shape-result": "ROPShapeResultArtifact"]
        ),
        "atlas-spec": PortContract(
            inputs: ["atlas-network": "AtlasNetwork"], outputs: ["atlas-spec": "AtlasSpec"]
        ),
        "atlas-query-config": PortContract(outputs: ["atlas-query": "AtlasQuery"]),
        "atlas-query-result": PortContract(inputs: [
            "atlas": "AtlasArtifact", "atlas-query": "AtlasQuery",
        ]),
        "atlas-inverse-result": PortContract(inputs: [
            "atlas-spec": "AtlasSpec", "atlas": "AtlasArtifact", "atlas-query": "AtlasQuery",
        ]),
        "model-summary": PortContract(inputs: ["model": "ModelArtifact"]),
        "vertices-table": PortContract(inputs: ["model": "ModelArtifact"]),
        "regime-graph": PortContract(inputs: ["model": "ModelArtifact"]),
        "sbml-import": PortContract(outputs: ["reactions": "NetworkIR"]),
        "sbml-export": PortContract(inputs: ["reactions": "NetworkIR"]),
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

    private init(normalizedStorage: [String: JSONValue]) {
        storage = normalizedStorage
    }

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
        guard storage["schema_version"] == .string(Self.currentSchemaVersion) else {
            throw WorkspaceDocumentError.invalidField(
                "schema_version",
                expected: Self.currentSchemaVersion
            )
        }

        return WorkspaceDocument(
            normalizedStorage: try Self.normalizedDecodedObject(storage)
        )
    }

    static func starter(named _: String) -> WorkspaceDocument {
        WorkspaceDocument(rawObject: [
            "version": .number(Double(currentVersion)),
            "schema_version": .string(currentSchemaVersion),
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

        if normalized["schema_version"] != .string(currentSchemaVersion) {
            normalized["schema_version"] = .string(currentSchemaVersion)
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
        let sourceVersion: Int
        if let suppliedVersion = normalized["version"], suppliedVersion != .null {
            guard let version = suppliedVersion.intValue else {
                throw WorkspaceDocumentError.invalidField("version", expected: "an integer")
            }
            sourceVersion = version
        } else {
            sourceVersion = 1
        }
        guard sourceVersion >= 1 else {
            throw WorkspaceDocumentError.invalidVersion(sourceVersion)
        }
        guard sourceVersion <= currentVersion else {
            throw WorkspaceDocumentError.unsupportedVersion(
                sourceVersion,
                supportedVersion: currentVersion
            )
        }
        if sourceVersion == currentVersion {
            guard normalized["schema_version"] == .string(currentSchemaVersion) else {
                throw WorkspaceDocumentError.invalidField(
                    "schema_version",
                    expected: currentSchemaVersion
                )
            }
        }
        normalized["version"] = .number(Double(sourceVersion))

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
        let allowedNodeTypes = sourceVersion == 1 ? supportedNodeTypes : activeV2NodeTypes
        normalized["nodes"] = .array(try normalizedNodes(
            suppliedNodes,
            allowedNodeTypes: allowedNodeTypes
        ))

        if let suppliedConnections = normalized["connections"], suppliedConnections != .null {
            guard suppliedConnections.arrayValue != nil else {
                throw WorkspaceDocumentError.invalidField("connections", expected: "an array")
            }
        } else {
            normalized["connections"] = .array([])
        }

        guard case let .object(runtimeStripped) = strippingRuntimeFields(.object(normalized)) else {
            throw WorkspaceDocumentError.invalidField("document", expected: "an object")
        }
        normalized = runtimeStripped
        if sourceVersion == 1 {
            expandLegacyNodes(in: &normalized)
        }
        normalizeRestoredResults(in: &normalized)
        try normalizeConnections(in: &normalized, dropInvalid: sourceVersion == 1)
        try validateDesignAgent(in: normalized)
        try validateLifecycleRecords(in: normalized)
        normalized["version"] = .number(Double(currentVersion))
        normalized["schema_version"] = .string(currentSchemaVersion)

        return normalized
    }

    private static func normalizedNodes(
        _ values: [JSONValue],
        allowedNodeTypes: Set<String>
    ) throws -> [JSONValue] {
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

            guard case let .string(type)? = sourceNode["type"], allowedNodeTypes.contains(type) else {
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

    private static func strippingRuntimeFields(_ value: JSONValue) -> JSONValue {
        switch value {
        case let .array(values):
            return .array(values.map(strippingRuntimeFields))
        case let .object(object):
            var stripped: [String: JSONValue] = [:]
            for (key, child) in object where !runtimeFieldNames.contains(key) {
                stripped[key] = strippingRuntimeFields(child)
            }
            return .object(stripped)
        default:
            return value
        }
    }

    private static func allocateNodeID(_ base: String, reserved: inout Set<String>) -> String {
        var candidate = base
        var suffix = 2
        while reserved.contains(candidate) {
            candidate = "\(base)-\(suffix)"
            suffix += 1
        }
        reserved.insert(candidate)
        return candidate
    }

    private static func markHistorical(_ data: inout [String: JSONValue]) {
        let evidence: JSONValue
        if case let .object(lifecycle)? = data["lifecycle"],
           let existingEvidence = lifecycle["evidence"] {
            evidence = existingEvidence
        } else {
            evidence = data["evidence"] ?? .null
        }
        data["lifecycle"] = .object([
            "state": .string("historical"),
            "freshness": .string("historical"),
            "evidence": evidence,
        ])
        if case let .object(selectionLifecycle)? = data["selectionLifecycle"] {
            data["selectionLifecycle"] = .object([
                "state": .string("historical"),
                "freshness": .string("historical"),
                "evidence": selectionLifecycle["evidence"] ?? .null,
            ])
        }
        for metaKey in ["scan1DResultMeta", "scan2DResultMeta"] {
            guard case var .object(metadata)? = data[metaKey] else {
                continue
            }
            metadata["historical"] = .bool(true)
            data[metaKey] = .object(metadata)
        }
    }

    private static func expandLegacyNodes(in document: inout [String: JSONValue]) {
        guard case let .array(sourceNodes)? = document["nodes"] else {
            return
        }
        var reserved = Set(sourceNodes.compactMap { value -> String? in
            guard case let .object(node) = value, case let .string(id)? = node["id"] else {
                return nil
            }
            return id
        })
        var nodes: [JSONValue] = []
        var internalConnections: [JSONValue] = []

        for value in sourceNodes {
            guard case var .object(sourceNode) = value,
                  case let .string(sourceType)? = sourceNode["type"],
                  let migration = legacyNodeMigrations[sourceType],
                  case let .string(sourceID)? = sourceNode["id"]
            else {
                nodes.append(value)
                continue
            }
            let resultID = allocateNodeID("\(sourceID)--result", reserved: &reserved)
            let sourceData = sourceNode["data"]?.objectValue ?? [:]
            let resultKeys = migration.resultKeys.union(commonResultKeys)
            var configData: [String: JSONValue] = [:]
            var resultData: [String: JSONValue] = [:]
            for (key, dataValue) in sourceData {
                if resultKeys.contains(key) {
                    resultData[key] = dataValue
                } else {
                    configData[key] = dataValue
                }
            }
            sourceNode["type"] = .string(migration.configType)
            sourceNode["data"] = .object(configData)
            let width = sourceNode["width"]?.doubleValue
            let sourceWidth = width.map { $0.isFinite && $0 > 0 ? $0 : 420 } ?? 420
            let horizontalOffset = sourceWidth + 40
            var resultNode: [String: JSONValue] = [
                "id": .string(resultID),
                "type": .string(migration.resultType),
                "x": .number((sourceNode["x"]?.doubleValue ?? 0) + horizontalOffset),
                "y": .number(sourceNode["y"]?.doubleValue ?? 0),
                "data": .object(resultData),
            ]
            if !resultData.isEmpty {
                markHistorical(&resultData)
                resultNode["data"] = .object(resultData)
            }
            nodes.append(.object(sourceNode))
            nodes.append(.object(resultNode))
            internalConnections.append(.object([
                "fromNode": .string(sourceID),
                "fromPort": .string("params"),
                "toNode": .string(resultID),
                "toPort": .string("params"),
            ]))
        }

        document["nodes"] = .array(nodes)
        let existingConnections = document["connections"]?.arrayValue ?? []
        document["connections"] = .array(existingConnections + internalConnections)
    }

    private static func hasStoredDerivedResult(type: String, data: [String: JSONValue]) -> Bool {
        guard let keys = derivedDataKeys[type] else {
            return false
        }
        if type == "design-target" {
            guard case let .object(config)? = data["config"] else {
                return false
            }
            return config["resolvedDefinition"] != nil || config["selectedNid"] != nil
        }
        return keys.contains { key in
            guard let value = data[key] else {
                return false
            }
            return value != .null
        }
    }

    private static func normalizeRestoredResults(in document: inout [String: JSONValue]) {
        guard case let .array(sourceNodes)? = document["nodes"] else {
            return
        }
        document["nodes"] = .array(sourceNodes.map { value in
            guard case var .object(node) = value,
                  case let .string(type)? = node["type"]
            else {
                return value
            }
            var data = node["data"]?.objectValue ?? [:]
            if type == "model-builder" {
                data.removeValue(forKey: "built")
            }
            let hasPersistedCurrentState = ["lifecycle", "selectionLifecycle"].contains { key in
                guard case let .object(lifecycle)? = data[key] else {
                    return false
                }
                return lifecycle["freshness"] == .string("current")
                    || lifecycle["freshness"] == .string("historical")
                    || lifecycle["state"] == .string("running")
            }
            if hasStoredDerivedResult(type: type, data: data) || hasPersistedCurrentState {
                markHistorical(&data)
            }
            node["data"] = .object(data)
            return .object(node)
        })
    }

    private static func normalizeConnections(
        in document: inout [String: JSONValue],
        dropInvalid: Bool
    ) throws {
        guard case let .array(sourceConnections)? = document["connections"],
              case let .array(nodes)? = document["nodes"]
        else {
            return
        }
        let nodeTypes = Dictionary(uniqueKeysWithValues: nodes.compactMap { value -> (String, String)? in
            guard case let .object(node) = value,
                  case let .string(id)? = node["id"],
                  case let .string(type)? = node["type"]
            else {
                return nil
            }
            return (id, type)
        })
        var seen: Set<String> = []
        var kept: [JSONValue] = []
        for value in sourceConnections {
            guard case let .object(connection) = value,
                  case let .string(fromNode)? = connection["fromNode"], !fromNode.isEmpty,
                  case let .string(fromPort)? = connection["fromPort"], !fromPort.isEmpty,
                  case let .string(toNode)? = connection["toNode"], !toNode.isEmpty,
                  case let .string(toPort)? = connection["toPort"], !toPort.isEmpty,
                  fromNode != toNode,
                  let fromType = nodeTypes[fromNode],
                  let toType = nodeTypes[toNode],
                  let outputType = portContracts[fromType]?.outputs[fromPort],
                  let inputType = portContracts[toType]?.inputs[toPort],
                  outputType == inputType
            else {
                if dropInvalid {
                    continue
                }
                throw WorkspaceDocumentError.invalidField(
                    "connections",
                    expected: "declared endpoints with compatible typed ports"
                )
            }
            let key = [fromNode, fromPort, toNode, toPort].joined(separator: "\u{0}")
            guard seen.insert(key).inserted else {
                if dropInvalid {
                    continue
                }
                throw WorkspaceDocumentError.invalidField(
                    "connections",
                    expected: "unique semantic connections"
                )
            }
            kept.append(value)
        }
        document["connections"] = .array(kept)
    }

    private static func validateDesignAgent(in document: [String: JSONValue]) throws {
        guard let value = document["designAgent"] else {
            return
        }
        guard case let .object(agent) = value,
              case let .array(turns)? = agent["convo"],
              turns.count <= 60,
              agent["chatState"]?.objectValue != nil
        else {
            throw WorkspaceDocumentError.invalidField(
                "designAgent",
                expected: "an object with convo and chatState"
            )
        }
        for (index, turnValue) in turns.enumerated() {
            guard case let .object(turn) = turnValue,
                  case let .string(role)? = turn["role"],
                  role == "user" || role == "agent"
            else {
                throw WorkspaceDocumentError.invalidField(
                    "designAgent.convo[\(index)]",
                    expected: "a user or agent turn"
                )
            }
            if role == "user", case .string? = turn["text"] {
                continue
            }
            if role == "agent", turn["res"]?.objectValue != nil {
                continue
            }
            throw WorkspaceDocumentError.invalidField(
                "designAgent.convo[\(index)]",
                expected: role == "user" ? "a text string" : "a response object"
            )
        }
    }

    private static func validateLifecycleRecords(in document: [String: JSONValue]) throws {
        let states: Set<String> = [
            "empty", "running", "current", "failed", "blocked", "invalidated", "historical",
        ]
        let freshnessValues: Set<String> = ["empty", "current", "invalidated", "historical"]
        guard case let .array(nodes)? = document["nodes"] else {
            return
        }
        for (index, value) in nodes.enumerated() {
            guard case let .object(node) = value,
                  case let .object(data)? = node["data"]
            else {
                continue
            }
            for key in ["lifecycle", "selectionLifecycle"] {
                guard let lifecycleValue = data[key] else {
                    continue
                }
                guard case let .object(lifecycle) = lifecycleValue,
                      case let .string(state)? = lifecycle["state"], states.contains(state),
                      case let .string(freshness)? = lifecycle["freshness"],
                      freshnessValues.contains(freshness),
                      lifecycle["evidence"] != nil
                else {
                    throw WorkspaceDocumentError.invalidField(
                        "nodes[\(index)].data.\(key)",
                        expected: "a known state, freshness, and evidence field"
                    )
                }
            }
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

    var doubleValue: Double? {
        guard case let .number(value) = self else {
            return nil
        }

        return value
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
