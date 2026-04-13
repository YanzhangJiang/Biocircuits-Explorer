//
//  BiocircuitsExplorerMacTests.swift
//  BiocircuitsExplorerMacTests
//
//  Created by jyzer resico on 3/16/26.
//

import Foundation
import Testing
@testable import BiocircuitsExplorerMac

struct BiocircuitsExplorerMacTests {

    @Test func workspaceDocumentNormalizesRequiredFields() async throws {
        let data = """
        {
          "custom": "kept"
        }
        """.data(using: .utf8)!

        let document = try JSONDecoder().decode(WorkspaceDocument.self, from: data)

        #expect(document.version == WorkspaceDocument.currentVersion)
        #expect(document.rawObject["custom"] == JSONValue.string("kept"))
        #expect(document.rawObject["nodes"] == JSONValue.array([]))
        #expect(document.rawObject["connections"] == JSONValue.array([]))

        guard case let .object(canvas)? = document.rawObject["canvas"] else {
            Issue.record("Expected a normalized canvas object")
            return
        }

        #expect(canvas["panX"] == JSONValue.number(0))
        #expect(canvas["panY"] == JSONValue.number(0))
        #expect(canvas["scale"] == JSONValue.number(1))
    }

    @Test func workspaceDocumentPreservesUnknownFieldsDuringRoundTrip() async throws {
        let original = WorkspaceDocument(rawObject: [
            "version": JSONValue.number(2),
            "timestamp": JSONValue.string("2026-03-17T00:00:00Z"),
            "canvas": JSONValue.object([
                "panX": JSONValue.number(12),
                "panY": JSONValue.number(-4),
                "scale": JSONValue.number(1.5),
                "future": JSONValue.string("field"),
            ]),
            "nodes": JSONValue.array([
                JSONValue.object([
                    "id": JSONValue.string("node-1"),
                    "type": JSONValue.string("reaction-network"),
                    "extra": JSONValue.bool(true),
                ]),
            ]),
            "connections": JSONValue.array([]),
            "futureTopLevel": JSONValue.object([
                "flag": JSONValue.bool(true),
            ]),
        ])

        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(WorkspaceDocument.self, from: encoded)

        #expect(decoded.rawObject["futureTopLevel"] == JSONValue.object(["flag": JSONValue.bool(true)]))
        guard
            case let .array(nodes)? = decoded.rawObject["nodes"],
            case let .object(firstNode) = nodes.first
        else {
            Issue.record("Expected a preserved node payload")
            return
        }

        #expect(firstNode["extra"] == JSONValue.bool(true))
    }

}
