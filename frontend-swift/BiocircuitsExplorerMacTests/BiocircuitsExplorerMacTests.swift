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

    @Test func bundledDesignChatDiscoveryIncludesPackagedBackendTree() async throws {
        let resources = URL(fileURLWithPath: "/Applications/Biocircuits Explorer.app/Contents/Resources")
        let candidates = DesignChatBackendController
            .bundledChatScriptCandidates(resourceURL: resources)
            .map(\.path)

        #expect(candidates.first == resources.appendingPathComponent("scripts/chat_api.py").path)
        #expect(candidates.contains(
            resources.appendingPathComponent(
                "backend/share/biocircuits-explorer/webapp/scripts/chat_api.py"
            ).path
        ))
    }

    @Test func backendReadinessProbeRejectsUnrelatedOrWarmingServers() async throws {
        let ready = Data(#"{"status":"ready","checks":{"static_assets":true}}"#.utf8)
        let warming = Data(#"{"status":"not_ready"}"#.utf8)
        let unrelated = Data("<html>another service</html>".utf8)

        #expect(BiocircuitsBackendController.readinessProbeSucceeded(statusCode: 200, body: ready))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(statusCode: 503, body: warming))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(statusCode: 200, body: unrelated))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(statusCode: 204, body: Data()))
    }

    @Test func designChatUsesTheJuliaBackendPortWithoutChangingItsOwnPort() async throws {
        #expect(DesignChatBackendController.resolveConfiguredEnginePort(from: [:]) == 18_088)
        #expect(DesignChatBackendController.resolveConfiguredEnginePort(
            from: ["BIOCIRCUITS_EXPLORER_PORT": "19001", "ROP_PORT": "19002"]
        ) == 19_001)

        let engineEnvironment = DesignChatBackendController.enginePortEnvironment(18_088)
        #expect(engineEnvironment["BIOCIRCUITS_EXPLORER_HOST"] == "127.0.0.1")
        #expect(engineEnvironment["BIOCIRCUITS_EXPLORER_PORT"] == "18088")
        #expect(engineEnvironment["ROP_HOST"] == "127.0.0.1")
        #expect(engineEnvironment["ROP_PORT"] == "18088")
        #expect(engineEnvironment["BNE_CHAT_PORT"] == nil)
    }

    @Test func nativeDesignChatRotatesAndPropagatesItsBearerContract() async throws {
        let firstToken = DesignChatBackendController.makeBearerToken()
        let secondToken = DesignChatBackendController.makeBearerToken()

        #expect(firstToken.count == 64)
        #expect(firstToken.range(of: "^[0-9a-f]{64}$", options: .regularExpression) != nil)
        #expect(secondToken.count == 64)
        #expect(firstToken != secondToken)

        let securityEnvironment = DesignChatBackendController.nativeSecurityEnvironment(
            enginePort: 18_088,
            bearerToken: firstToken
        )
        #expect(securityEnvironment["BNE_CHAT_ALLOWED_ORIGIN"] == "http://127.0.0.1:18088")
        #expect(securityEnvironment["BNE_CHAT_BEARER_TOKEN"] == firstToken)
        #expect(securityEnvironment["BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK"] == "0")

        let request = DesignChatBackendController.authenticatedRequest(
            url: URL(string: "http://127.0.0.1:8765/health")!,
            bearerToken: firstToken,
            allowedOrigin: "http://127.0.0.1:18088"
        )
        #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer \(firstToken)")
        #expect(request.value(forHTTPHeaderField: "Origin") == "http://127.0.0.1:18088")
    }

    @Test func awsRuntimeFileCannotOverrideNativeBackendBootstrap() async throws {
        let runtimeEnvironment = [
            "AWS_REGION": "us-west-2",
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE": "trusted-queue",
            "BIOCIRCUITS_EXPLORER_IMAGE": "unrelated-to-the-native-runtime",
            "BIOCIRCUITS_EXPLORER_HOST": "0.0.0.0",
            "BIOCIRCUITS_EXPLORER_PORT": "9999",
            "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": "/tmp/attacker-public",
            "BIOCIRCUITS_EXPLORER_PARENT_PID": "1",
            "ROP_HOST": "0.0.0.0",
            "ROP_PORT": "9999",
            "ROP_PUBLIC_DIR": "/tmp/attacker-public",
            "ROP_PARENT_PID": "1",
            "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES": "1",
            "UNRELATED_OPERATOR_SETTING": "ignored",
        ]
        let bootstrapEnvironment = [
            "HOME": "/tmp/biocircuits-test-home",
            "BIOCIRCUITS_EXPLORER_HOST": "127.0.0.1",
            "BIOCIRCUITS_EXPLORER_PORT": "18088",
            "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": "/safe/public",
            "BIOCIRCUITS_EXPLORER_PARENT_PID": "4242",
            "ROP_HOST": "127.0.0.1",
            "ROP_PORT": "18088",
            "ROP_PUBLIC_DIR": "/safe/public",
            "ROP_PARENT_PID": "4242",
        ]

        let secured = BiocircuitsBackendController.securedBackendEnvironment(
            runtimeEnvironment: runtimeEnvironment,
            bootstrapEnvironment: bootstrapEnvironment
        )

        for (key, expected) in bootstrapEnvironment {
            #expect(secured[key] == expected)
        }
        #expect(secured["AWS_REGION"] == "us-west-2")
        #expect(secured["BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE"] == "trusted-queue")
        #expect(secured["BIOCIRCUITS_EXPLORER_IMAGE"] == nil)
        #expect(secured["BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES"] == nil)
        #expect(secured["UNRELATED_OPERATOR_SETTING"] == nil)
    }

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
