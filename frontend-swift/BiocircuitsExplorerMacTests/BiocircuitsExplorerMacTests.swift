//
//  BiocircuitsExplorerMacTests.swift
//  BiocircuitsExplorerMacTests
//
//  Created by jyzer resico on 3/16/26.
//

import AppKit
import Foundation
import Testing
import WebKit
@testable import BiocircuitsExplorerMac

struct BiocircuitsExplorerMacTests {

    @MainActor
    @Test func terminationRegistrationRejectsStaleViewCleanup() async throws {
        let coordinator = AppTerminationCoordinator()
        var preparations: [String] = []
        let staleSession = coordinator.makeSessionToken()
        let currentSession = coordinator.makeSessionToken()

        let staleToken = coordinator.registerPreparation(for: staleSession) {
            preparations.append("stale")
            return false
        }
        let currentToken = coordinator.registerPreparation(for: currentSession) {
            preparations.append("current")
            return true
        }

        // A delayed callback from the older view remains registered only as a
        // fallback; it cannot replace the newer mounted session.
        let delayedStaleToken = coordinator.registerPreparation(for: staleSession) {
            preparations.append("delayed-stale")
            return false
        }

        #expect(!coordinator.unregisterPreparation(staleToken))
        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparations == ["current"])

        #expect(coordinator.unregisterPreparation(currentToken))
        #expect(!(await coordinator.runRegisteredPreparation()))
        #expect(preparations == ["current", "delayed-stale"])

        #expect(coordinator.unregisterPreparation(delayedStaleToken))
        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparations == ["current", "delayed-stale"])
    }

    @MainActor
    @Test func disappearingViewCannotUnregisterPreparationWhileFinalSaveIsRunning() async throws {
        let coordinator = AppTerminationCoordinator()
        let session = coordinator.makeSessionToken()
        var saveStarted = false
        var finishSave: CheckedContinuation<Void, Never>?
        let token = coordinator.registerPreparation(for: session) {
            saveStarted = true
            await withCheckedContinuation { continuation in
                finishSave = continuation
            }
            return true
        }

        let disappearance = Task { @MainActor in
            await coordinator.prepareAndUnregister(token)
        }
        while !saveStarted {
            await Task.yield()
        }

        #expect(!coordinator.unregisterPreparation(token))
        finishSave?.resume()
        #expect(await disappearance.value)
        #expect(await coordinator.runRegisteredPreparation())
    }

    @MainActor
    @Test func mainWindowCloseIsRedirectedThroughApplicationTermination() async throws {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 320, height: 240),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        var requestedWindow: NSWindow?
        let coordinator = ApplicationTerminationWindowBridge.Coordinator { closingWindow in
            requestedWindow = closingWindow
        }
        coordinator.attach(to: window)

        #expect(window.delegate === coordinator)
        #expect(!coordinator.windowShouldClose(window))
        #expect(requestedWindow === window)

        coordinator.detach()
        #expect(window.delegate == nil)
    }

    @Test func bundledDesignChatDiscoveryIncludesPackagedBackendTree() async throws {
        let resources = URL(fileURLWithPath: "/Applications/Biocircuits Explorer.app/Contents/Resources")
        let candidates = DesignChatBackendController
            .bundledChatScriptCandidates(resourceURL: resources)
            .map(\.path)

        #expect(candidates.first == URL(fileURLWithPath:
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/" +
            "BiocircuitsExplorerBackend/share/biocircuits-explorer/" +
            "webapp/scripts/chat_api.py"
        ).path)
        #expect(candidates.contains(
            resources.appendingPathComponent(
                "backend/share/biocircuits-explorer/webapp/scripts/chat_api.py"
            ).path
        ))

        let pythonCandidates = DesignChatBackendController
            .bundledPythonExecutableCandidates(resourceURL: resources)
            .map(\.path)
        #expect(pythonCandidates.first == URL(fileURLWithPath:
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/" +
            "BiocircuitsExplorerBackend/python/bin/python3"
        ).path)
        #expect(pythonCandidates.contains(
            resources.appendingPathComponent("backend/python/bin/python3").path
        ))

        let backendRoots = BiocircuitsBackendController.bundledBackendRootCandidates(
            bundleURL: URL(fileURLWithPath: "/Applications/Biocircuits Explorer.app"),
            resourceURL: resources
        ).map(\.path)
        #expect(backendRoots.first ==
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/BiocircuitsExplorerBackend")
        #expect(backendRoots.contains(
            "/Applications/Biocircuits Explorer.app/Contents/Resources/backend"
        ))
    }

    @Test func backendReadinessProbeRejectsUnrelatedOrWarmingServers() async throws {
        let nonce = String(repeating: "a", count: 64)
        let ready = Data(#"{"status":"ready","service":"biocircuits-explorer-backend","instance_nonce":"\#(nonce)","checks":{"static_assets":true}}"#.utf8)
        let warming = Data(#"{"status":"not_ready","service":"biocircuits-explorer-backend","instance_nonce":"\#(nonce)"}"#.utf8)
        let missingIdentity = Data(#"{"status":"ready","checks":{"static_assets":true}}"#.utf8)
        let unrelated = Data("<html>another service</html>".utf8)

        #expect(BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: ready, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 503, body: warming, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: ready, expectedNonce: String(repeating: "b", count: 64)
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: missingIdentity, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: unrelated, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 204, body: Data(), expectedNonce: nonce
        ))
    }

    @MainActor
    @Test func backendLaunchLifecycleRejectsOldStartupAndTerminationEvents() async throws {
        var lifecycle = BackendLaunchLifecycle()
        let firstLaunch = lifecycle.advance()
        #expect(lifecycle.accepts(firstLaunch))

        let replacementLaunch = lifecycle.advance()
        #expect(!lifecycle.accepts(firstLaunch))
        #expect(lifecycle.accepts(replacementLaunch))
        do {
            try lifecycle.requireCurrent(firstLaunch)
            Issue.record("A superseded backend launch must not return as successful")
        } catch {
            #expect(error is CancellationError)
        }
        try lifecycle.requireCurrent(replacementLaunch)

        let stoppedGeneration = lifecycle.advance()
        #expect(!lifecycle.accepts(replacementLaunch))
        #expect(lifecycle.accepts(stoppedGeneration))
    }

    @Test func aCleanBackendExitIsUnexpectedUnlessStopWasRequested() async throws {
        #expect(!BiocircuitsBackendController.processTerminationWasExpected(
            stopRequested: false,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(BiocircuitsBackendController.processTerminationWasExpected(
            stopRequested: true,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(!DesignChatBackendController.processTerminationWasExpected(
            stopRequested: false,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(DesignChatBackendController.processTerminationWasExpected(
            stopRequested: true,
            terminationReason: .exit,
            terminationStatus: 0
        ))
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

        #expect(BiocircuitsBackendController.resolveConfiguredPort(
            from: ["BIOCIRCUITS_EXPLORER_PORT": "19001"]
        ) == 19_001)
        #expect(BiocircuitsBackendController.resolveConfiguredPort(from: [:]) == 18_088)
        #expect(DesignChatBackendController.resolveConfiguredPort(
            from: ["BNE_CHAT_PORT": "19002"],
            excluding: 19_001
        ) == 19_002)
    }

    @Test func nativeBackendKeepsStableCognitoOriginWhileChatUsesAnEphemeralPort() async throws {
        let enginePort = BiocircuitsBackendController.resolveConfiguredPort(from: [:])
        let chatPort = DesignChatBackendController.resolveConfiguredPort(
            from: [:],
            excluding: enginePort
        )
        #expect(enginePort == 18_088)
        #expect((1...65_535).contains(chatPort))
        #expect(chatPort != enginePort)
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
            bearerToken: firstToken,
            instanceNonce: secondToken
        )
        #expect(securityEnvironment["BNE_CHAT_ALLOWED_ORIGIN"] == "http://127.0.0.1:18088")
        #expect(securityEnvironment["BNE_CHAT_BEARER_TOKEN"] == firstToken)
        #expect(securityEnvironment["BNE_CHAT_INSTANCE_NONCE"] == secondToken)
        #expect(securityEnvironment["BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK"] == "0")

        let request = DesignChatBackendController.authenticatedRequest(
            url: URL(string: "http://127.0.0.1:8765/health")!,
            bearerToken: firstToken,
            allowedOrigin: "http://127.0.0.1:18088"
        )
        #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer \(firstToken)")
        #expect(request.value(forHTTPHeaderField: "Origin") == "http://127.0.0.1:18088")

        let identityBody = Data(#"{"service":"biocircuits-design-chat","instance_nonce":"\#(secondToken)"}"#.utf8)
        let healthBody = Data(#"{"ok":true,"service":"biocircuits-design-chat","instance_nonce":"\#(secondToken)"}"#.utf8)
        #expect(DesignChatBackendController.identityProbeSucceeded(
            statusCode: 200,
            body: identityBody,
            expectedNonce: secondToken
        ))
        #expect(!DesignChatBackendController.identityProbeSucceeded(
            statusCode: 200,
            body: identityBody,
            expectedNonce: firstToken
        ))
        #expect(DesignChatBackendController.healthProbeSucceeded(
            statusCode: 200,
            body: healthBody,
            expectedNonce: secondToken
        ))
    }

    @Test func nativeRuntimeWritesStayInApplicationSupport() async throws {
        let appSupport = URL(fileURLWithPath: "/tmp/BiocircuitsExplorerTests/Application Support")
        let nonce = String(repeating: "c", count: 64)
        let backendEnvironment = BiocircuitsBackendController.runtimeStorageEnvironment(
            applicationSupportDirectory: appSupport,
            instanceNonce: nonce
        )
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_INSTANCE_NONCE"] == nonce)
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_JOB_STORE"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/Jobs")
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/Atlas")

        let designEnvironment = DesignChatBackendController.runtimeStorageEnvironment(
            applicationSupportDirectory: appSupport
        )
        #expect(designEnvironment["BNE_TRACE_DIR"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/DesignAgentTraces")
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
          "custom": "kept",
          "nodes": []
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

    @Test func workspaceDocumentRejectsMalformedRequiredFields() throws {
        let malformedDocuments = [
            #"{"version":"1","nodes":[],"connections":[],"canvas":{}}"#,
            #"{"version":1,"nodes":{},"connections":[],"canvas":{}}"#,
            #"{"version":1,"nodes":[],"connections":{},"canvas":{}}"#,
            #"{"version":1,"nodes":[],"connections":[],"canvas":[]}"#,
        ]

        for json in malformedDocuments {
            #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
                _ = try JSONDecoder().decode(
                    WorkspaceDocument.self,
                    from: Data(json.utf8)
                )
            }
        }
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

    @MainActor
    @Test func webShellBridgeLifecycleRejectsStaleNavigationWork() async throws {
        var lifecycle = WebShellBridgeLifecycle(generation: "navigation-1")

        #expect(lifecycle.accepts("navigation-1"))
        let initialApplyAccepted = lifecycle.markProjectApplied(for: "navigation-1")
        #expect(initialApplyAccepted)
        #expect(lifecycle.currentProjectIsApplied)
        #expect(lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: false,
            isCapturingSnapshot: false
        ))
        #expect(!lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: true,
            isCapturingSnapshot: false
        ))
        #expect(!lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: false,
            isCapturingSnapshot: true
        ))

        lifecycle.beginNavigation(generation: "navigation-2")

        #expect(!lifecycle.accepts("navigation-1"))
        #expect(lifecycle.accepts("navigation-2"))
        #expect(!lifecycle.currentProjectIsApplied)
        let staleApplyAccepted = lifecycle.markProjectApplied(for: "navigation-1")
        #expect(!staleApplyAccepted)
        let currentApplyAccepted = lifecycle.markProjectApplied(for: "navigation-2")
        #expect(currentApplyAccepted)
        #expect(lifecycle.currentProjectIsApplied)
        lifecycle.markProjectUnapplied()
        #expect(!lifecycle.currentProjectIsApplied)
    }

    @MainActor
    @Test func webShellDoesNotPersistWebsiteDataAcrossAppSessions() async throws {
        let controller = WebShellController()
        #expect(!controller.webView.configuration.websiteDataStore.isPersistent)
    }

    @MainActor
    @Test func webShellNavigationQueueIsLatestWins() async throws {
        var queue = WebShellNavigationQueue()
        let firstURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        let latestURL = URL(string: "https://login.example.test/oauth2/authorize")!

        queue.enqueue(firstURL)
        queue.enqueue(latestURL, trust: .externalAuthentication)

        let selectedRequest = queue.takeLatestRequest()
        #expect(selectedRequest == WebShellNavigationRequest(
            url: latestURL,
            trust: .externalAuthentication
        ))
        #expect(queue.latestURL == nil)
        let noRemainingURL = queue.takeLatest()
        #expect(noRemainingURL == nil)
    }

    @MainActor
    @Test func webShellPersistenceQueuePreservesAutosaveAndFinalSnapshotOrder() async throws {
        let queue = WebShellPersistenceQueue()
        var events: [String] = []

        let autosave = queue.enqueue {
            events.append("autosave-start")
            try await Task.sleep(nanoseconds: 5_000_000)
            events.append("autosave-end")
        }
        let finalSnapshot = queue.enqueue {
            events.append("final-snapshot")
        }

        try await autosave.value.get()
        try await finalSnapshot.value.get()

        #expect(events == ["autosave-start", "autosave-end", "final-snapshot"])
    }

    @MainActor
    @Test func webShellOriginPolicyIsExactAndExternalAuthIsHTTPSOnly() async throws {
        guard let trustedOrigin = WebShellOrigin(
            url: URL(string: "http://127.0.0.1:18088/index-node.html")!
        ) else {
            Issue.record("Expected a valid local workspace origin")
            return
        }
        let authenticationOrigin = WebShellOrigin(
            url: URL(string: "https://login.example.test/oauth2/authorize")!
        )
        let policy = WebShellOriginPolicy(
            trustedOrigin: trustedOrigin,
            authenticationOrigin: authenticationOrigin
        )

        #expect(trustedOrigin.serialized == "http://127.0.0.1:18088")
        #expect(trustedOrigin.contains(
            URL(string: "http://127.0.0.1:18088/auth-callback.html?code=abc")!
        ))
        #expect(!trustedOrigin.contains(
            URL(string: "http://localhost:18088/index-node.html")!
        ))
        #expect(!trustedOrigin.contains(
            URL(string: "http://127.0.0.1:18089/index-node.html")!
        ))
        #expect(trustedOrigin.matches(
            protocol: "http",
            host: "127.0.0.1",
            port: 18088
        ))
        #expect(!trustedOrigin.matches(
            protocol: "http",
            host: "127.0.0.1",
            port: 18089
        ))

        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/index-node.html")!
        ) == .trustedWorkspace)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/auth-callback.html?code=abc")!
        ) == .localAuthenticationCallback)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/wiki.html")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "https://login.example.test/oauth2/authorize")!
        ) == .externalAuthentication)
        #expect(policy.disposition(
            for: URL(string: "https://phishing.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(WebShellOriginPolicy(trustedOrigin: trustedOrigin).disposition(
            for: URL(string: "https://login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "http://login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "https://user:password@login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "javascript:alert(1)")!
        ) == .blocked)
        #expect(WebShellController.allowsSubframeNavigation(
            disposition: .trustedWorkspace,
            navigationTrust: .trustedWorkspace
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: .trustedWorkspace
        ))
        #expect(WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: .externalAuthentication
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .trustedWorkspace,
            navigationTrust: .externalAuthentication
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .localAuthenticationCallback,
            navigationTrust: .localAuthenticationCallback
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: nil
        ))
        #expect(WebShellController.navigationTrust(for: .trustedWorkspace) == .trustedWorkspace)
        #expect(WebShellController.navigationTrust(
            for: .localAuthenticationCallback
        ) == .localAuthenticationCallback)
        #expect(WebShellController.navigationTrust(
            for: .externalAuthentication
        ) == .externalAuthentication)
        #expect(WebShellController.navigationTrust(for: .blocked) == nil)

        let enabledConfiguration = Data(
            #"{"enabled":true,"cognito_domain":"Login.Example.Test"}"#.utf8
        )
        #expect(WebShellController.authenticationOrigin(
            statusCode: 200,
            body: enabledConfiguration
        ) == authenticationOrigin)
        #expect(WebShellController.authenticationOrigin(
            statusCode: 503,
            body: enabledConfiguration
        ) == nil)
        for invalidBody in [
            Data(#"{"enabled":false,"cognito_domain":"login.example.test"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"evil.test/path"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"user@evil.test"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"evil.test:444"}"#.utf8),
        ] {
            #expect(WebShellController.authenticationOrigin(
                statusCode: 200,
                body: invalidBody
            ) == nil)
        }
        #expect(WebShellController.isExternalHTTPSURL(
            URL(string: "https://docs.example.test/guide")!
        ))
        #expect(!WebShellController.isExternalHTTPSURL(
            URL(string: "https://user:password@docs.example.test/guide")!
        ))
    }

    @MainActor
    @Test func webShellSnapshotCompletionIsBoundedAndExactlyOnce() async throws {
        var results: [WebShellSnapshotCaptureResult] = []
        let completion = WebShellSnapshotCaptureCompletion(generation: "navigation-1") {
            results.append($0)
        }

        #expect(completion.generation == "navigation-1")
        #expect(completion.resolve(.failed))
        #expect(!completion.resolve(.captured(WebShellCapturedProjectSnapshot(
            document: WorkspaceDocument(rawObject: ["revision": .number(2)]),
            sequence: 1
        ))))
        #expect(results == [.failed])
        #expect(WebShellController.snapshotCaptureTimeoutNanoseconds <= 3_000_000_000)
    }

    @MainActor
    @Test func webShellProjectChangeRequiresExactExplicitIdentity() async throws {
        let exactPayload: [String: Any] = ["projectID": "project-a"]
        let wrongPayload: [String: Any] = ["projectID": "project-b"]
        let nullPayload: [String: Any] = ["projectID": NSNull()]

        #expect(WebShellController.admittedProjectID(
            from: exactPayload,
            currentProjectID: "project-a"
        ) == "project-a")
        #expect(WebShellController.admittedProjectID(
            from: wrongPayload,
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.admittedProjectID(
            from: nullPayload,
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.admittedProjectID(
            from: [:],
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 7]) == 7)
        #expect(WebShellController.snapshotSequence(from: ["sequence": NSNumber(value: 8)]) == 8)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 0]) == nil)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 1.5]) == nil)
    }

    @MainActor
    @Test func webContentTerminationRecoveryTargetsCanonicalTrustedWorkspace() async throws {
        let workspaceURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        let callbackURL = URL(
            string: "http://127.0.0.1:18088/auth-callback.html?code=abc&state=xyz"
        )!
        let logoutReturnURL = URL(string: "http://127.0.0.1:18088/")!

        #expect(WebShellController.webContentRecoveryRequest(for: workspaceURL)
            == WebShellNavigationRequest(url: workspaceURL, trust: .trustedWorkspace))
        #expect(WebShellController.webContentRecoveryRequest(for: nil) == nil)
        #expect(WebShellController.trustedReturnURL(
            navigationURL: callbackURL,
            canonicalWorkspaceURL: workspaceURL
        ) == callbackURL)
        #expect(WebShellController.trustedReturnURL(
            navigationURL: logoutReturnURL,
            canonicalWorkspaceURL: workspaceURL
        ) == workspaceURL)
    }

    @MainActor
    @Test func failedWorkspaceCaptureDiscardsQueuedNavigation() async throws {
        var queue = WebShellNavigationQueue()
        let reloadURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        queue.enqueue(reloadURL)

        let failedCaptureMayNavigate = queue.allowNavigation(after: .failed)
        #expect(!failedCaptureMayNavigate)
        #expect(queue.latestURL == nil)

        queue.enqueue(reloadURL)
        let document = WorkspaceDocument(rawObject: ["name": .string("captured")])
        let successfulCaptureMayNavigate = queue.allowNavigation(after: .captured(
            WebShellCapturedProjectSnapshot(document: document, sequence: 1)
        ))
        #expect(successfulCaptureMayNavigate)
        #expect(queue.latestURL == reloadURL)
    }

    @MainActor
    @Test func webShellNavigationRequeuesCurrentProjectWithoutOverwritingNewerPendingWork() async throws {
        let currentDocument = WorkspaceDocument(rawObject: ["name": .string("current")])
        let pendingDocument = WorkspaceDocument(rawObject: ["name": .string("pending")])
        let pendingProject = WebShellController.PendingProject(
            id: "project-newer",
            document: pendingDocument
        )

        #expect(WebShellController.projectToReapply(
            pendingProject: nil,
            currentProjectID: "project-current",
            currentProjectDocument: currentDocument
        ) == WebShellController.PendingProject(id: "project-current", document: currentDocument))
        #expect(WebShellController.projectToReapply(
            pendingProject: pendingProject,
            currentProjectID: "project-current",
            currentProjectDocument: currentDocument
        ) == pendingProject)

        #expect(!WebShellController.hasNewerPendingProject(
            than: pendingProject,
            pendingProject: pendingProject
        ))
        #expect(WebShellController.hasNewerPendingProject(
            than: WebShellController.PendingProject(id: "project-old", document: currentDocument),
            pendingProject: pendingProject
        ))
    }

    @MainActor
    @Test func returningToCapturedProjectDoesNotReapplyItsStaleStoreCopy() async throws {
        let staleDocument = WorkspaceDocument(rawObject: ["revision": .number(1)])
        let otherDocument = WorkspaceDocument(rawObject: ["revision": .number(2)])

        let returnToCurrent = WebShellController.PendingProject(
            id: "project-a",
            document: staleDocument
        )
        #expect(WebShellController.pendingProjectAfterCapture(
            pendingProject: returnToCurrent,
            capturedProjectID: "project-a"
        ) == nil)

        let switchToOther = WebShellController.PendingProject(
            id: "project-b",
            document: otherDocument
        )
        #expect(WebShellController.pendingProjectAfterCapture(
            pendingProject: switchToOther,
            capturedProjectID: "project-a"
        ) == switchToOther)

        let renamedProject = WebShellController.PendingProject(
            id: "project-renamed",
            document: otherDocument
        )
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: returnToCurrent,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == nil)
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: renamedProject,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == nil)
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: switchToOther,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == switchToOther)
    }

    @MainActor
    @Test func identityHandoffKeepsWorkspaceLockedUntilQueuedReplacement() async throws {
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: true,
            hasPendingProject: false
        ) == .replaceByNavigation)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: false,
            hasPendingProject: true
        ) == .loadPendingProjectWhileLocked)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: true,
            hasPendingProject: true
        ) == .replaceByNavigation)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: false,
            hasPendingProject: false
        ) == .unlockCurrentWorkspace)
    }

    @MainActor
    @Test func fileOperationPersistsImmediateWebEditBeforeRename() async throws {
        let latestDocument = WorkspaceDocument(rawObject: ["revision": .number(2)])
        let snapshot = WebShellProjectSnapshot(id: "project-a", document: latestDocument)
        var events: [String] = []
        var persistedDocument: WorkspaceDocument?

        let renamedID = try await ProjectFileOperationCoordinator.run(
            capture: {
                events.append("capture")
                // WebShell capture now returns only after its ordered final
                // snapshot persistence has completed successfully.
                events.append("persist")
                persistedDocument = snapshot.document
                return snapshot
            },
            operation: {
                events.append("rename")
                #expect(persistedDocument == latestDocument)
                return "project-renamed"
            }
        )

        #expect(renamedID == "project-renamed")
        #expect(events == ["capture", "persist", "rename"])
    }

    @MainActor
    @Test func captureFailureDoesNotRenameOrOverwriteStoredDocument() async throws {
        let storedDocument = WorkspaceDocument(rawObject: ["revision": .number(1)])
        let persistedDocument = storedDocument
        var renamePerformed = false

        do {
            let _: String = try await ProjectFileOperationCoordinator.run(
                capture: {
                    throw WebShellFileOperationError.captureFailed("synthetic capture failure")
                },
                operation: {
                    renamePerformed = true
                    return "project-renamed"
                }
            )
            Issue.record("A failed capture must abort the file operation")
        } catch {
            #expect(error as? WebShellFileOperationError == .captureFailed("synthetic capture failure"))
        }

        #expect(!renamePerformed)
        #expect(persistedDocument == storedDocument)
    }

    @MainActor
    @Test func committedRenameUpdatesNativeIdentityBeforeWebRebindFailure() async throws {
        enum SyntheticRebindError: Error {
            case failed
        }

        var events: [String] = []
        var selectedProjectID = "project-old"

        do {
            _ = try await ProjectRenameCommitCoordinator.run(
                renameOnDisk: {
                    events.append("disk-rename")
                    return "project-new"
                },
                commitNativeState: { renamedProjectID in
                    events.append("native-selection")
                    selectedProjectID = renamedProjectID
                },
                rebindWebIdentity: { _ in
                    events.append("web-rebind")
                    throw SyntheticRebindError.failed
                }
            )
            Issue.record("Expected the synthetic WebKit rebind to fail")
        } catch SyntheticRebindError.failed {
            // The native identity commit must survive the later WebKit error.
        }

        #expect(events == ["disk-rename", "native-selection", "web-rebind"])
        #expect(selectedProjectID == "project-new")
    }

    @MainActor
    @Test func staleThemeCompletionCannotOverrideTheCurrentMode() async throws {
        #expect(ThemeCompletionLifecycle.accepts(
            requestedMode: "dark",
            currentMode: "dark"
        ))
        #expect(!ThemeCompletionLifecycle.accepts(
            requestedMode: "light",
            currentMode: "dark"
        ))
    }

    @MainActor
    @Test func webShellBridgeTagsMessagesAndCommitsIdentityAfterWorkspaceApply() async throws {
        let source = WebShellController.bridgeScriptSource(
            initialThemeMode: "auto",
            generation: "navigation-42",
            trustedOrigin: "http://127.0.0.1:18088"
        )

        #expect(source.contains("const bridgeGeneration = \"navigation-42\";"))
        #expect(source.contains("setFileOperationLocked(locked)"))
        #expect(source.contains("document.documentElement.inert = shouldLock"))
        #expect(source.contains("snapshotSequence: 0"))
        #expect(source.contains("sequence: shellState.snapshotSequence"))
        #expect(source.contains("captureProjectSnapshot()"))
        #expect(source.contains("rebindProjectIDAndCapture(projectID)"))
        #expect(source.contains("return postSnapshot(jsonString, true);"))
        #expect(source.contains("return applyFileOperationLock(locked);"))
        #expect(source.contains("const trustedOrigin = \"http://127.0.0.1:18088\";"))
        #expect(source.contains("const trustedPath = \"/index-node.html\";"))
        #expect(source.contains("if (window.location.origin !== trustedOrigin) return;"))
        #expect(source.contains("if (window.location.pathname !== trustedPath) return;"))
        #expect(source.contains(
            "handler.postMessage({ generation: bridgeGeneration, type, payload });"
        ))
        #expect(source.contains("rebindProjectID(projectID)"))
        #expect(source.contains("shellState.projectID = projectID;"))

        guard
            let finalCaptureStart = source.range(of: "rebindProjectIDAndCapture(projectID)"),
            let setLockStart = source.range(
                of: "setFileOperationLocked(locked)",
                range: finalCaptureStart.upperBound..<source.endIndex
            )
        else {
            Issue.record("Expected the identity final-capture bridge body")
            return
        }
        let finalCaptureBody = String(
            source[finalCaptureStart.lowerBound..<setLockStart.lowerBound]
        )
        #expect(finalCaptureBody.contains("return postSnapshot(jsonString, true);"))
        #expect(!finalCaptureBody.contains("applyFileOperationLock(false)"))

        guard
            let loadStart = source.range(of: "loadProjectFromJSONString(jsonString, projectID)"),
            let copyStart = source.range(of: "copyText(text)", range: loadStart.upperBound..<source.endIndex)
        else {
            Issue.record("Expected the native project-load bridge body")
            return
        }
        let loadBody = String(source[loadStart.lowerBound..<copyStart.lowerBound])
        guard
            let apply = loadBody.range(of: "applied = contract.applyWorkspaceFromJSONString(jsonString);"),
            let projectCommit = loadBody.range(of: "shellState.projectID = projectID;"),
            let fallbackSnapshotCommit = loadBody.range(of: "shellState.lastSnapshot = jsonString;"),
            let serialization = loadBody.range(
                of: "shellState.lastSnapshot = contract.serializeWorkspace?.() || jsonString;"
            ),
            let serializationFailure = loadBody.range(
                of: "Workspace applied, but snapshot serialization failed:"
            ),
            let successfulReturn = loadBody.range(
                of: "return true;",
                options: .backwards
            )
        else {
            Issue.record("Expected an apply-then-commit bridge sequence")
            return
        }

        #expect(loadBody.contains("if (applied === false) return false;"))
        #expect(loadBody.contains(
            "if (typeof projectID !== 'string' || projectID.length === 0) return false;"
        ))
        #expect(!loadBody.contains("projectID || shellState.projectID"))
        #expect(apply.lowerBound < projectCommit.lowerBound)
        #expect(projectCommit.lowerBound < serialization.lowerBound)
        #expect(fallbackSnapshotCommit.lowerBound < serialization.lowerBound)
        #expect(serialization.lowerBound < serializationFailure.lowerBound)
        #expect(serializationFailure.lowerBound < successfulReturn.lowerBound)
        #expect(loadBody.contains("diagnostics must not undo it"))
    }

}
