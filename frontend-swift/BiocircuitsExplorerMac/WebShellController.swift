import AppKit
import Combine
import Foundation
import WebKit

struct WebShellOrigin: Equatable {
    let scheme: String
    let host: String
    let port: Int

    init?(url: URL) {
        guard
            let scheme = url.scheme?.lowercased(),
            scheme == "http" || scheme == "https",
            let host = url.host?.lowercased(),
            !host.isEmpty
        else {
            return nil
        }

        self.scheme = scheme
        self.host = host
        port = url.port ?? Self.defaultPort(for: scheme)
    }

    var serialized: String {
        var components = URLComponents()
        components.scheme = scheme
        components.host = host
        if port != Self.defaultPort(for: scheme) {
            components.port = port
        }
        return components.string ?? "\(scheme)://\(host):\(port)"
    }

    func contains(_ url: URL?) -> Bool {
        guard let url, let candidate = WebShellOrigin(url: url) else {
            return false
        }
        return candidate == self
    }

    func matches(protocol protocolName: String, host: String, port: Int) -> Bool {
        let normalizedProtocol = protocolName.lowercased()
        let normalizedPort = port > 0 ? port : Self.defaultPort(for: normalizedProtocol)
        return normalizedProtocol == scheme
            && host.lowercased() == self.host
            && normalizedPort == self.port
    }

    private static func defaultPort(for scheme: String) -> Int {
        scheme == "https" ? 443 : 80
    }
}

enum WebShellNavigationTrust: Equatable {
    case trustedWorkspace
    case localAuthenticationCallback
    case externalAuthentication
}

enum WebShellNavigationDisposition: Equatable {
    case trustedWorkspace
    case localAuthenticationCallback
    case externalAuthentication
    case blocked
}

struct WebShellOriginPolicy: Equatable {
    static let workspacePath = "/index-node.html"
    static let authenticationCallbackPath = "/auth-callback.html"

    let trustedOrigin: WebShellOrigin
    let authenticationOrigin: WebShellOrigin?

    init(
        trustedOrigin: WebShellOrigin,
        authenticationOrigin: WebShellOrigin? = nil
    ) {
        self.trustedOrigin = trustedOrigin
        self.authenticationOrigin = authenticationOrigin
    }

    func disposition(for url: URL) -> WebShellNavigationDisposition {
        if trustedOrigin.contains(url) {
            switch url.path {
            case Self.workspacePath:
                return .trustedWorkspace
            case Self.authenticationCallbackPath:
                return .localAuthenticationCallback
            default:
                return .blocked
            }
        }

        guard
            let authenticationOrigin,
            authenticationOrigin.scheme == "https",
            authenticationOrigin.contains(url),
            url.user == nil,
            url.password == nil
        else {
            return .blocked
        }
        return .externalAuthentication
    }
}

struct WebShellBridgeLifecycle {
    private(set) var generation: String
    private(set) var appliedGeneration: String?

    init(generation: String = UUID().uuidString) {
        self.generation = generation
    }

    @discardableResult
    mutating func beginNavigation(generation: String = UUID().uuidString) -> String {
        self.generation = generation
        appliedGeneration = nil
        return generation
    }

    func accepts(_ generation: String) -> Bool {
        generation == self.generation
    }

    @discardableResult
    mutating func markProjectApplied(for generation: String) -> Bool {
        guard accepts(generation) else {
            return false
        }

        appliedGeneration = generation
        return true
    }

    mutating func markProjectUnapplied() {
        appliedGeneration = nil
    }

    var currentProjectIsApplied: Bool {
        appliedGeneration == generation
    }

    func shouldCaptureBeforeNavigation(
        isReady: Bool,
        isLoadingProject: Bool,
        isCapturingSnapshot: Bool
    ) -> Bool {
        isReady && currentProjectIsApplied && !isLoadingProject && !isCapturingSnapshot
    }
}

struct WebShellNavigationRequest: Equatable {
    let url: URL
    let trust: WebShellNavigationTrust
}

enum WebShellIdentityHandoffAction: Equatable {
    case replaceByNavigation
    case loadPendingProjectWhileLocked
    case unlockCurrentWorkspace
}

struct WebShellNavigationQueue {
    private(set) var latestRequest: WebShellNavigationRequest?

    var latestURL: URL? {
        latestRequest?.url
    }

    mutating func enqueue(
        _ url: URL,
        trust: WebShellNavigationTrust = .trustedWorkspace
    ) {
        latestRequest = WebShellNavigationRequest(url: url, trust: trust)
    }

    mutating func takeLatest() -> URL? {
        takeLatestRequest()?.url
    }

    mutating func takeLatestRequest() -> WebShellNavigationRequest? {
        defer { latestRequest = nil }
        return latestRequest
    }

    mutating func discard() {
        latestRequest = nil
    }
}

struct WebShellCapturedProjectSnapshot: Equatable {
    let document: WorkspaceDocument
    let sequence: UInt64
}

enum WebShellSnapshotCaptureResult: Equatable {
    case captured(WebShellCapturedProjectSnapshot)
    case failed
}

struct WebShellProjectSnapshot: Equatable {
    let id: String
    let document: WorkspaceDocument
}

private struct WebShellSequencedProjectChange {
    let sequence: UInt64
    let document: WorkspaceDocument
}

private struct WebShellFinalizedProjectSnapshot: Decodable {
    let sequence: UInt64
    let jsonString: String
}

@MainActor
final class WebShellSnapshotCaptureCompletion {
    let generation: String
    private var completion: ((WebShellSnapshotCaptureResult) -> Void)?

    init(
        generation: String,
        completion: @escaping (WebShellSnapshotCaptureResult) -> Void
    ) {
        self.generation = generation
        self.completion = completion
    }

    @discardableResult
    func resolve(_ result: WebShellSnapshotCaptureResult) -> Bool {
        guard let completion else {
            return false
        }
        self.completion = nil
        completion(result)
        return true
    }
}

@MainActor
final class WebShellPersistenceQueue {
    typealias Operation = @MainActor () async throws -> Void

    private var tail: Task<Void, Never>?

    @discardableResult
    func enqueue(
        _ operation: @escaping Operation
    ) -> Task<Result<Void, Error>, Never> {
        let predecessor = tail
        let work: Task<Result<Void, Error>, Never> = Task { @MainActor in
            if let predecessor {
                await predecessor.value
            }

            do {
                try await operation()
                return .success(())
            } catch {
                return .failure(error)
            }
        }
        tail = Task { @MainActor in
            _ = await work.value
        }
        return work
    }
}

enum WebShellPersistenceError: LocalizedError {
    case unavailable

    var errorDescription: String? {
        "The native project store is unavailable, so the latest workspace snapshot was not saved."
    }
}

enum WebShellFileOperationError: LocalizedError, Equatable {
    case workspaceBusy
    case captureFailed(String)
    case workspaceChanged

    var errorDescription: String? {
        switch self {
        case .workspaceBusy:
            return "Wait for the current workspace switch or reload to finish, then try again."
        case let .captureFailed(message):
            return message
        case .workspaceChanged:
            return "The selected workspace changed while its latest edits were being captured. The file operation was not performed."
        }
    }
}

extension WebShellNavigationQueue {
    mutating func allowNavigation(after result: WebShellSnapshotCaptureResult) -> Bool {
        guard case .captured = result else {
            discard()
            return false
        }

        return true
    }
}

@MainActor
final class WebShellController: NSObject, ObservableObject {
    @Published private(set) var isReady = false
    @Published var lastErrorMessage: String?

    let webView: WKWebView
    private let contentController: WKUserContentController

    var onProjectChange: (@MainActor (String, WorkspaceDocument) async throws -> Void)?

    private var currentProjectID: String?
    private var currentProjectDocument: WorkspaceDocument?
    private var pendingProject: PendingProject?
    private var currentURL: URL?
    private var activeNavigation: WKNavigation?
    private var admittedMainFrameRequest: WebShellNavigationRequest?
    private var navigationTrust: WebShellNavigationTrust?
    private var trustedOrigin: WebShellOrigin?
    private var authenticationOrigin: WebShellOrigin?
    private var authenticationConfigurationTask: Task<Void, Never>?
    private var bridgeMessageHandlerInstalled = false
    private var isCapturingSnapshot = false
    private var identityChangeSourceProjectID: String?
    private var identityChangeTargetProjectID: String?
    private var deferredIdentityProjectChange: WebShellSequencedProjectChange?
    private var unlockWorkspaceAfterPendingProjectLoad = false
    private var latestPersistedSnapshotSequence: UInt64 = 0
    private var isLoadingProject = false
    private var injectedThemeMode: String
    private var bridgeLifecycle: WebShellBridgeLifecycle
    private var navigationQueue = WebShellNavigationQueue()
    private let persistenceQueue = WebShellPersistenceQueue()
    private var pendingSnapshotCapture: WebShellSnapshotCaptureCompletion?
    private var snapshotCaptureTimeoutTask: Task<Void, Never>?

    private var isProjectIdentityChangeInProgress: Bool {
        identityChangeSourceProjectID != nil
    }

    init(initialThemeMode: String = "auto") {
        injectedThemeMode = Self.normalizedThemeMode(initialThemeMode)
        let bridgeLifecycle = WebShellBridgeLifecycle()
        self.bridgeLifecycle = bridgeLifecycle
        let contentController = WKUserContentController()
        self.contentController = contentController

        let configuration = WKWebViewConfiguration()
        configuration.userContentController = contentController
        configuration.websiteDataStore = .nonPersistent()

        webView = WKWebView(frame: .zero, configuration: configuration)
        super.init()

        webView.navigationDelegate = self
        webView.uiDelegate = self
    }

    func prepareInitialThemeMode(_ mode: String) {
        let normalized = Self.normalizedThemeMode(mode)
        guard normalized != injectedThemeMode else {
            return
        }

        injectedThemeMode = normalized
    }

    func showProject(id: String, document: WorkspaceDocument) {
        let requestedProject = PendingProject(id: id, document: document)
        if currentProjectID == id, currentProjectDocument == document, pendingProject == nil {
            return
        }

        pendingProject = requestedProject
        if currentProjectID == id, !bridgeLifecycle.currentProjectIsApplied {
            currentProjectDocument = document
        }

        guard isReady else {
            return
        }

        guard !isCapturingSnapshot, !isLoadingProject else {
            return
        }

        if
            bridgeLifecycle.currentProjectIsApplied,
            let currentProjectID,
            currentProjectID != id
        {
            isCapturingSnapshot = true
            let generation = bridgeLifecycle.generation
            let capturedProjectID = currentProjectID
            captureCurrentWorkspaceSnapshot(for: generation) { [weak self] documentSnapshot in
                Task { @MainActor [weak self] in
                    guard let self else {
                        return
                    }

                    guard await self.finishSnapshotCapture(
                        documentSnapshot,
                        capturedProjectID: capturedProjectID,
                        generation: generation
                    ) else {
                        return
                    }
                    if self.performLatestQueuedNavigation() {
                        return
                    }

                    self.pushPendingProject()
                }
            }
            return
        }

        pushPendingProject()
    }

    func reloadShell() {
        guard let currentURL else {
            return
        }

        queueNavigation(to: currentURL, trust: .trustedWorkspace)
    }

    func loadBackend(url: URL) {
        guard
            let origin = WebShellOrigin(url: url),
            origin.scheme == "http",
            origin.host == "127.0.0.1",
            url.port != nil,
            url.path == WebShellOriginPolicy.workspacePath
        else {
            lastErrorMessage = "The embedded workspace must use the canonical /index-node.html path on an explicit 127.0.0.1 HTTP origin."
            return
        }

        if
            currentURL == url,
            webView.url == url,
            navigationQueue.latestURL == nil,
            isReady
        {
            return
        }

        currentURL = url
        trustedOrigin = origin
        refreshAuthenticationOrigin(for: origin)
        queueNavigation(to: url, trust: .trustedWorkspace)
    }

    private func refreshAuthenticationOrigin(for trustedOrigin: WebShellOrigin) {
        authenticationConfigurationTask?.cancel()
        authenticationOrigin = nil

        guard let configurationURL = URL(
            string: "\(trustedOrigin.serialized)/api/v1/auth/config"
        ) else {
            return
        }

        authenticationConfigurationTask = Task { @MainActor [weak self] in
            do {
                var request = URLRequest(url: configurationURL)
                request.cachePolicy = .reloadIgnoringLocalCacheData
                request.timeoutInterval = 10
                let (data, response) = try await URLSession.shared.data(for: request)
                try Task.checkCancellation()
                guard
                    let self,
                    self.trustedOrigin == trustedOrigin,
                    let response = response as? HTTPURLResponse
                else {
                    return
                }
                self.authenticationOrigin = Self.authenticationOrigin(
                    statusCode: response.statusCode,
                    body: data
                )
            } catch {
                guard !Task.isCancelled, self?.trustedOrigin == trustedOrigin else {
                    return
                }
                // Authentication stays fail-closed. The workspace remains fully
                // usable when Cognito is disabled or its bootstrap is unavailable.
                self?.authenticationOrigin = nil
            }
        }
    }

    static func authenticationOrigin(statusCode: Int, body: Data) -> WebShellOrigin? {
        struct Configuration: Decodable {
            let enabled: Bool
            let cognitoDomain: String?

            enum CodingKeys: String, CodingKey {
                case enabled
                case cognitoDomain = "cognito_domain"
            }
        }

        guard
            statusCode == 200,
            let configuration = try? JSONDecoder().decode(Configuration.self, from: body),
            configuration.enabled,
            let domain = configuration.cognitoDomain?.lowercased(),
            !domain.isEmpty,
            domain.unicodeScalars.allSatisfy({
                !$0.properties.isWhitespace && $0.isASCII
            }),
            !domain.contains("/"),
            !domain.contains("@"),
            !domain.contains(":"),
            let url = URL(string: "https://\(domain)"),
            url.host?.lowercased() == domain,
            let origin = WebShellOrigin(url: url),
            origin.scheme == "https",
            origin.port == 443
        else {
            return nil
        }
        return origin
    }

    static func isExternalHTTPSURL(_ url: URL) -> Bool {
        url.scheme?.lowercased() == "https"
            && url.host?.isEmpty == false
            && url.user == nil
            && url.password == nil
    }

    func captureCurrentProjectForFileOperation(
        projectIDs: Set<String>
    ) async throws -> WebShellProjectSnapshot? {
        try await captureCurrentProjectForFileOperation(
            projectIDs: projectIDs,
            retainWorkspaceLockOnSuccess: false
        )
    }

    func captureCurrentProjectForIdentityChange(
        projectID: String
    ) async throws -> WebShellProjectSnapshot? {
        guard currentProjectID == projectID else {
            return nil
        }
        guard !isProjectIdentityChangeInProgress else {
            throw WebShellFileOperationError.workspaceBusy
        }

        identityChangeSourceProjectID = projectID
        identityChangeTargetProjectID = nil
        deferredIdentityProjectChange = nil
        do {
            try await setWorkspaceInteractionLocked(true)
            return try await captureCurrentProjectForFileOperation(
                projectIDs: [projectID],
                retainWorkspaceLockOnSuccess: true
            )
        } catch {
            await cancelProjectIdentityChange()
            throw error
        }
    }

    private func captureCurrentProjectForFileOperation(
        projectIDs: Set<String>,
        retainWorkspaceLockOnSuccess: Bool
    ) async throws -> WebShellProjectSnapshot? {
        if let pendingProject, projectIDs.contains(pendingProject.id) {
            throw WebShellFileOperationError.workspaceBusy
        }
        guard let capturedProjectID = currentProjectID, projectIDs.contains(capturedProjectID) else {
            return nil
        }
        guard
            isReady,
            bridgeLifecycle.currentProjectIsApplied,
            !isCapturingSnapshot,
            !isLoadingProject,
            navigationQueue.latestURL == nil,
            pendingProject == nil
        else {
            throw WebShellFileOperationError.workspaceBusy
        }

        isCapturingSnapshot = true
        let generation = bridgeLifecycle.generation
        let result = await withCheckedContinuation { continuation in
            captureCurrentWorkspaceSnapshot(for: generation) { result in
                continuation.resume(returning: result)
            }
        }

        guard await finishSnapshotCapture(
            result,
            capturedProjectID: capturedProjectID,
            generation: generation,
            retainWorkspaceLockOnSuccess: retainWorkspaceLockOnSuccess
        ) else {
            throw WebShellFileOperationError.captureFailed(
                lastErrorMessage ?? "Failed to capture the latest workspace edits. The file operation was not performed."
            )
        }

        if !retainWorkspaceLockOnSuccess {
            if performLatestQueuedNavigation() {
                throw WebShellFileOperationError.workspaceChanged
            }
            if pendingProject != nil {
                pushPendingProject()
                throw WebShellFileOperationError.workspaceChanged
            }
        }
        guard case let .captured(snapshot) = result else {
            throw WebShellFileOperationError.captureFailed(
                "Failed to capture the latest workspace edits. The file operation was not performed."
            )
        }

        return WebShellProjectSnapshot(id: capturedProjectID, document: snapshot.document)
    }

    func rebindProjectIdentity(
        from oldProjectID: String,
        to newProjectID: String,
        document: WorkspaceDocument
    ) async throws {
        guard
            identityChangeSourceProjectID == oldProjectID,
            currentProjectID == oldProjectID
        else {
            await cancelProjectIdentityChange()
            throw WebShellFileOperationError.workspaceChanged
        }

        identityChangeTargetProjectID = newProjectID
        // During this short handoff, bridge messages carrying either the source
        // or target identity remain admissible and are sequenced below. This
        // closes the gap between the native file move and JavaScript rebind.
        currentProjectID = newProjectID
        currentProjectDocument = document

        do {
            try await finishProjectIdentityChange(persistenceProjectID: newProjectID)
        } catch {
            lastErrorMessage = error.localizedDescription
            throw error
        }
    }

    func cancelProjectIdentityChange() async {
        guard let sourceProjectID = identityChangeSourceProjectID else {
            return
        }
        let persistenceProjectID = identityChangeTargetProjectID ?? sourceProjectID
        do {
            try await finishProjectIdentityChange(
                persistenceProjectID: persistenceProjectID
            )
        } catch {
            lastErrorMessage = error.localizedDescription
        }
    }

    private func finishProjectIdentityChange(
        persistenceProjectID: String
    ) async throws {
        guard let sourceProjectID = identityChangeSourceProjectID else {
            return
        }

        var firstError: Error?
        do {
            let forcedSnapshot = try await captureFinalProjectIdentityInJavaScript(
                projectID: persistenceProjectID
            )
            deferIdentityProjectChange(forcedSnapshot)
        } catch {
            firstError = error
        }

        do {
            try await persistDeferredIdentityProjectChanges(
                projectID: persistenceProjectID
            )
        } catch {
            if firstError == nil {
                firstError = error
            }
        }

        pendingProject = Self.pendingProjectAfterIdentityChange(
            pendingProject: pendingProject,
            sourceProjectID: sourceProjectID,
            targetProjectID: identityChangeTargetProjectID
        )

        identityChangeSourceProjectID = nil
        identityChangeTargetProjectID = nil
        deferredIdentityProjectChange = nil
        isCapturingSnapshot = false

        let navigationStarted = performLatestQueuedNavigation()
        switch Self.identityHandoffAction(
            navigationStarted: navigationStarted,
            hasPendingProject: pendingProject != nil
        ) {
        case .replaceByNavigation:
            // A navigation replaces the locked document. Do not unlock the old
            // workspace in the interval between its final persistence and the
            // page transition.
            break

        case .loadPendingProjectWhileLocked:
            // Keep the old document inert until the queued project has actually
            // been applied. The load completion releases the retained lock.
            unlockWorkspaceAfterPendingProjectLoad = true
            pushPendingProject()

        case .unlockCurrentWorkspace:
            do {
                try await setWorkspaceInteractionLocked(false)
            } catch {
                if firstError == nil {
                    firstError = error
                }
            }
        }

        if let firstError {
            throw firstError
        }
    }

    static func identityHandoffAction(
        navigationStarted: Bool,
        hasPendingProject: Bool
    ) -> WebShellIdentityHandoffAction {
        if navigationStarted {
            return .replaceByNavigation
        }
        if hasPendingProject {
            return .loadPendingProjectWhileLocked
        }
        return .unlockCurrentWorkspace
    }

    private func deferIdentityProjectChange(
        _ change: WebShellSequencedProjectChange
    ) {
        guard change.sequence > latestPersistedSnapshotSequence else {
            return
        }
        guard
            let existing = deferredIdentityProjectChange,
            existing.sequence >= change.sequence
        else {
            deferredIdentityProjectChange = change
            return
        }
    }

    private func persistDeferredIdentityProjectChanges(
        projectID: String
    ) async throws {
        while let change = deferredIdentityProjectChange {
            deferredIdentityProjectChange = nil
            guard change.sequence > latestPersistedSnapshotSequence else {
                continue
            }

            currentProjectDocument = change.document
            do {
                try await persistProjectInOrder(
                    projectID: projectID,
                    document: change.document
                )
                latestPersistedSnapshotSequence = change.sequence
            } catch {
                deferIdentityProjectChange(change)
                throw error
            }
        }
    }

    func forgetProjectIdentities(_ projectIDs: Set<String>) {
        if let pendingProject, projectIDs.contains(pendingProject.id) {
            self.pendingProject = nil
        }
        guard let currentProjectID, projectIDs.contains(currentProjectID) else {
            return
        }

        self.currentProjectID = nil
        currentProjectDocument = nil
        bridgeLifecycle.markProjectUnapplied()
    }

    private func queueNavigation(
        to url: URL,
        trust: WebShellNavigationTrust
    ) {
        navigationQueue.enqueue(url, trust: trust)

        guard !isCapturingSnapshot else {
            return
        }

        if bridgeLifecycle.shouldCaptureBeforeNavigation(
            isReady: isReady,
            isLoadingProject: isLoadingProject,
            isCapturingSnapshot: isCapturingSnapshot
        ) {
            isCapturingSnapshot = true
            let generation = bridgeLifecycle.generation
            guard let capturedProjectID = currentProjectID else {
                isCapturingSnapshot = false
                navigationQueue.discard()
                lastErrorMessage = "Cannot reload before the selected workspace identity is available."
                return
            }
            captureCurrentWorkspaceSnapshot(for: generation) { [weak self] documentSnapshot in
                Task { @MainActor [weak self] in
                    guard let self else {
                        return
                    }

                    guard await self.finishSnapshotCapture(
                        documentSnapshot,
                        capturedProjectID: capturedProjectID,
                        generation: generation
                    ) else {
                        return
                    }
                    _ = self.performLatestQueuedNavigation()
                }
            }
            return
        }

        _ = performLatestQueuedNavigation()
    }

    @discardableResult
    private func performLatestQueuedNavigation() -> Bool {
        guard let request = navigationQueue.takeLatestRequest() else {
            return false
        }

        return beginNavigationNow(request)
    }

    @discardableResult
    private func beginNavigationNow(_ request: WebShellNavigationRequest) -> Bool {
        guard let trustedOrigin else {
            lastErrorMessage = "The embedded workspace origin is unavailable."
            return false
        }

        let disposition = WebShellOriginPolicy(
            trustedOrigin: trustedOrigin,
            authenticationOrigin: authenticationOrigin
        )
            .disposition(for: request.url)
        guard Self.navigationTrust(for: disposition) == request.trust else {
            lastErrorMessage = "Blocked an untrusted embedded navigation."
            return false
        }

        pendingProject = Self.projectToReapply(
            pendingProject: pendingProject,
            currentProjectID: currentProjectID,
            currentProjectDocument: currentProjectDocument
        )
        isReady = false
        isCapturingSnapshot = false
        isLoadingProject = false
        unlockWorkspaceAfterPendingProjectLoad = false
        latestPersistedSnapshotSequence = 0
        bridgeLifecycle.beginNavigation()
        navigationTrust = request.trust
        switch request.trust {
        case .trustedWorkspace:
            installBridgeForTrustedOrigin(trustedOrigin)
        case .localAuthenticationCallback, .externalAuthentication:
            removeBridgeFromWebContent()
        }
        admittedMainFrameRequest = request
        activeNavigation = nil
        activeNavigation = webView.load(URLRequest(url: request.url))
        return true
    }

    @discardableResult
    private func finishSnapshotCapture(
        _ result: WebShellSnapshotCaptureResult,
        capturedProjectID: String,
        generation: String,
        retainWorkspaceLockOnSuccess: Bool = false
    ) async -> Bool {
        guard bridgeLifecycle.accepts(generation) else {
            return false
        }
        guard navigationQueue.allowNavigation(after: result) else {
            releaseSnapshotCapture(for: generation)
            // A reload or backend navigation destroys the live JavaScript state.
            // If its final snapshot cannot be decoded, keep the current page and
            // require an explicit retry instead of silently discarding edits.
            return false
        }
        guard case let .captured(snapshot) = result else {
            releaseSnapshotCapture(for: generation)
            return false
        }
        guard currentProjectID == capturedProjectID else {
            navigationQueue.discard()
            lastErrorMessage = "The selected workspace changed while its final snapshot was being captured."
            releaseSnapshotCapture(for: generation)
            return false
        }

        currentProjectDocument = snapshot.document
        do {
            try await persistProjectInOrder(
                projectID: capturedProjectID,
                document: snapshot.document
            )
            latestPersistedSnapshotSequence = max(
                latestPersistedSnapshotSequence,
                snapshot.sequence
            )
        } catch {
            guard bridgeLifecycle.accepts(generation) else {
                return false
            }
            navigationQueue.discard()
            lastErrorMessage = error.localizedDescription
            releaseSnapshotCapture(for: generation)
            return false
        }

        guard
            bridgeLifecycle.accepts(generation),
            currentProjectID == capturedProjectID
        else {
            return false
        }
        pendingProject = Self.pendingProjectAfterCapture(
            pendingProject: pendingProject,
            capturedProjectID: capturedProjectID
        )
        if !retainWorkspaceLockOnSuccess {
            releaseSnapshotCapture(for: generation)
        }
        return true
    }

    private func releaseSnapshotCapture(for generation: String) {
        guard bridgeLifecycle.accepts(generation) else {
            return
        }
        isCapturingSnapshot = false
    }

    func addNode(ofType nodeType: String) {
        do {
            let argument = try javaScriptStringLiteral(for: nodeType)
            evaluateNativeShellScript("typeof window.addNodeFromMenu === 'function' && window.addNodeFromMenu(\(argument));")
        } catch {
            lastErrorMessage = error.localizedDescription
        }
    }

    func addQuickAddWorkflow(_ chainType: String) {
        do {
            let argument = try javaScriptStringLiteral(for: chainType)
            evaluateNativeShellScript("typeof window.addQuickAddChain === 'function' && window.addQuickAddChain(\(argument));")
        } catch {
            lastErrorMessage = error.localizedDescription
        }
    }

    func saveWorkspace() {
        evaluateNativeShellScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.saveWorkspace?.();")
    }

    func loadWorkspace() {
        evaluateNativeShellScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.loadWorkspace?.();")
    }

    func resetWorkspaceView() {
        evaluateNativeShellScript("typeof window.resetView === 'function' && window.resetView();")
    }

    func toggleDebugConsole() {
        evaluateNativeShellScript("typeof window.toggleDebugConsole === 'function' && window.toggleDebugConsole();")
    }

    func setCloudComputeEnabled(_ enabled: Bool) {
        evaluateNativeShellScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.setCloudComputeEnabled?.(\(enabled ? "true" : "false"));")
    }

    func setSurface(_ surface: String) {
        let normalized = (surface == "agent") ? "agent" : "workspace"
        evaluateNativeShellScript("typeof window.setNodeView === 'function' && window.setNodeView('\(normalized)');")
    }

    /// Point the embedded Design Agent at the locally-spawned design-chat backend
    /// (`DesignChatBackendController`). The web default is 127.0.0.1:8765; this
    /// keeps the two in sync if the port was overridden.
    func setDesignChatEndpoint(_ urlString: String, bearerToken: String) {
        do {
            let urlArgument = try javaScriptStringLiteral(for: urlString)
            let tokenArgument = try javaScriptStringLiteral(for: bearerToken)
            evaluateNativeShellScript(
                "typeof window.setDesignChatEndpoint === 'function' && " +
                "window.setDesignChatEndpoint(\(urlArgument), \(tokenArgument));"
            )
        } catch {
            lastErrorMessage = error.localizedDescription
        }
    }

    func setThemeMode(_ mode: String, effectiveThemeOverride: String? = nil, completion: (() -> Void)? = nil) {
        prepareInitialThemeMode(mode)
        do {
            let argument = try javaScriptStringLiteral(for: mode)
            let effectiveArgument: String
            if let effectiveThemeOverride {
                effectiveArgument = try javaScriptStringLiteral(for: effectiveThemeOverride)
            } else {
                effectiveArgument = "null"
            }
            evaluateNativeShellScript(
                "(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.setThemeMode?.(\(argument), \(effectiveArgument));",
                completeIfStale: true
            ) { _, _ in
                completion?()
            }
        } catch {
            lastErrorMessage = error.localizedDescription
            completion?()
        }
    }

    func runConnectedWorkspace() {
        evaluateNativeShellScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.runConnectedWorkspace?.();")
    }

    private func pushPendingProject() {
        guard isReady, !isCapturingSnapshot, !isLoadingProject, let pendingProject else {
            return
        }

        do {
            isLoadingProject = true
            let generation = bridgeLifecycle.generation
            let data = try JSONEncoder().encode(pendingProject.document)
            let jsonString = String(decoding: data, as: UTF8.self)
            let argument = try javaScriptStringLiteral(for: jsonString)
            let projectID = try javaScriptStringLiteral(for: pendingProject.id)
            let script = "(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.loadProjectFromJSONString(\(argument), \(projectID));"

            webView.evaluateJavaScript(script) { [weak self] result, error in
                guard let self else {
                    return
                }
                guard self.bridgeLifecycle.accepts(generation) else {
                    return
                }

                self.isLoadingProject = false
                if let error {
                    self.lastErrorMessage = error.localizedDescription
                    if Self.hasNewerPendingProject(
                        than: pendingProject,
                        pendingProject: self.pendingProject
                    ) {
                        self.pushPendingProject()
                    } else {
                        self.releaseWorkspaceLockAfterPendingProjectLoadIfNeeded()
                    }
                    return
                }

                guard result as? Bool == true else {
                    self.lastErrorMessage = "Failed to load the selected workspace."
                    if Self.hasNewerPendingProject(
                        than: pendingProject,
                        pendingProject: self.pendingProject
                    ) {
                        self.pushPendingProject()
                    } else {
                        self.releaseWorkspaceLockAfterPendingProjectLoadIfNeeded()
                    }
                    return
                }

                guard self.bridgeLifecycle.markProjectApplied(for: generation) else {
                    return
                }
                self.currentProjectID = pendingProject.id
                self.currentProjectDocument = pendingProject.document
                if self.pendingProject == pendingProject {
                    self.pendingProject = nil
                }

                if self.pendingProject != nil {
                    self.pushPendingProject()
                } else {
                    self.releaseWorkspaceLockAfterPendingProjectLoadIfNeeded()
                }
            }
        } catch {
            isLoadingProject = false
            lastErrorMessage = error.localizedDescription
            releaseWorkspaceLockAfterPendingProjectLoadIfNeeded()
        }
    }

    private func releaseWorkspaceLockAfterPendingProjectLoadIfNeeded() {
        guard unlockWorkspaceAfterPendingProjectLoad, !isLoadingProject else {
            return
        }
        unlockWorkspaceAfterPendingProjectLoad = false
        let script = "(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.setFileOperationLocked?.(false);"
        // Submit the unlock while still handling the project-load completion,
        // so no native event can begin another handoff before WebKit receives it.
        evaluateNativeShellScript(script) { [weak self] result, error in
            guard let self, error == nil else {
                return
            }
            if result as? Bool != true {
                self.lastErrorMessage = "The workspace project changed, but interaction could not be restored."
            }
        }
    }

    private func handleMessage(_ body: [String: Any]) {
        guard
            let generation = body["generation"] as? String,
            bridgeLifecycle.accepts(generation),
            let type = body["type"] as? String
        else {
            return
        }

        switch type {
        case "ready":
            isReady = true
            pushPendingProject()

        case "contractError":
            if let payload = body["payload"] as? String {
                lastErrorMessage = payload
            }

        case "projectChanged":
            guard
                bridgeLifecycle.currentProjectIsApplied,
                !isLoadingProject,
                let payload = body["payload"] as? [String: Any],
                let projectID = payload["projectID"] as? String,
                !projectID.isEmpty,
                let sequence = Self.snapshotSequence(from: payload),
                let jsonString = payload["jsonString"] as? String,
                let document = decodeDocument(fromJSONString: jsonString)
            else {
                return
            }

            if let sourceProjectID = identityChangeSourceProjectID {
                let admittedProjectIDs = Set([
                    sourceProjectID,
                    identityChangeTargetProjectID,
                ].compactMap { $0 })
                guard admittedProjectIDs.contains(projectID) else {
                    return
                }
                deferIdentityProjectChange(WebShellSequencedProjectChange(
                    sequence: sequence,
                    document: document
                ))
                return
            }

            guard
                !isCapturingSnapshot,
                projectID == currentProjectID,
                sequence > latestPersistedSnapshotSequence
            else {
                return
            }

            latestPersistedSnapshotSequence = sequence
            currentProjectDocument = document
            enqueueAutosave(
                projectID: projectID,
                document: document,
                generation: generation
            )

        case "requestCurrentProject":
            if
                bridgeLifecycle.currentProjectIsApplied,
                pendingProject == nil,
                let currentProjectID,
                let currentProjectDocument
            {
                pendingProject = PendingProject(id: currentProjectID, document: currentProjectDocument)
            }
            pushPendingProject()

        case "copyText":
            if let payload = body["payload"] as? String {
                let pasteboard = NSPasteboard.general
                pasteboard.clearContents()
                pasteboard.setString(payload, forType: .string)
            }

        case "log":
            if let payload = body["payload"] as? String {
                lastErrorMessage = payload
            }

        default:
            break
        }
    }

    private func enqueueAutosave(
        projectID: String,
        document: WorkspaceDocument,
        generation: String
    ) {
        guard let onProjectChange else {
            if bridgeLifecycle.accepts(generation), currentProjectID == projectID {
                lastErrorMessage = WebShellPersistenceError.unavailable.localizedDescription
            }
            return
        }

        let work = persistenceQueue.enqueue {
            try await onProjectChange(projectID, document)
        }
        Task { @MainActor [weak self] in
            guard case let .failure(error) = await work.value else {
                return
            }
            guard
                let self,
                self.bridgeLifecycle.accepts(generation),
                self.currentProjectID == projectID
            else {
                return
            }
            self.lastErrorMessage = error.localizedDescription
        }
    }

    private func persistProjectInOrder(
        projectID: String,
        document: WorkspaceDocument
    ) async throws {
        guard let onProjectChange else {
            throw WebShellPersistenceError.unavailable
        }

        let work = persistenceQueue.enqueue {
            try await onProjectChange(projectID, document)
        }
        try await work.value.get()
    }

    private func decodeDocument(fromJSONString jsonString: String) -> WorkspaceDocument? {
        do {
            let data = Data(jsonString.utf8)
            return try JSONDecoder().decode(WorkspaceDocument.self, from: data)
        } catch {
            lastErrorMessage = error.localizedDescription
            return nil
        }
    }

    private func javaScriptStringLiteral(for string: String) throws -> String {
        let wrapper = [string]
        let data = try JSONSerialization.data(withJSONObject: wrapper, options: [])
        let encoded = String(decoding: data, as: UTF8.self)
        return String(encoded.dropFirst().dropLast())
    }

    private func captureCurrentWorkspaceSnapshot(
        for generation: String,
        completion: @escaping (WebShellSnapshotCaptureResult) -> Void
    ) {
        guard
            isReady,
            bridgeLifecycle.accepts(generation),
            bridgeLifecycle.currentProjectIsApplied
        else {
            lastErrorMessage = "The current workspace is not ready to be captured."
            completion(.failed)
            return
        }

        guard pendingSnapshotCapture == nil else {
            lastErrorMessage = "A workspace snapshot is already being captured."
            completion(.failed)
            return
        }

        let capture = WebShellSnapshotCaptureCompletion(
            generation: generation,
            completion: completion
        )
        pendingSnapshotCapture = capture
        snapshotCaptureTimeoutTask?.cancel()
        snapshotCaptureTimeoutTask = Task { @MainActor [weak self, weak capture] in
            do {
                try await Task.sleep(nanoseconds: Self.snapshotCaptureTimeoutNanoseconds)
            } catch {
                return
            }
            guard let self, let capture else {
                return
            }
            self.resolveSnapshotCapture(
                capture,
                result: .failed,
                errorMessage: "The embedded workspace did not return a snapshot in time."
            )
        }

        webView.evaluateJavaScript("(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.captureProjectSnapshot?.();") { [weak self] result, error in
            guard let self else {
                capture.resolve(.failed)
                return
            }
            guard self.pendingSnapshotCapture === capture else {
                return
            }
            guard self.bridgeLifecycle.accepts(generation) else {
                self.resolveSnapshotCapture(capture, result: .failed)
                return
            }
            if let error {
                self.resolveSnapshotCapture(
                    capture,
                    result: .failed,
                    errorMessage: error.localizedDescription
                )
                return
            }

            let captured: WebShellSequencedProjectChange
            do {
                captured = try self.finalizedProjectSnapshot(
                    from: result as? String,
                    failureMessage: "The current workspace returned an empty or invalid snapshot."
                )
            } catch {
                self.resolveSnapshotCapture(
                    capture,
                    result: .failed,
                    errorMessage: error.localizedDescription
                )
                return
            }
            self.resolveSnapshotCapture(
                capture,
                result: .captured(WebShellCapturedProjectSnapshot(
                    document: captured.document,
                    sequence: captured.sequence
                ))
            )
        }
    }

    private func resolveSnapshotCapture(
        _ capture: WebShellSnapshotCaptureCompletion,
        result: WebShellSnapshotCaptureResult,
        errorMessage: String? = nil
    ) {
        guard pendingSnapshotCapture === capture else {
            return
        }

        pendingSnapshotCapture = nil
        snapshotCaptureTimeoutTask?.cancel()
        snapshotCaptureTimeoutTask = nil
        if let errorMessage {
            lastErrorMessage = errorMessage
        }
        capture.resolve(result)
    }

    private func evaluateNativeShellScript(
        _ script: String,
        completeIfStale: Bool = false,
        completion: ((Any?, Error?) -> Void)? = nil
    ) {
        guard isReady else {
            completion?(nil, nil)
            return
        }

        let generation = bridgeLifecycle.generation
        webView.evaluateJavaScript(script) { [weak self] result, error in
            guard let self else {
                return
            }
            guard self.bridgeLifecycle.accepts(generation) else {
                if completeIfStale {
                    completion?(nil, nil)
                }
                return
            }
            if let error {
                self.lastErrorMessage = error.localizedDescription
            }
            completion?(result, error)
        }
    }

    private func evaluateNativeShellBoolean(_ script: String) async throws -> Bool {
        try await withCheckedThrowingContinuation { continuation in
            evaluateNativeShellScript(script, completeIfStale: true) { result, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: result as? Bool == true)
                }
            }
        }
    }

    private func evaluateNativeShellString(_ script: String) async throws -> String? {
        try await withCheckedThrowingContinuation { continuation in
            evaluateNativeShellScript(script, completeIfStale: true) { result, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: result as? String)
                }
            }
        }
    }

    private func finalizedProjectSnapshot(
        from result: String?,
        failureMessage: String
    ) throws -> WebShellSequencedProjectChange {
        guard
            let result,
            let payloadData = result.data(using: .utf8),
            let payload = try? JSONDecoder().decode(
                WebShellFinalizedProjectSnapshot.self,
                from: payloadData
            ),
            payload.sequence > 0,
            let document = decodeDocument(fromJSONString: payload.jsonString)
        else {
            throw WebShellFileOperationError.captureFailed(failureMessage)
        }
        return WebShellSequencedProjectChange(
            sequence: payload.sequence,
            document: document
        )
    }

    private func captureFinalProjectIdentityInJavaScript(
        projectID: String
    ) async throws -> WebShellSequencedProjectChange {
        let projectID = try javaScriptStringLiteral(for: projectID)
        let script = "(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.rebindProjectIDAndCapture?.(\(projectID));"
        return try finalizedProjectSnapshot(
            from: try await evaluateNativeShellString(script),
            failureMessage: "Failed to serialize and rebind the renamed workspace before completing the file operation."
        )
    }

    private func setWorkspaceInteractionLocked(_ locked: Bool) async throws {
        let script = "(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.setFileOperationLocked?.(\(locked ? "true" : "false"));"
        guard try await evaluateNativeShellBoolean(script) else {
            throw WebShellFileOperationError.captureFailed(
                locked
                    ? "The workspace could not be paused safely for the file operation."
                    : "The workspace file operation completed, but interaction could not be restored."
            )
        }
    }

    private func installBridgeForTrustedOrigin(_ origin: WebShellOrigin) {
        contentController.removeAllUserScripts()
        let bridgeScript = WKUserScript(
            source: Self.bridgeScriptSource(
                initialThemeMode: injectedThemeMode,
                generation: bridgeLifecycle.generation,
                trustedOrigin: origin.serialized
            ),
            injectionTime: .atDocumentStart,
            forMainFrameOnly: true
        )
        contentController.addUserScript(bridgeScript)
        if !bridgeMessageHandlerInstalled {
            contentController.add(self, name: Self.bridgeName)
            bridgeMessageHandlerInstalled = true
        }
    }

    private func removeBridgeFromWebContent() {
        contentController.removeAllUserScripts()
        if bridgeMessageHandlerInstalled {
            contentController.removeScriptMessageHandler(forName: Self.bridgeName)
            bridgeMessageHandlerInstalled = false
        }
    }

    private func isTrustedMainFrame(_ frame: WKFrameInfo) -> Bool {
        guard
            navigationTrust == .trustedWorkspace,
            frame.isMainFrame,
            frame.webView === webView,
            let trustedOrigin,
            let frameURL = frame.request.url,
            trustedOrigin.contains(frameURL),
            frameURL.path == WebShellOriginPolicy.workspacePath
        else {
            return false
        }

        let securityOrigin = frame.securityOrigin
        return trustedOrigin.matches(
            protocol: securityOrigin.protocol,
            host: securityOrigin.host,
            port: securityOrigin.port
        )
    }
}

extension WebShellController: WKScriptMessageHandler {
    nonisolated func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        Task { @MainActor [weak self] in
            guard
                let self,
                message.name == Self.bridgeName,
                message.webView === self.webView,
                self.isTrustedMainFrame(message.frameInfo),
                let body = message.body as? [String: Any]
            else {
                return
            }

            self.handleMessage(body)
        }
    }
}

extension WebShellController: WKNavigationDelegate {
    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationAction: WKNavigationAction,
        decisionHandler: @escaping @MainActor @Sendable (WKNavigationActionPolicy) -> Void
    ) {
        guard
            webView === self.webView,
            let url = navigationAction.request.url,
            let trustedOrigin
        else {
            decisionHandler(.cancel)
            return
        }

        let policy = WebShellOriginPolicy(
            trustedOrigin: trustedOrigin,
            authenticationOrigin: authenticationOrigin
        )
        let disposition = policy.disposition(for: url)

        guard let targetFrame = navigationAction.targetFrame else {
            switch disposition {
            case .externalAuthentication:
                NSWorkspace.shared.open(url)

            case .localAuthenticationCallback
                where navigationTrust == .externalAuthentication:
                queueNavigation(to: url, trust: .localAuthenticationCallback)

            case .trustedWorkspace
                where navigationTrust == .externalAuthentication
                    || navigationTrust == .localAuthenticationCallback:
                if let currentURL {
                    queueNavigation(to: currentURL, trust: .trustedWorkspace)
                }

            case .trustedWorkspace:
                NSWorkspace.shared.open(url)

            case .blocked where trustedOrigin.contains(url)
                && navigationTrust == .trustedWorkspace:
                NSWorkspace.shared.open(url)

            case .blocked where Self.isExternalHTTPSURL(url):
                NSWorkspace.shared.open(url)

            default:
                break
            }
            decisionHandler(.cancel)
            return
        }

        guard targetFrame.isMainFrame else {
            decisionHandler(Self.allowsSubframeNavigation(
                disposition: disposition,
                navigationTrust: navigationTrust
            ) ? .allow : .cancel)
            return
        }

        let requestTrust = Self.navigationTrust(for: disposition)

        if let admittedMainFrameRequest {
            self.admittedMainFrameRequest = nil
            if
                admittedMainFrameRequest.url == url,
                admittedMainFrameRequest.trust == requestTrust
            {
                decisionHandler(.allow)
                return
            }
        }

        switch disposition {
        case .trustedWorkspace:
            decisionHandler(.cancel)
            if
                navigationTrust == .externalAuthentication
                    || navigationTrust == .localAuthenticationCallback,
                let currentURL
            {
                queueNavigation(to: currentURL, trust: .trustedWorkspace)
            } else {
                queueNavigation(to: url, trust: .trustedWorkspace)
            }

        case .localAuthenticationCallback:
            decisionHandler(.cancel)
            guard navigationTrust == .externalAuthentication else {
                lastErrorMessage = "Blocked an authentication callback outside an active OAuth navigation."
                return
            }
            queueNavigation(to: url, trust: .localAuthenticationCallback)

        case .externalAuthentication:
            if navigationTrust == .externalAuthentication {
                prepareForAllowedExternalMainFrameNavigation()
                decisionHandler(.allow)
            } else {
                decisionHandler(.cancel)
                queueNavigation(to: url, trust: .externalAuthentication)
            }

        case .blocked:
            decisionHandler(.cancel)
            if
                trustedOrigin.contains(url),
                navigationTrust == .externalAuthentication
                    || navigationTrust == .localAuthenticationCallback,
                let currentURL
            {
                // Cognito logout commonly returns to `/`. Never embed that or
                // another same-origin utility page; restore the canonical shell.
                queueNavigation(to: currentURL, trust: .trustedWorkspace)
            } else {
                if Self.isExternalHTTPSURL(url) {
                    NSWorkspace.shared.open(url)
                } else {
                    lastErrorMessage = "Blocked a navigation outside the canonical workspace and OAuth paths."
                }
            }
        }
    }

    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
        activeNavigation = navigation
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        guard isActiveNavigation(navigation) else {
            return
        }

        admittedMainFrameRequest = nil
        lastErrorMessage = nil
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        guard isActiveNavigation(navigation) else {
            return
        }

        admittedMainFrameRequest = nil
        lastErrorMessage = error.localizedDescription
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        guard isActiveNavigation(navigation) else {
            return
        }

        admittedMainFrameRequest = nil
        lastErrorMessage = error.localizedDescription
    }

    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) {
        guard webView === self.webView else {
            return
        }

        let recoveryMessage = "The embedded web workspace stopped unexpectedly and was restarted from its latest native snapshot."
        if let pendingSnapshotCapture {
            resolveSnapshotCapture(
                pendingSnapshotCapture,
                result: .failed,
                errorMessage: recoveryMessage
            )
        } else {
            snapshotCaptureTimeoutTask?.cancel()
            snapshotCaptureTimeoutTask = nil
        }

        isCapturingSnapshot = false
        navigationQueue.discard()
        admittedMainFrameRequest = nil

        guard let recoveryRequest = Self.webContentRecoveryRequest(for: currentURL) else {
            isReady = false
            isLoadingProject = false
            bridgeLifecycle.beginNavigation()
            navigationTrust = nil
            removeBridgeFromWebContent()
            lastErrorMessage = recoveryMessage
            return
        }

        beginNavigationNow(recoveryRequest)
        lastErrorMessage = recoveryMessage
    }

    private func prepareForAllowedExternalMainFrameNavigation() {
        pendingProject = Self.projectToReapply(
            pendingProject: pendingProject,
            currentProjectID: currentProjectID,
            currentProjectDocument: currentProjectDocument
        )
        isReady = false
        isLoadingProject = false
        bridgeLifecycle.beginNavigation()
        navigationTrust = .externalAuthentication
        removeBridgeFromWebContent()
    }

    private func isActiveNavigation(_ navigation: WKNavigation?) -> Bool {
        guard let navigation, let activeNavigation else {
            return false
        }

        return navigation === activeNavigation
    }
}

extension WebShellController: WKUIDelegate {
    // WKWebView does not show a file picker for <input type="file"> by itself
    // on macOS — the host app must implement this delegate method and bridge
    // through to NSOpenPanel. Without this, clicks on the AI Import file
    // chooser are silently swallowed.
    func webView(
        _ webView: WKWebView,
        runOpenPanelWith parameters: WKOpenPanelParameters,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping @MainActor @Sendable ([URL]?) -> Void
    ) {
        guard webView === self.webView, isTrustedMainFrame(frame) else {
            completionHandler(nil)
            return
        }

        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = parameters.allowsDirectories
        panel.allowsMultipleSelection = parameters.allowsMultipleSelection
        panel.resolvesAliases = true

        let handle: @MainActor @Sendable (NSApplication.ModalResponse) -> Void = { response in
            completionHandler(response == .OK ? panel.urls : nil)
        }

        if let host = webView.window {
            panel.beginSheetModal(for: host, completionHandler: handle)
        } else {
            panel.begin(completionHandler: handle)
        }
    }
}

extension WebShellController {
    static let bridgeName = "biocircuitsExplorerShell"
    static let supportedContractVersion = 1
    static let supportedWorkspaceVersion = 1
    static let snapshotCaptureTimeoutNanoseconds: UInt64 = 3_000_000_000

    static func admittedProjectID(
        from payload: [String: Any],
        currentProjectID: String?
    ) -> String? {
        guard
            let projectID = payload["projectID"] as? String,
            !projectID.isEmpty,
            projectID == currentProjectID
        else {
            return nil
        }
        return projectID
    }

    static func snapshotSequence(from payload: [String: Any]) -> UInt64? {
        if let sequence = payload["sequence"] as? UInt64, sequence > 0 {
            return sequence
        }
        if let sequence = payload["sequence"] as? Int, sequence > 0 {
            return UInt64(sequence)
        }
        if let number = payload["sequence"] as? NSNumber {
            let value = number.doubleValue
            guard
                value.isFinite,
                value > 0,
                value.rounded(.towardZero) == value,
                value <= 9_007_199_254_740_991
            else {
                return nil
            }
            return UInt64(value)
        }
        return nil
    }

    static func webContentRecoveryRequest(for currentURL: URL?) -> WebShellNavigationRequest? {
        guard let currentURL else {
            return nil
        }
        return WebShellNavigationRequest(url: currentURL, trust: .trustedWorkspace)
    }

    static func navigationTrust(
        for disposition: WebShellNavigationDisposition
    ) -> WebShellNavigationTrust? {
        switch disposition {
        case .trustedWorkspace:
            return .trustedWorkspace
        case .localAuthenticationCallback:
            return .localAuthenticationCallback
        case .externalAuthentication:
            return .externalAuthentication
        case .blocked:
            return nil
        }
    }

    static func allowsSubframeNavigation(
        disposition: WebShellNavigationDisposition,
        navigationTrust: WebShellNavigationTrust?
    ) -> Bool {
        switch navigationTrust {
        case .trustedWorkspace:
            return disposition == .trustedWorkspace
        case .localAuthenticationCallback:
            return false
        case .externalAuthentication:
            return disposition == .externalAuthentication
        case nil:
            return false
        }
    }

    static func trustedReturnURL(
        navigationURL: URL,
        canonicalWorkspaceURL: URL?
    ) -> URL {
        if navigationURL.path == WebShellOriginPolicy.authenticationCallbackPath {
            return navigationURL
        }
        return canonicalWorkspaceURL ?? navigationURL
    }

    static func normalizedThemeMode(_ mode: String) -> String {
        switch mode {
        case "light", "dark":
            return mode
        default:
            return "auto"
        }
    }

    static func bridgeScriptSource(
        initialThemeMode: String,
        generation: String,
        trustedOrigin: String
    ) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .withoutEscapingSlashes
        let trustedOriginLiteral = String(
            decoding: (try? encoder.encode(trustedOrigin)) ?? Data("\"\"".utf8),
            as: UTF8.self
        )
        return #"""
    (() => {
      const nativeThemeMode = "\#(normalizedThemeMode(initialThemeMode))";
      const bridgeGeneration = "\#(generation)";
      const trustedOrigin = \#(trustedOriginLiteral);
      const trustedPath = "\#(WebShellOriginPolicy.workspacePath)";
      if (window.location.origin !== trustedOrigin) return;
      if (window.location.pathname !== trustedPath) return;
      const shellState = {
        ready: false,
        registered: false,
        suppressSync: false,
        lastSnapshot: '',
        snapshotSequence: 0,
        projectID: null,
        reportedError: null,
      };

      function resolveInitialEffectiveTheme() {
        if (nativeThemeMode === 'light' || nativeThemeMode === 'dark') {
          return nativeThemeMode;
        }
        return window.matchMedia?.('(prefers-color-scheme: light)')?.matches ? 'light' : 'dark';
      }

      function nativeShellStyleText() {
        return `
          #header {
            display: none !important;
          }
          #editor {
            top: 0 !important;
            height: 100vh !important;
          }
          #debug-console {
            top: 0 !important;
            height: 100vh !important;
          }
          #agent-view {
            top: 0 !important;
            height: 100vh !important;
          }
          html.biocircuits-native-shell[data-effective-theme="light"],
          html.biocircuits-native-shell[data-effective-theme="light"] body {
            background: #eef3f8 !important;
          }
          html.biocircuits-native-shell[data-effective-theme="dark"],
          html.biocircuits-native-shell[data-effective-theme="dark"] body {
            background: #1c1c1c !important;
          }
          html.biocircuits-file-operation-locked,
          html.biocircuits-file-operation-locked body {
            cursor: wait !important;
            pointer-events: none !important;
            user-select: none !important;
          }
        `;
      }

      function ensureNativeShellStyle() {
        let style = document.getElementById('biocircuits-native-shell-style');
        if (!style) {
          style = document.createElement('style');
          style.id = 'biocircuits-native-shell-style';
          (document.head || document.documentElement).appendChild(style);
        }
        if (style.textContent !== nativeShellStyleText()) {
          style.textContent = nativeShellStyleText();
        }
      }

      function applyInitialThemeHint() {
        const effectiveTheme = resolveInitialEffectiveTheme();
        const root = document.documentElement;
        if (!root) return;

        root.dataset.themeMode = nativeThemeMode;
        root.dataset.effectiveTheme = effectiveTheme;
        root.style.colorScheme = effectiveTheme;
        ensureNativeShellStyle();

        root.classList.add('biocircuits-native-shell');
        if (document.body) {
          document.body.classList.add('biocircuits-native-shell');
        } else {
          document.addEventListener('DOMContentLoaded', () => {
            document.body?.classList.add('biocircuits-native-shell');
          }, { once: true });
        }
      }

      applyInitialThemeHint();

      function postToNative(type, payload) {
        const handler = window.webkit?.messageHandlers?.biocircuitsExplorerShell;
        if (!handler) return;
        handler.postMessage({ generation: bridgeGeneration, type, payload });
      }

      function reportError(message) {
        if (!message || shellState.reportedError === message) return;
        shellState.reportedError = message;
        postToNative('contractError', message);
      }

      function currentContract() {
        return window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell;
      }

      function contractMetadata(contract, payload = null) {
        return {
          contractVersion: Number(payload?.contractVersion ?? contract?.contractVersion ?? 0),
          workspaceVersion: Number(payload?.workspaceVersion ?? payload?.schemaVersion ?? contract?.workspaceVersion ?? contract?.schemaVersion ?? 0),
        };
      }

      function validateMetadata(contract, payload = null) {
        const metadata = contractMetadata(contract, payload);
        if (metadata.contractVersion !== \#(supportedContractVersion)) {
          reportError(`Unsupported workspace shell contract version: ${metadata.contractVersion}`);
          return null;
        }
        if (metadata.workspaceVersion > \#(supportedWorkspaceVersion)) {
          reportError(`Workspace version ${metadata.workspaceVersion} is newer than this native shell supports.`);
          return null;
        }
        return metadata;
      }

      function postSnapshot(jsonString, force = false) {
        if (shellState.suppressSync) return;
        if (!jsonString) return;
        if (!force && jsonString === shellState.lastSnapshot) return;
        shellState.lastSnapshot = jsonString;
        shellState.snapshotSequence += 1;
        const payload = {
          projectID: shellState.projectID,
          jsonString,
          sequence: shellState.snapshotSequence,
        };
        postToNative('projectChanged', payload);
        return JSON.stringify(payload);
      }

      function captureSnapshotWithoutPosting() {
        const contract = currentContract();
        if (typeof contract?.serializeWorkspace !== 'function') return null;
        const jsonString = contract.serializeWorkspace();
        if (typeof jsonString !== 'string' || jsonString.length === 0) return null;
        shellState.lastSnapshot = jsonString;
        shellState.snapshotSequence += 1;
        return JSON.stringify({
          projectID: shellState.projectID,
          jsonString,
          sequence: shellState.snapshotSequence,
        });
      }

      function applyFileOperationLock(locked) {
        const shouldLock = Boolean(locked);
        if (shouldLock) document.activeElement?.blur?.();
        document.documentElement.inert = shouldLock;
        document.documentElement.classList.toggle('biocircuits-file-operation-locked', shouldLock);
        return document.documentElement.inert === shouldLock;
      }

      const nativeShell = {
        loadProjectFromJSONString(jsonString, projectID) {
          const contract = currentContract();
          if (typeof contract?.applyWorkspaceFromJSONString !== 'function') return false;
          if (typeof projectID !== 'string' || projectID.length === 0) return false;
          shellState.suppressSync = true;
          try {
            let applied;
            try {
              applied = contract.applyWorkspaceFromJSONString(jsonString);
            } catch (error) {
              reportError(error?.message ?? String(error));
              return false;
            }
            if (applied === false) return false;
            shellState.projectID = projectID;
            shellState.lastSnapshot = jsonString;
            try {
              shellState.lastSnapshot = contract.serializeWorkspace?.() || jsonString;
            } catch (error) {
              try {
                reportError(`Workspace applied, but snapshot serialization failed: ${error?.message ?? String(error)}`);
              } catch (_) {
                // The workspace identity is already committed; diagnostics must not undo it.
              }
            }
          } finally {
            shellState.suppressSync = false;
          }
          return true;
        },
        rebindProjectID(projectID) {
          if (typeof projectID !== 'string' || projectID.length === 0) return false;
          shellState.projectID = projectID;
          return true;
        },
        captureProjectSnapshot() {
          try {
            return captureSnapshotWithoutPosting();
          } catch (error) {
            reportError(`Workspace snapshot serialization failed: ${error?.message ?? String(error)}`);
            return null;
          }
        },
        rebindProjectIDAndCapture(projectID) {
          if (typeof projectID !== 'string' || projectID.length === 0) return null;
          shellState.projectID = projectID;
          try {
            const contract = currentContract();
            if (typeof contract?.serializeWorkspace !== 'function') return null;
            const jsonString = contract.serializeWorkspace();
            if (typeof jsonString !== 'string' || jsonString.length === 0) return null;
            return postSnapshot(jsonString, true);
          } catch (error) {
            reportError(`Workspace final serialization failed: ${error?.message ?? String(error)}`);
            return null;
          }
        },
        setFileOperationLocked(locked) {
          return applyFileOperationLock(locked);
        },
        copyText(text) {
          postToNative('copyText', String(text ?? ''));
          return true;
        },
      };
      window.BiocircuitsExplorerNativeShell = nativeShell;
      window.ROPNativeShell = nativeShell;

      function registerHost(contract) {
        if (shellState.registered) return;

        const host = {
          shellDidBecomeReady(payload) {
            const metadata = validateMetadata(contract, payload);
            if (!metadata) return;
            shellState.ready = true;
            shellState.lastSnapshot = contract.serializeWorkspace?.() || shellState.lastSnapshot;
            updateHeader();
            postToNative('ready', metadata);
          },
          workspaceDidChange(jsonString) {
            postSnapshot(jsonString, false);
          },
          requestCurrentWorkspace() {
            postToNative('requestCurrentProject', null);
            if (typeof window.showToast === 'function') {
              window.showToast('Reloaded from the selected JSON project');
            }
          },
          saveWorkspaceJSONString(jsonString) {
            postSnapshot(jsonString, true);
            if (typeof window.showToast === 'function') {
              window.showToast('Saved to the current JSON project');
            }
          },
          log(message) {
            if (typeof message === 'string' && message.length > 0) {
              postToNative('log', message);
            }
          },
        };

        contract.registerHost(host);
        shellState.registered = true;
        shellState.lastSnapshot = contract.serializeWorkspace?.() || shellState.lastSnapshot;

        if (!shellState.ready) {
          const metadata = validateMetadata(contract);
          if (metadata) {
            shellState.ready = true;
            updateHeader();
            postToNative('ready', metadata);
          }
        }
      }

      function updateHeader() {
        installNativeShellChrome();
        const title = document.querySelector('#header h1');
        if (title) {
          title.textContent = '';
          title.setAttribute('aria-hidden', 'true');
        }
      }

      function installNativeShellChrome() {
        applyInitialThemeHint();
        document.documentElement.classList.add('biocircuits-native-shell');
        document.body?.classList.add('biocircuits-native-shell');
      }

      function boot() {
        const contract = currentContract();
        if (
          typeof contract?.registerHost !== 'function' ||
          typeof contract?.serializeWorkspace !== 'function' ||
          typeof contract?.applyWorkspaceFromJSONString !== 'function'
        ) {
          window.setTimeout(boot, 100);
          return;
        }

        registerHost(contract);
      }

      window.addEventListener('biocircuits-explorer:workspace-shell-ready', boot);
      window.addEventListener('rop:workspace-shell-ready', boot);
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', boot, { once: true });
      } else {
        boot();
      }
    })();
    """#
    }

    struct PendingProject: Equatable {
        let id: String
        let document: WorkspaceDocument
    }

    static func projectToReapply(
        pendingProject: PendingProject?,
        currentProjectID: String?,
        currentProjectDocument: WorkspaceDocument?
    ) -> PendingProject? {
        if let pendingProject {
            return pendingProject
        }

        guard let currentProjectID, let currentProjectDocument else {
            return nil
        }

        return PendingProject(id: currentProjectID, document: currentProjectDocument)
    }

    static func hasNewerPendingProject(
        than requestedProject: PendingProject,
        pendingProject: PendingProject?
    ) -> Bool {
        guard let pendingProject else {
            return false
        }

        return pendingProject != requestedProject
    }

    static func pendingProjectAfterCapture(
        pendingProject: PendingProject?,
        capturedProjectID: String
    ) -> PendingProject? {
        guard pendingProject?.id == capturedProjectID else {
            return pendingProject
        }

        // A -> B -> A while A's snapshot is in flight means "stay on A".
        // The captured document is newer than the ProjectStore copy that came
        // back through the selection, so do not apply that stale copy over it.
        return nil
    }

    static func pendingProjectAfterIdentityChange(
        pendingProject: PendingProject?,
        sourceProjectID: String,
        targetProjectID: String?
    ) -> PendingProject? {
        guard let pendingProject else {
            return nil
        }
        let staleProjectIDs = Set([sourceProjectID, targetProjectID].compactMap { $0 })
        guard staleProjectIDs.contains(pendingProject.id) else {
            return pendingProject
        }

        // Store publication during rename can reflect the moved file back
        // through SwiftUI while the WebView is still locked. That pending copy
        // predates the forced final snapshot and must not be reapplied.
        return nil
    }
}
