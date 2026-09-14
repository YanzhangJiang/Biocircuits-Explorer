import Combine
import Darwin
import Foundation

enum LocalLoopbackService {
    nonisolated static func makeNonce() -> String {
        var generator = SystemRandomNumberGenerator()
        return (0..<32)
            .map { _ in String(format: "%02x", UInt8.random(in: .min ... .max, using: &generator)) }
            .joined()
    }

    nonisolated static func configuredPort(
        keys: [String],
        environment: [String: String],
        excluding: Set<Int> = [],
        fallback: Int
    ) -> Int {
        for key in keys {
            guard
                let rawPort = environment[key]?.trimmingCharacters(in: .whitespacesAndNewlines),
                let port = Int(rawPort),
                (1...65_535).contains(port),
                !excluding.contains(port)
            else {
                continue
            }
            return port
        }

        return ephemeralPort(excluding: excluding) ?? fallback
    }

    /// Ask the kernel for an unused loopback port. The socket is intentionally
    /// released before the child process launches, so the per-launch nonce is
    /// still the authority if another process wins that small race.
    nonisolated static func ephemeralPort(excluding: Set<Int> = []) -> Int? {
        for _ in 0..<16 {
            let descriptor = socket(AF_INET, SOCK_STREAM, 0)
            guard descriptor >= 0 else {
                return nil
            }
            defer { Darwin.close(descriptor) }

            var address = sockaddr_in()
            address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            address.sin_family = sa_family_t(AF_INET)
            address.sin_port = in_port_t(0)
            address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

            let bindResult = withUnsafeMutablePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.bind(
                        descriptor,
                        $0,
                        socklen_t(MemoryLayout<sockaddr_in>.size)
                    )
                }
            }
            guard bindResult == 0 else {
                continue
            }

            var addressLength = socklen_t(MemoryLayout<sockaddr_in>.size)
            let nameResult = withUnsafeMutablePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.getsockname(descriptor, $0, &addressLength)
                }
            }
            guard nameResult == 0 else {
                continue
            }

            let port = Int(UInt16(bigEndian: address.sin_port))
            if port > 0, !excluding.contains(port) {
                return port
            }
        }
        return nil
    }
}

enum LocalProcessShutdown {
    nonisolated static func waitForExit(
        _ process: Process,
        timeout: TimeInterval
    ) async throws -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while process.isRunning, Date() < deadline {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(50))
        }
        return !process.isRunning
    }

    nonisolated static func waitForExitOrKill(
        _ process: Process,
        gracefulTimeout: TimeInterval = 3,
        forcedTimeout: TimeInterval = 1
    ) async throws -> Bool {
        guard process.isRunning else {
            return true
        }
        if try await waitForExit(process, timeout: gracefulTimeout) {
            return true
        }

        if process.isRunning {
            _ = Darwin.kill(process.processIdentifier, SIGKILL)
        }
        return try await waitForExit(process, timeout: forcedTimeout)
    }
}

struct BackendLaunchLifecycle {
    private(set) var generation: UInt64 = 0

    @discardableResult
    mutating func advance() -> UInt64 {
        generation &+= 1
        return generation
    }

    func accepts(_ generation: UInt64) -> Bool {
        generation == self.generation
    }

    func requireCurrent(_ generation: UInt64) throws {
        guard accepts(generation) else {
            throw CancellationError()
        }
    }
}

@MainActor
final class BiocircuitsBackendController: ObservableObject {
    private struct LaunchSpec {
        let executableURL: URL
        let arguments: [String]
        let currentDirectoryURL: URL
        let environment: [String: String]
        let startupTimeout: TimeInterval
        let startupStatus: String
    }

    @Published private(set) var isReady = false
    @Published private(set) var isStarting = false
    @Published private(set) var statusMessage = "Backend not started"
    @Published var lastErrorMessage: String?

    let port: Int

    var baseURL: URL {
        URL(string: "http://127.0.0.1:\(port)/")!
    }

    private var readinessURL: URL {
        baseURL.appendingPathComponent("ready")
    }

    private let environment: [String: String]
    private let fileManager: FileManager
    private var process: Process?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    private var stopRequested = false
    private var startedByApp = false
    private var logBuffer = ""
    private var launchLifecycle = BackendLaunchLifecycle()
    private var instanceNonce: String?

    nonisolated static let serviceIdentity = "biocircuits-explorer-backend"

    private var parentProcessIdentifierString: String {
        String(ProcessInfo.processInfo.processIdentifier)
    }

    init(
        port: Int? = nil,
        fileManager: FileManager = .default,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) {
        self.environment = environment
        self.port = port ?? Self.resolveConfiguredPort(from: environment)
        self.fileManager = fileManager
    }

    nonisolated static func resolveConfiguredPort(from environment: [String: String]) -> Int {
        for key in ["BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"] {
            guard
                let rawPort = environment[key]?.trimmingCharacters(in: .whitespacesAndNewlines),
                let port = Int(rawPort),
                (1...65_535).contains(port)
            else {
                continue
            }
            return port
        }

        // The workspace WebView pins this documented default origin, so the
        // engine retains it; the per-launch nonce prevents an unrelated
        // listener on that port being accepted.
        return 18_088
    }

    func startIfNeeded() async throws {
        if isReady {
            return
        }

        if isStarting {
            try await waitForOngoingStartup()
            return
        }

        let generation = launchLifecycle.advance()
        isStarting = true
        lastErrorMessage = nil
        statusMessage = "Checking backend"

        defer {
            if launchLifecycle.accepts(generation) {
                isStarting = false
            }
        }

        do {
            // Discovery and Process.run can fail before readiness polling starts.
            // Keep those failures in the same published error state as a backend
            // that exits during startup, so dismissing the alert does not leave
            // the window showing an endless startup spinner.
            try launchLifecycle.requireCurrent(generation)
            let nextInstanceNonce = LocalLoopbackService.makeNonce()
            let launchSpec = try resolveLaunchSpec(instanceNonce: nextInstanceNonce)
            instanceNonce = nextInstanceNonce
            try launchBackend(using: launchSpec, generation: generation)

            try await waitUntilReady(
                timeout: launchSpec.startupTimeout,
                generation: generation,
                expectedNonce: nextInstanceNonce
            )
            try launchLifecycle.requireCurrent(generation)
            isReady = true
            statusMessage = "Backend ready"
        } catch is CancellationError {
            if launchLifecycle.accepts(generation) {
                stop()
            }
            throw CancellationError()
        } catch {
            try launchLifecycle.requireCurrent(generation)
            stop()
            lastErrorMessage = error.localizedDescription
            statusMessage = "Backend failed to start"
            throw error
        }
    }

    func restart() async throws {
        let processToStop = requestStop()
        let stoppedGeneration = launchLifecycle.generation
        if let processToStop {
            let didStop = try await LocalProcessShutdown.waitForExitOrKill(processToStop)
            if !didStop {
                throw BackendError.startFailed("Backend process did not stop before restart.")
            }
        }
        try launchLifecycle.requireCurrent(stoppedGeneration)
        try await startIfNeeded()
    }

    func stop() {
        _ = requestStop()
    }

    /// Stops the backend and waits (graceful SIGTERM, then SIGKILL) for the
    /// child process to exit. The quit-preparation path must use this instead
    /// of the fire-and-forget `stop()` so the app cannot terminate before the
    /// Julia process has left, orphaning it (the historical macOS orphan
    /// process failure mode).
    func stopAndWait() async {
        let processToStop = requestStop()
        guard let processToStop else {
            return
        }
        _ = try? await LocalProcessShutdown.waitForExitOrKill(processToStop)
    }

    @discardableResult
    private func requestStop() -> Process? {
        launchLifecycle.advance()
        stopRequested = true
        isReady = false
        isStarting = false
        let processToStop = process
        clearPipeHandlers()
        self.process = nil
        if let processToStop, processToStop.isRunning {
            processToStop.terminate()
        }
        if startedByApp {
            statusMessage = "Backend stopped"
        }
        startedByApp = false
        instanceNonce = nil
        return processToStop
    }

    private func waitForOngoingStartup() async throws {
        while isStarting {
            try await Task.sleep(for: .milliseconds(200))
        }

        if isReady {
            return
        }

        if let lastErrorMessage {
            throw BackendError.startFailed(lastErrorMessage)
        }

        throw CancellationError()
    }

    private static func environmentValue(keys: [String], from environment: [String: String]) -> String? {
        for key in keys {
            if let value = environment[key] {
                return value
            }
        }
        return nil
    }

    nonisolated static func runtimeStorageEnvironment(
        applicationSupportDirectory: URL,
        instanceNonce: String
    ) -> [String: String] {
        let runtimeRoot = applicationSupportDirectory
            .appendingPathComponent("Biocircuits Explorer", isDirectory: true)
            .appendingPathComponent("Runtime", isDirectory: true)
        return [
            "BIOCIRCUITS_EXPLORER_INSTANCE_NONCE": instanceNonce,
            "BIOCIRCUITS_EXPLORER_JOB_STORE": runtimeRoot
                .appendingPathComponent("Jobs", isDirectory: true)
                .path,
            "BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT": runtimeRoot
                .appendingPathComponent("Atlas", isDirectory: true)
                .path,
        ]
    }

    private func nativeRuntimeEnvironment(instanceNonce: String) -> [String: String] {
        let applicationSupportDirectory = fileManager.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        ).first ?? URL(fileURLWithPath: NSHomeDirectory(), isDirectory: true)
            .appendingPathComponent("Library/Application Support", isDirectory: true)
        return Self.runtimeStorageEnvironment(
            applicationSupportDirectory: applicationSupportDirectory,
            instanceNonce: instanceNonce
        )
    }

    private func resolveLaunchSpec(instanceNonce: String) throws -> LaunchSpec {
        let repoRoots = configuredRepoRoots()

        if
            let configuredCompiledRoot = normalizedDirectoryURL(
                from: Self.environmentValue(
                    keys: ["BIOCIRCUITS_EXPLORER_BACKEND_ROOT", "ROP_BACKEND_ROOT"],
                    from: environment
                )
            ),
            let launchSpec = compiledLaunchSpec(
                for: configuredCompiledRoot,
                instanceNonce: instanceNonce
            )
        {
            return launchSpec
        }

        if
            prefersSourceBackendDuringDevelopment,
            let launchSpec = try sourceLaunchSpec(
                repoRoots: repoRoots,
                instanceNonce: instanceNonce
            )
        {
            return launchSpec
        }

        let compiledRoots = Self.bundledBackendRootCandidates(
            bundleURL: Bundle.main.bundleURL,
            resourceURL: Bundle.main.resourceURL
        )
        + repoRoots.map {
            [
                $0.appendingPathComponent("dist", isDirectory: true)
                    .appendingPathComponent("BiocircuitsExplorerBackend", isDirectory: true),
                $0.appendingPathComponent("dist", isDirectory: true)
                    .appendingPathComponent("ROPExplorerBackend", isDirectory: true),
            ]
        }
        .flatMap { $0 }

        for backendRoot in compiledRoots {
            if let launchSpec = compiledLaunchSpec(
                for: backendRoot,
                instanceNonce: instanceNonce
            ) {
                return launchSpec
            }
        }

        if let launchSpec = try sourceLaunchSpec(
            repoRoots: repoRoots,
            instanceNonce: instanceNonce
        ) {
            return launchSpec
        }

        throw BackendError.runtimeMissing
    }

    nonisolated static func bundledBackendRootCandidates(
        bundleURL: URL,
        resourceURL: URL?
    ) -> [URL] {
        var candidates = [
            bundleURL
                .appendingPathComponent("Contents", isDirectory: true)
                .appendingPathComponent("Helpers", isDirectory: true)
                .appendingPathComponent("BiocircuitsExplorerBackend", isDirectory: true),
        ]
        if let resourceURL {
            candidates.append(contentsOf: [
                resourceURL.appendingPathComponent("backend", isDirectory: true),
                resourceURL.appendingPathComponent("BiocircuitsExplorerBackend", isDirectory: true),
                resourceURL.appendingPathComponent("ROPExplorerBackend", isDirectory: true),
            ])
        }
        return candidates
    }

    private var prefersSourceBackendDuringDevelopment: Bool {
        if let rawOverride = Self.environmentValue(
            keys: ["BIOCIRCUITS_EXPLORER_PREFER_SOURCE_BACKEND", "ROP_PREFER_SOURCE_BACKEND"],
            from: environment
        )?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
            switch rawOverride {
            case "1", "true", "yes", "on":
                return true
            case "0", "false", "no", "off":
                return false
            default:
                break
            }
        }

        #if DEBUG
        return true
        #else
        return false
        #endif
    }

    private func compiledLaunchSpec(
        for backendRoot: URL,
        instanceNonce: String
    ) -> LaunchSpec? {
        let executableURL = [
            "biocircuits-explorer-backend",
            "rop-explorer-backend",
        ]
        .map {
            backendRoot
                .appendingPathComponent("bin", isDirectory: true)
                .appendingPathComponent($0)
        }
        .first(where: { fileManager.isExecutableFile(atPath: $0.path) })

        let publicDir = [
            "biocircuits-explorer",
            "rop-explorer",
        ]
        .map {
            backendRoot
                .appendingPathComponent("share", isDirectory: true)
                .appendingPathComponent($0, isDirectory: true)
                .appendingPathComponent("public", isDirectory: true)
        }
        .first(where: { fileManager.fileExists(atPath: $0.path) })

        guard
            let executableURL,
            let publicDir
        else {
            return nil
        }

        var bootstrapEnvironment: [String: String] = [
            "HOME": NSHomeDirectory(),
            "BIOCIRCUITS_EXPLORER_HOST": "127.0.0.1",
            "BIOCIRCUITS_EXPLORER_PORT": String(port),
            "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": publicDir.path,
            "BIOCIRCUITS_EXPLORER_PARENT_PID": parentProcessIdentifierString,
            "ROP_HOST": "127.0.0.1",
            "ROP_PORT": String(port),
            "ROP_PUBLIC_DIR": publicDir.path,
            "ROP_PARENT_PID": parentProcessIdentifierString,
        ]
        bootstrapEnvironment.merge(
            nativeRuntimeEnvironment(instanceNonce: instanceNonce)
        ) { _, nativeValue in nativeValue }
        let spawnEnv = bootstrapEnvironment

        return LaunchSpec(
            executableURL: executableURL,
            arguments: [],
            currentDirectoryURL: backendRoot,
            environment: spawnEnv,
            startupTimeout: 900,
            startupStatus: "Starting compiled backend"
        )
    }

    private func sourceLaunchSpec(
        repoRoots: [URL],
        instanceNonce: String
    ) throws -> LaunchSpec? {
        for repoRoot in repoRoots {
            let webappDir = repoRoot.appendingPathComponent("webapp", isDirectory: true)
            let bncDir = repoRoot.appendingPathComponent("Bnc_julia", isDirectory: true)
            let serverPath = webappDir.appendingPathComponent("server.jl")
            let publicDir = webappDir.appendingPathComponent("public", isDirectory: true)

            guard
                fileManager.fileExists(atPath: serverPath.path),
                fileManager.fileExists(atPath: bncDir.path)
            else {
                continue
            }

            let juliaURL = try resolveJuliaExecutable()
            var bootstrapEnvironment: [String: String] = [
                "HOME": NSHomeDirectory(),
                "BIOCIRCUITS_EXPLORER_HOST": "127.0.0.1",
                "BIOCIRCUITS_EXPLORER_PORT": String(port),
                "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": publicDir.path,
                "BIOCIRCUITS_EXPLORER_PARENT_PID": parentProcessIdentifierString,
                "ROP_HOST": "127.0.0.1",
                "ROP_PORT": String(port),
                "ROP_PUBLIC_DIR": publicDir.path,
                "ROP_PARENT_PID": parentProcessIdentifierString,
            ]
            bootstrapEnvironment.merge(
                nativeRuntimeEnvironment(instanceNonce: instanceNonce)
            ) { _, nativeValue in nativeValue }
            let spawnEnv = bootstrapEnvironment

            return LaunchSpec(
                executableURL: juliaURL,
                arguments: [
                    "--startup-file=no",
                    "--project=\(webappDir.path)",
                    serverPath.path,
                ],
                currentDirectoryURL: webappDir,
                environment: spawnEnv,
                startupTimeout: 900,
                    startupStatus: "Starting Julia backend from source"
            )
        }

        return nil
    }

    private func resolveJuliaExecutable() throws -> URL {
        if let configuredPath = normalizedExecutablePath(from: environment["JULIA_EXECUTABLE"]) {
            return URL(fileURLWithPath: configuredPath)
        }

        let candidates = executableSearchCandidates(named: "julia") + [
            "\(NSHomeDirectory())/.juliaup/bin/julia",
            "/opt/homebrew/bin/julia",
            "/usr/local/bin/julia",
        ]

        for path in candidates where fileManager.isExecutableFile(atPath: path) {
            return URL(fileURLWithPath: path)
        }

        throw BackendError.juliaMissing
    }

    private func configuredRepoRoots() -> [URL] {
        var candidates = [URL]()

        if let configuredRoot = normalizedDirectoryURL(
            from: Self.environmentValue(
                keys: ["BIOCIRCUITS_EXPLORER_REPO_ROOT", "ROP_REPO_ROOT"],
                from: environment
            )
        ) {
            candidates.append(configuredRoot)
        }

        let sourceFileURL = URL(fileURLWithPath: #filePath)
        let derivedRoot = sourceFileURL
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .standardizedFileURL
        candidates.append(derivedRoot)

        return uniqueURLs(candidates)
    }

    private func normalizedDirectoryURL(from rawPath: String?) -> URL? {
        guard let rawPath else {
            return nil
        }

        let trimmed = rawPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        return URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath, isDirectory: true)
            .standardizedFileURL
    }

    private func normalizedExecutablePath(from rawPath: String?) -> String? {
        guard let rawPath else {
            return nil
        }

        let trimmed = rawPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        let expanded = (trimmed as NSString).expandingTildeInPath
        let standardizedPath = URL(fileURLWithPath: expanded).standardizedFileURL.path
        return fileManager.isExecutableFile(atPath: standardizedPath) ? standardizedPath : nil
    }

    private func executableSearchCandidates(named executableName: String) -> [String] {
        guard let rawPath = environment["PATH"] else {
            return []
        }

        let candidates = rawPath
            .split(separator: ":")
            .map { String($0) }
            .filter { !$0.isEmpty }
            .map { pathEntry in
                URL(fileURLWithPath: pathEntry, isDirectory: true)
                    .appendingPathComponent(executableName, isDirectory: false)
                    .path
            }

        return uniquePaths(candidates)
    }

    private func uniqueURLs(_ urls: [URL]) -> [URL] {
        var seen = Set<String>()
        return urls.filter { url in
            let path = url.path
            return seen.insert(path).inserted
        }
    }

    private func uniquePaths(_ paths: [String]) -> [String] {
        var seen = Set<String>()
        return paths.filter { path in
            seen.insert(path).inserted
        }
    }

    private func launchBackend(using launchSpec: LaunchSpec, generation: UInt64) throws {
        try launchLifecycle.requireCurrent(generation)

        let process = Process()
        process.executableURL = launchSpec.executableURL
        process.arguments = launchSpec.arguments
        process.currentDirectoryURL = launchSpec.currentDirectoryURL

        var environment = ProcessInfo.processInfo.environment
        environment.merge(launchSpec.environment) { _, newValue in newValue }
        process.environment = environment

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        self.stdoutPipe = stdoutPipe
        self.stderrPipe = stderrPipe
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        stdoutPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else {
                return
            }
            Task { @MainActor [weak self] in
                guard
                    let self,
                    self.launchLifecycle.accepts(generation),
                    self.stdoutPipe === stdoutPipe
                else {
                    return
                }
                self.appendLog(text)
            }
        }

        stderrPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else {
                return
            }
            Task { @MainActor [weak self] in
                guard
                    let self,
                    self.launchLifecycle.accepts(generation),
                    self.stderrPipe === stderrPipe
                else {
                    return
                }
                self.appendLog(text)
            }
        }

        process.terminationHandler = { [weak self] terminatedProcess in
            Task { @MainActor [weak self] in
                guard let self else {
                    return
                }
                guard
                    self.launchLifecycle.accepts(generation),
                    self.process === terminatedProcess
                else {
                    return
                }

                self.isReady = false
                self.isStarting = false
                self.clearPipeHandlers()
                self.process = nil
                self.instanceNonce = nil
                let expectedStop = Self.processTerminationWasExpected(
                    stopRequested: self.stopRequested,
                    terminationReason: terminatedProcess.terminationReason,
                    terminationStatus: terminatedProcess.terminationStatus
                )
                self.stopRequested = false
                self.startedByApp = false

                if expectedStop {
                    return
                }

                let output = self.logBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
                self.lastErrorMessage = output.isEmpty
                    ? "ROP backend exited unexpectedly."
                    : output
                self.statusMessage = "Backend exited unexpectedly"
            }
        }

        stopRequested = false
        startedByApp = true
        statusMessage = launchSpec.startupStatus
        logBuffer = ""
        self.process = process
        do {
            try process.run()
        } catch {
            if launchLifecycle.accepts(generation), self.process === process {
                clearPipeHandlers()
                self.process = nil
                startedByApp = false
            }
            throw error
        }
    }

    private func clearPipeHandlers() {
        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil
        stdoutPipe = nil
        stderrPipe = nil
    }

    private func appendLog(_ text: String) {
        logBuffer.append(text)
        if logBuffer.count > 20_000 {
            logBuffer = String(logBuffer.suffix(20_000))
        }
    }

    private func waitUntilReady(
        timeout: TimeInterval,
        generation: UInt64,
        expectedNonce: String
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        while Date() < deadline {
            try launchLifecycle.requireCurrent(generation)
            if await probeBackend(expectedNonce: expectedNonce) {
                try launchLifecycle.requireCurrent(generation)
                return
            }

            try launchLifecycle.requireCurrent(generation)
            guard let process, process.isRunning else {
                let detail = lastErrorMessage ?? "Backend process exited before readiness."
                throw BackendError.startFailed(detail)
            }

            try await Task.sleep(for: .seconds(1))
        }

        throw BackendError.startFailed("ROP backend did not become ready within \(Int(timeout)) seconds.")
    }

    private func probeBackend(expectedNonce: String) async -> Bool {
        var request = URLRequest(url: readinessURL)
        request.timeoutInterval = 2

        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                return false
            }
            return Self.readinessProbeSucceeded(
                statusCode: http.statusCode,
                body: data,
                expectedNonce: expectedNonce
            )
        } catch {
            return false
        }
    }

    nonisolated static func readinessProbeSucceeded(
        statusCode: Int,
        body: Data,
        expectedNonce: String
    ) -> Bool {
        guard
            statusCode == 200,
            let payload = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
            payload["status"] as? String == "ready",
            payload["service"] as? String == serviceIdentity,
            payload["instance_nonce"] as? String == expectedNonce
        else {
            return false
        }

        return true
    }

    nonisolated static func processTerminationWasExpected(
        stopRequested: Bool,
        terminationReason _: Process.TerminationReason,
        terminationStatus _: Int32
    ) -> Bool {
        // The backend is a long-running service. A zero exit status is still an
        // unexpected loss unless this controller explicitly requested the stop.
        stopRequested
    }
}

extension BiocircuitsBackendController {
    enum BackendError: LocalizedError {
        case runtimeMissing
        case juliaMissing
        case startFailed(String)

        var errorDescription: String? {
            switch self {
            case .runtimeMissing:
                return "Could not find a usable Biocircuits Explorer backend. Expected either a bundled/compiled backend, or a repo root with `webapp/` and `Bnc_julia/`. You can override the discovery roots with `BIOCIRCUITS_EXPLORER_BACKEND_ROOT` or `BIOCIRCUITS_EXPLORER_REPO_ROOT` (legacy `ROP_BACKEND_ROOT` / `ROP_REPO_ROOT`)."
            case .juliaMissing:
                return "Could not find a Julia executable on this machine, and no compiled backend was available. Set `JULIA_EXECUTABLE` to override the Julia path if needed."
            case let .startFailed(message):
                return message
            }
        }
    }
}
