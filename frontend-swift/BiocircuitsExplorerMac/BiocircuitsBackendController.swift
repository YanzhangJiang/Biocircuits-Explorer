import Combine
import Foundation

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

    private let environment: [String: String]
    private let fileManager: FileManager
    private var process: Process?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    private var stopRequested = false
    private var startedByApp = false
    private var logBuffer = ""

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

    private static func resolveConfiguredPort(from environment: [String: String]) -> Int {
        guard
            let rawPort = Self.environmentValue(
                keys: ["BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"],
                from: environment
            )?.trimmingCharacters(in: .whitespacesAndNewlines),
            let port = Int(rawPort),
            (1...65_535).contains(port)
        else {
            return 18_088
        }

        return port
    }

    func startIfNeeded() async throws {
        if isReady {
            return
        }

        if isStarting {
            try await waitForOngoingStartup()
            return
        }

        isStarting = true
        lastErrorMessage = nil
        statusMessage = "Checking backend"

        defer {
            isStarting = false
        }

        if await probeBackend() {
            isReady = true
            statusMessage = "Connected to running backend"
            return
        }

        let launchSpec = try resolveLaunchSpec()
        try launchBackend(using: launchSpec)

        do {
            try await waitUntilReady(timeout: launchSpec.startupTimeout)
            isReady = true
            statusMessage = "Backend ready"
        } catch {
            stop()
            lastErrorMessage = error.localizedDescription
            statusMessage = "Backend failed to start"
            throw error
        }
    }

    func restart() async throws {
        stop()
        try await Task.sleep(for: .milliseconds(500))
        try await startIfNeeded()
    }

    func stop() {
        stopRequested = true
        isReady = false
        isStarting = false
        clearPipeHandlers()
        if let process, process.isRunning {
            process.terminate()
        }
        self.process = nil
        if startedByApp {
            statusMessage = "Backend stopped"
        }
        startedByApp = false
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
    }

    private static func environmentValue(keys: [String], from environment: [String: String]) -> String? {
        for key in keys {
            if let value = environment[key] {
                return value
            }
        }
        return nil
    }

    private func resolveLaunchSpec() throws -> LaunchSpec {
        let repoRoots = configuredRepoRoots()

        if
            let configuredCompiledRoot = normalizedDirectoryURL(
                from: Self.environmentValue(
                    keys: ["BIOCIRCUITS_EXPLORER_BACKEND_ROOT", "ROP_BACKEND_ROOT"],
                    from: environment
                )
            ),
            let launchSpec = compiledLaunchSpec(for: configuredCompiledRoot)
        {
            return launchSpec
        }

        if prefersSourceBackendDuringDevelopment, let launchSpec = try sourceLaunchSpec(repoRoots: repoRoots) {
            return launchSpec
        }

        let compiledRoots = [
            Bundle.main.resourceURL?.appendingPathComponent("backend", isDirectory: true),
            Bundle.main.resourceURL?.appendingPathComponent("BiocircuitsExplorerBackend", isDirectory: true),
            Bundle.main.resourceURL?.appendingPathComponent("ROPExplorerBackend", isDirectory: true),
        ]
        .compactMap { $0 }
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
            if let launchSpec = compiledLaunchSpec(for: backendRoot) {
                return launchSpec
            }
        }

        if let launchSpec = try sourceLaunchSpec(repoRoots: repoRoots) {
            return launchSpec
        }

        throw BackendError.runtimeMissing
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

    private func compiledLaunchSpec(for backendRoot: URL) -> LaunchSpec? {
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

        return LaunchSpec(
            executableURL: executableURL,
            arguments: [],
            currentDirectoryURL: backendRoot,
            environment: [
                "HOME": NSHomeDirectory(),
                "BIOCIRCUITS_EXPLORER_PORT": String(port),
                "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": publicDir.path,
                "BIOCIRCUITS_EXPLORER_PARENT_PID": parentProcessIdentifierString,
                "ROP_PORT": String(port),
                "ROP_PUBLIC_DIR": publicDir.path,
                "ROP_PARENT_PID": parentProcessIdentifierString,
            ],
            startupTimeout: 90,
            startupStatus: "Starting compiled backend"
        )
    }

    private func sourceLaunchSpec(repoRoots: [URL]) throws -> LaunchSpec? {
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
            return LaunchSpec(
                executableURL: juliaURL,
                arguments: [
                    "--startup-file=no",
                    "--project=\(webappDir.path)",
                    serverPath.path,
                ],
                currentDirectoryURL: webappDir,
                environment: [
                    "HOME": NSHomeDirectory(),
                    "BIOCIRCUITS_EXPLORER_PORT": String(port),
                    "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": publicDir.path,
                    "BIOCIRCUITS_EXPLORER_PARENT_PID": parentProcessIdentifierString,
                    "ROP_PORT": String(port),
                    "ROP_PUBLIC_DIR": publicDir.path,
                    "ROP_PARENT_PID": parentProcessIdentifierString,
                ],
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

    private func launchBackend(using launchSpec: LaunchSpec) throws {
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
                self?.appendLog(text)
            }
        }

        stderrPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else {
                return
            }
            Task { @MainActor [weak self] in
                self?.appendLog(text)
            }
        }

        process.terminationHandler = { [weak self] terminatedProcess in
            Task { @MainActor [weak self] in
                guard let self else {
                    return
                }

                self.isReady = false
                self.clearPipeHandlers()
                self.process = nil
                let expectedStop = self.stopRequested || terminatedProcess.terminationReason == .exit && terminatedProcess.terminationStatus == 0
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
        try process.run()
        self.process = process
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

    private func waitUntilReady(timeout: TimeInterval) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        while Date() < deadline {
            if await probeBackend() {
                return
            }

            try await Task.sleep(for: .seconds(1))
        }

        throw BackendError.startFailed("ROP backend did not become ready within \(Int(timeout)) seconds.")
    }

    private func probeBackend() async -> Bool {
        var request = URLRequest(url: baseURL)
        request.timeoutInterval = 2

        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                return false
            }
            return 200..<400 ~= http.statusCode
        } catch {
            return false
        }
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
