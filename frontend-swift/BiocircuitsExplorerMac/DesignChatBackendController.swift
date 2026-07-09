import Combine
import Foundation

/// Supervises the Python design-chat backend (`webapp/scripts/chat_api.py`) — the
/// conversational NL→behavior_spec→candidates service the Design Agent surface
/// talks to. It is intentionally a *sibling* of `BiocircuitsBackendController`
/// (the Julia analysis server): the Design Agent posts to this process, the node
/// workspace posts to the Julia one. The script is dependency-free (Python stdlib
/// only for its chat entry path. The service can start without an LLM key, but
/// design requests then return `need_key`; optional Reader features also need
/// their documented host Python packages and data.
///
/// Failure here is non-fatal: if Python or the script/datasets are missing the
/// node Workspace still works and the Design Agent surface reports "offline".
@MainActor
final class DesignChatBackendController: ObservableObject {
    private struct LaunchSpec {
        let executableURL: URL
        let arguments: [String]
        let currentDirectoryURL: URL
        let environment: [String: String]
    }

    @Published private(set) var isReady = false
    @Published private(set) var isStarting = false
    @Published private(set) var statusMessage = "Design backend not started"
    @Published var lastErrorMessage: String?
    private(set) var bearerToken: String?

    let port: Int
    let enginePort: Int

    /// The endpoint the frontend POSTs each conversation turn to.
    var endpointURL: URL {
        URL(string: "http://127.0.0.1:\(port)/design-chat")!
    }

    private var healthURL: URL {
        URL(string: "http://127.0.0.1:\(port)/health")!
    }

    private var allowedOrigin: String {
        Self.nativeAllowedOrigin(enginePort: enginePort)
    }

    private let startupTimeout: TimeInterval = 30

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
        enginePort: Int? = nil,
        fileManager: FileManager = .default,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) {
        self.environment = environment
        self.port = port ?? Self.resolveConfiguredPort(from: environment)
        self.enginePort = enginePort ?? Self.resolveConfiguredEnginePort(from: environment)
        self.fileManager = fileManager
    }

    private static func resolveConfiguredPort(from environment: [String: String]) -> Int {
        guard
            let rawPort = environment["BNE_CHAT_PORT"]?
                .trimmingCharacters(in: .whitespacesAndNewlines),
            let port = Int(rawPort),
            (1...65_535).contains(port)
        else {
            return 8_765
        }

        return port
    }

    nonisolated static func resolveConfiguredEnginePort(from environment: [String: String]) -> Int {
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
        return 18_088
    }

    nonisolated static func enginePortEnvironment(_ enginePort: Int) -> [String: String] {
        [
            "BIOCIRCUITS_EXPLORER_HOST": "127.0.0.1",
            "BIOCIRCUITS_EXPLORER_PORT": String(enginePort),
            "ROP_HOST": "127.0.0.1",
            "ROP_PORT": String(enginePort),
        ]
    }

    nonisolated static func nativeAllowedOrigin(enginePort: Int) -> String {
        "http://127.0.0.1:\(enginePort)"
    }

    nonisolated static func makeBearerToken() -> String {
        var generator = SystemRandomNumberGenerator()
        return (0..<32)
            .map { _ in String(format: "%02x", UInt8.random(in: .min ... .max, using: &generator)) }
            .joined()
    }

    nonisolated static func nativeSecurityEnvironment(
        enginePort: Int,
        bearerToken: String
    ) -> [String: String] {
        [
            "BNE_CHAT_ALLOWED_ORIGIN": nativeAllowedOrigin(enginePort: enginePort),
            "BNE_CHAT_BEARER_TOKEN": bearerToken,
            "BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK": "0",
        ]
    }

    nonisolated static func authenticatedRequest(
        url: URL,
        bearerToken: String,
        allowedOrigin: String
    ) -> URLRequest {
        var request = URLRequest(url: url)
        request.setValue("Bearer \(bearerToken)", forHTTPHeaderField: "Authorization")
        request.setValue(allowedOrigin, forHTTPHeaderField: "Origin")
        return request
    }

    /// Start the backend if it is not already running. Never throws: discovery or
    /// launch problems are surfaced via `statusMessage` / `lastErrorMessage` so the
    /// rest of the app keeps working.
    func startIfNeeded() async {
        if isReady {
            return
        }

        // Don't double-launch: join the in-flight startup instead of returning
        // immediately, so a concurrent caller (e.g. an early restart) can't race
        // it into spawning a second process or tearing down a good one.
        if isStarting {
            await waitForOngoingStartup()
            return
        }

        isStarting = true
        lastErrorMessage = nil
        statusMessage = "Checking design backend"

        defer {
            isStarting = false
        }

        if process?.isRunning == true, bearerToken != nil, await probeBackend() {
            isReady = true
            statusMessage = "Connected to running design backend"
            return
        }

        let nextBearerToken = Self.makeBearerToken()
        let launchSpec: LaunchSpec
        do {
            launchSpec = try resolveLaunchSpec(bearerToken: nextBearerToken)
        } catch {
            lastErrorMessage = error.localizedDescription
            statusMessage = "Design backend unavailable"
            return
        }

        do {
            try launchBackend(using: launchSpec)
            bearerToken = nextBearerToken
        } catch {
            bearerToken = nil
            lastErrorMessage = error.localizedDescription
            statusMessage = "Design backend failed to launch"
            return
        }

        if await waitUntilReady(timeout: startupTimeout) {
            isReady = true
            statusMessage = "Design backend ready"
        } else {
            let output = logBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
            stop()
            lastErrorMessage = output.isEmpty
                ? "Design backend did not become ready within \(Int(startupTimeout)) seconds."
                : output
            statusMessage = "Design backend failed to start"
        }
    }

    func restart() async {
        // Let any in-flight startup finish first so its trailing waitUntilReady
        // loop can't terminate the process we're about to launch.
        if isStarting {
            await waitForOngoingStartup()
        }
        stop()
        try? await Task.sleep(for: .milliseconds(300))
        await startIfNeeded()
    }

    private func waitForOngoingStartup() async {
        while isStarting {
            try? await Task.sleep(for: .milliseconds(200))
        }
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
        bearerToken = nil
        if startedByApp {
            statusMessage = "Design backend stopped"
        }
        startedByApp = false
    }

    private func resolveLaunchSpec(bearerToken: String) throws -> LaunchSpec {
        guard let scriptURL = locateChatScript() else {
            throw DesignChatError.scriptMissing
        }

        let pythonURL = try resolvePythonExecutable()
        let workingDir = scriptURL
            .deletingLastPathComponent() // scripts/
            .deletingLastPathComponent() // webapp/

        var spawnEnv: [String: String] = [
            "HOME": NSHomeDirectory(),
            "BNE_CHAT_HOST": "127.0.0.1",
            "BNE_CHAT_PORT": String(port),
            // Watchdog: chat_api.py exits if this PID disappears, so the helper
            // can never outlive the app (mirrors the Julia backend's PARENT_PID).
            "BNE_CHAT_PARENT_PID": parentProcessIdentifierString,
            "PYTHONUNBUFFERED": "1",
        ]
        spawnEnv.merge(Self.enginePortEnvironment(enginePort)) { _, explicit in explicit }
        spawnEnv.merge(Self.nativeSecurityEnvironment(
            enginePort: enginePort,
            bearerToken: bearerToken
        )) { _, explicit in explicit }
        if let configuredTraceDirectory = environment["BNE_TRACE_DIR"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
           !configuredTraceDirectory.isEmpty {
            spawnEnv["BNE_TRACE_DIR"] = configuredTraceDirectory
        } else if let appSupport = fileManager.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        ).first {
            spawnEnv["BNE_TRACE_DIR"] = appSupport
                .appendingPathComponent("Biocircuits Explorer", isDirectory: true)
                .appendingPathComponent("DesignAgentTraces", isDirectory: true)
                .path
        }
        // Surface any LLM credentials configured in the app's environment so the
        // NL→spec compiler can use them; the UI key panel still overrides per-turn.
        for key in ["BNE_LLM_PROVIDER", "BNE_LLM_API_KEY", "BNE_LLM_KEY_FILE", "BNE_LLM_BASE_URL", "BNE_LLM_MODEL"] {
            if let value = environment[key] {
                spawnEnv[key] = value
            }
        }

        return LaunchSpec(
            executableURL: pythonURL,
            arguments: ["-X", "utf8", scriptURL.path],
            currentDirectoryURL: workingDir,
            environment: spawnEnv
        )
    }

    /// Find `webapp/scripts/chat_api.py`. Search bundled resources first (packaged
    /// builds may ship the scripts), then the dev repo roots.
    private func locateChatScript() -> URL? {
        var candidates = [URL]()

        if let resourceURL = Bundle.main.resourceURL {
            candidates.append(contentsOf: Self.bundledChatScriptCandidates(resourceURL: resourceURL))
        }

        for root in configuredRepoRoots() {
            candidates.append(root
                .appendingPathComponent("webapp", isDirectory: true)
                .appendingPathComponent("scripts", isDirectory: true)
                .appendingPathComponent("chat_api.py"))
        }

        return candidates.first { fileManager.fileExists(atPath: $0.path) }
    }

    nonisolated static func bundledChatScriptCandidates(resourceURL: URL) -> [URL] {
        let scriptSuffix = ["webapp", "scripts", "chat_api.py"]
        var candidates = [
            resourceURL
                .appendingPathComponent("scripts", isDirectory: true)
                .appendingPathComponent("chat_api.py"),
            scriptSuffix.reduce(resourceURL) { $0.appendingPathComponent($1) },
        ]

        for backendName in ["backend", "BiocircuitsExplorerBackend", "ROPExplorerBackend"] {
            let backendResource = resourceURL
                .appendingPathComponent(backendName, isDirectory: true)
                .appendingPathComponent("share", isDirectory: true)
                .appendingPathComponent("biocircuits-explorer", isDirectory: true)
            candidates.append(scriptSuffix.reduce(backendResource) { $0.appendingPathComponent($1) })
        }

        return candidates
    }

    private func resolvePythonExecutable() throws -> URL {
        if let configuredPath = normalizedExecutablePath(
            from: Self.environmentValue(keys: ["BNE_PYTHON", "PYTHON_EXECUTABLE"], from: environment)
        ) {
            return URL(fileURLWithPath: configuredPath)
        }

        let candidates = executableSearchCandidates(named: "python3") + [
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3",
            "/usr/bin/python3",
        ]

        for path in candidates where fileManager.isExecutableFile(atPath: path) {
            return URL(fileURLWithPath: path)
        }

        throw DesignChatError.pythonMissing
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
            .deletingLastPathComponent() // BiocircuitsExplorerMac/
            .deletingLastPathComponent() // frontend-swift/
            .deletingLastPathComponent() // repo root
            .standardizedFileURL
        candidates.append(derivedRoot)

        var seen = Set<String>()
        return candidates.filter { seen.insert($0.path).inserted }
    }

    private static func environmentValue(keys: [String], from environment: [String: String]) -> String? {
        for key in keys {
            if let value = environment[key] {
                return value
            }
        }
        return nil
    }

    private func normalizedDirectoryURL(from rawPath: String?) -> URL? {
        guard let rawPath else { return nil }
        let trimmed = rawPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        return URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath, isDirectory: true)
            .standardizedFileURL
    }

    private func normalizedExecutablePath(from rawPath: String?) -> String? {
        guard let rawPath else { return nil }
        let trimmed = rawPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        let expanded = (trimmed as NSString).expandingTildeInPath
        let standardizedPath = URL(fileURLWithPath: expanded).standardizedFileURL.path
        return fileManager.isExecutableFile(atPath: standardizedPath) ? standardizedPath : nil
    }

    private func executableSearchCandidates(named executableName: String) -> [String] {
        guard let rawPath = environment["PATH"] else {
            return []
        }

        var seen = Set<String>()
        return rawPath
            .split(separator: ":")
            .map { String($0) }
            .filter { !$0.isEmpty }
            .map { pathEntry in
                URL(fileURLWithPath: pathEntry, isDirectory: true)
                    .appendingPathComponent(executableName, isDirectory: false)
                    .path
            }
            .filter { seen.insert($0).inserted }
    }

    private func launchBackend(using launchSpec: LaunchSpec) throws {
        let process = Process()
        process.executableURL = launchSpec.executableURL
        process.arguments = launchSpec.arguments
        process.currentDirectoryURL = launchSpec.currentDirectoryURL

        var environment = ProcessInfo.processInfo.environment
        environment.merge(launchSpec.environment) { _, newValue in newValue }
        // chat_api.py needs only the standard library — don't let a stray
        // conda/venv PYTHONHOME/PYTHONPATH shadow the chosen interpreter's stdlib.
        for key in ["PYTHONHOME", "PYTHONPATH", "PYTHONSTARTUP"] where launchSpec.environment[key] == nil {
            environment.removeValue(forKey: key)
        }
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
                guard let self else { return }
                guard self.process === terminatedProcess else { return }

                self.isReady = false
                self.clearPipeHandlers()
                self.process = nil
                self.bearerToken = nil
                let expectedStop = self.stopRequested
                    || (terminatedProcess.terminationReason == .exit && terminatedProcess.terminationStatus == 0)
                self.stopRequested = false
                self.startedByApp = false

                if expectedStop {
                    return
                }

                let output = self.logBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
                self.lastErrorMessage = output.isEmpty
                    ? "Design backend exited unexpectedly."
                    : output
                self.statusMessage = "Design backend exited unexpectedly"
            }
        }

        stopRequested = false
        startedByApp = true
        statusMessage = "Starting design backend"
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

    private func waitUntilReady(timeout: TimeInterval) async -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await probeBackend() {
                return true
            }
            try? await Task.sleep(for: .milliseconds(500))
        }
        return false
    }

    private func probeBackend() async -> Bool {
        guard let bearerToken else {
            return false
        }
        var request = Self.authenticatedRequest(
            url: healthURL,
            bearerToken: bearerToken,
            allowedOrigin: allowedOrigin
        )
        request.timeoutInterval = 2

        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                return false
            }
            return 200..<300 ~= http.statusCode
        } catch {
            return false
        }
    }
}

extension DesignChatBackendController {
    enum DesignChatError: LocalizedError {
        case pythonMissing
        case scriptMissing

        var errorDescription: String? {
            switch self {
            case .pythonMissing:
                return "Could not find a `python3` executable for the Design Agent backend. Install Python 3 or set `BNE_PYTHON` to its path. The node Workspace is unaffected."
            case .scriptMissing:
                return "Could not find `webapp/scripts/chat_api.py` for the Design Agent backend. Set `BIOCIRCUITS_EXPLORER_REPO_ROOT` to the repository root. The node Workspace is unaffected."
            }
        }
    }
}
