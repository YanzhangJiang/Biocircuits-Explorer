import Foundation

typealias ProjectTrashHandler = @Sendable (URL) throws -> Void

nonisolated struct PersistedProject: Sendable {
    let fileURL: URL
    var name: String
    var modifiedAt: Date
    var document: WorkspaceDocument

    var id: String { fileURL.path }
}

nonisolated struct ProjectPersistenceSnapshot: Sendable {
    let revision: UInt64
    let projects: [PersistedProject]
    let loadFailures: [String]
}

nonisolated struct ProjectPersistenceMutation<Value: Sendable>: Sendable {
    let value: Value
    let snapshot: ProjectPersistenceSnapshot
}

nonisolated struct ProjectPersistenceBatchFailure: Sendable {
    let item: String
    let message: String
}

nonisolated struct ProjectPersistenceBatch<Value: Sendable>: Sendable {
    let successes: [Value]
    let failures: [ProjectPersistenceBatchFailure]
    let snapshot: ProjectPersistenceSnapshot
}

actor ProjectPersistence {
    private let projectsDirectory: URL
    private let legacyProjectsDirectory: URL?
    private let fileManager: FileManager
    private let moveToTrash: ProjectTrashHandler
    private let decoder = JSONDecoder()
    private let encoder: JSONEncoder

    private var didBootstrap = false
    private var projects: [PersistedProject] = []
    private var revision: UInt64 = 0

    init(
        projectsDirectory: URL,
        legacyProjectsDirectory: URL?,
        fileManager: FileManager,
        moveToTrash: @escaping ProjectTrashHandler
    ) {
        self.projectsDirectory = projectsDirectory
        self.legacyProjectsDirectory = legacyProjectsDirectory
        self.fileManager = fileManager
        self.moveToTrash = moveToTrash

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        self.encoder = encoder
    }

    func bootstrap() throws -> ProjectPersistenceSnapshot {
        if didBootstrap {
            return snapshot()
        }

        try fileManager.createDirectory(
            at: projectsDirectory,
            withIntermediateDirectories: true,
            attributes: nil
        )
        try migrateLegacyProjectsIfNeeded()

        let loaded = try loadProjectsFromDisk()
        projects = loaded.projects
        sortProjects()

        if projects.isEmpty {
            _ = try createProjectOnDisk(named: "Untitled Workspace")
        }

        didBootstrap = true
        revision &+= 1
        return snapshot(loadFailures: loaded.failures)
    }

    func createProject(named requestedName: String?) throws
        -> ProjectPersistenceMutation<PersistedProject>
    {
        try ensureBootstrapped()
        let project = try createProjectOnDisk(named: requestedName ?? "Untitled Workspace")
        recordMutation()
        return ProjectPersistenceMutation(value: project, snapshot: snapshot())
    }

    func duplicateProject(id: String) throws
        -> ProjectPersistenceMutation<PersistedProject>
    {
        try ensureBootstrapped()
        let project = try duplicateProjectOnDisk(id: id)
        recordMutation()
        return ProjectPersistenceMutation(value: project, snapshot: snapshot())
    }

    func duplicateProjects(ids: [String]) throws
        -> ProjectPersistenceBatch<PersistedProject>
    {
        try ensureBootstrapped()
        var successes: [PersistedProject] = []
        var failures: [ProjectPersistenceBatchFailure] = []
        successes.reserveCapacity(ids.count)

        for id in ids {
            let item = projects.first(where: { $0.id == id })?.name ?? id
            do {
                successes.append(try duplicateProjectOnDisk(id: id))
            } catch {
                failures.append(ProjectPersistenceBatchFailure(
                    item: item,
                    message: error.localizedDescription
                ))
            }
        }

        if !successes.isEmpty {
            recordMutation()
        }
        return ProjectPersistenceBatch(
            successes: successes,
            failures: failures,
            snapshot: snapshot()
        )
    }

    func renameProject(id: String, to requestedName: String) throws
        -> ProjectPersistenceMutation<PersistedProject>
    {
        try ensureBootstrapped()
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStore.ProjectStoreError.projectNotFound
        }

        let current = projects[index]
        let name = uniqueProjectName(from: requestedName, excludingID: id)
        guard name != current.name else {
            return ProjectPersistenceMutation(value: current, snapshot: snapshot())
        }

        let newURL = fileURL(forProjectNamed: name)
        try fileManager.moveItem(at: current.fileURL, to: newURL)

        let renamed = makePersistedProject(
            name: name,
            document: current.document,
            fileURL: newURL
        )
        projects[index] = renamed
        sortProjects()
        recordMutation()
        return ProjectPersistenceMutation(value: renamed, snapshot: snapshot())
    }

    func deleteProject(id: String) throws -> ProjectPersistenceSnapshot {
        try ensureBootstrapped()
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStore.ProjectStoreError.projectNotFound
        }

        let project = projects[index]
        try moveToTrash(project.fileURL)
        projects.remove(at: index)
        recordMutation()
        return snapshot()
    }

    func deleteProjects(ids: [String]) throws -> ProjectPersistenceBatch<String> {
        try ensureBootstrapped()
        var successes: [String] = []
        var failures: [ProjectPersistenceBatchFailure] = []
        successes.reserveCapacity(ids.count)

        for id in ids {
            let item = projects.first(where: { $0.id == id })?.name ?? id
            do {
                guard let index = projects.firstIndex(where: { $0.id == id }) else {
                    throw ProjectStore.ProjectStoreError.projectNotFound
                }
                try moveToTrash(projects[index].fileURL)
                projects.remove(at: index)
                successes.append(id)
            } catch {
                failures.append(ProjectPersistenceBatchFailure(
                    item: item,
                    message: error.localizedDescription
                ))
            }
        }

        if !successes.isEmpty {
            recordMutation()
        }
        return ProjectPersistenceBatch(
            successes: successes,
            failures: failures,
            snapshot: snapshot()
        )
    }

    func importProjects(from urls: [URL]) throws
        -> ProjectPersistenceBatch<PersistedProject>
    {
        try ensureBootstrapped()
        var successes: [PersistedProject] = []
        var failures: [ProjectPersistenceBatchFailure] = []
        successes.reserveCapacity(urls.count)

        for url in urls {
            do {
                successes.append(try importProjectFromDisk(url))
            } catch {
                failures.append(ProjectPersistenceBatchFailure(
                    item: url.lastPathComponent,
                    message: error.localizedDescription
                ))
            }
        }

        if !successes.isEmpty {
            recordMutation()
        }
        return ProjectPersistenceBatch(
            successes: successes,
            failures: failures,
            snapshot: snapshot()
        )
    }

    func updateDocument(_ document: WorkspaceDocument, for id: String) throws
        -> ProjectPersistenceSnapshot
    {
        try ensureBootstrapped()
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStore.ProjectStoreError.projectNotFound
        }

        let url = projects[index].fileURL
        try write(document, to: url)

        projects[index].document = document
        projects[index].modifiedAt = modificationDate(for: url)
        recordMutation()
        return snapshot()
    }

    private func ensureBootstrapped() throws {
        if !didBootstrap {
            _ = try bootstrap()
        }
    }

    private func recordMutation() {
        revision &+= 1
    }

    private func snapshot(loadFailures: [String] = []) -> ProjectPersistenceSnapshot {
        ProjectPersistenceSnapshot(
            revision: revision,
            projects: projects,
            loadFailures: loadFailures
        )
    }

    private func createProjectOnDisk(named requestedName: String) throws -> PersistedProject {
        let name = uniqueProjectName(from: requestedName)
        let document = WorkspaceDocument.starter(named: name)
        let url = fileURL(forProjectNamed: name)
        try write(document, to: url)

        let project = makePersistedProject(
            name: name,
            document: document,
            fileURL: url
        )
        projects.append(project)
        sortProjects()
        return project
    }

    private func duplicateProjectOnDisk(id: String) throws -> PersistedProject {
        guard let original = projects.first(where: { $0.id == id }) else {
            throw ProjectStore.ProjectStoreError.projectNotFound
        }

        let name = uniqueProjectName(from: "\(original.name) Copy")
        let url = fileURL(forProjectNamed: name)
        try write(original.document, to: url)

        let duplicated = makePersistedProject(
            name: name,
            document: original.document,
            fileURL: url
        )
        projects.append(duplicated)
        sortProjects()
        return duplicated
    }

    private func importProjectFromDisk(_ externalURL: URL) throws -> PersistedProject {
        let data = try readExternalData(from: externalURL)
        let document = try decoder.decode(WorkspaceDocument.self, from: data)
            .validatedForPersistence()
        let preferredName = externalURL.deletingPathExtension().lastPathComponent
        let resolvedName = uniqueProjectName(from: preferredName)
        let destinationURL = fileURL(forProjectNamed: resolvedName)
        try write(document, to: destinationURL)

        let project = makePersistedProject(
            name: resolvedName,
            document: document,
            fileURL: destinationURL
        )
        projects.append(project)
        sortProjects()
        return project
    }

    private func migrateLegacyProjectsIfNeeded() throws {
        guard
            let legacyProjectsDirectory,
            legacyProjectsDirectory != projectsDirectory,
            fileManager.fileExists(atPath: legacyProjectsDirectory.path)
        else {
            return
        }

        let currentURLs = try jsonFiles(in: projectsDirectory, propertyKeys: [])
        guard currentURLs.isEmpty else {
            return
        }

        let legacyURLs = try jsonFiles(in: legacyProjectsDirectory, propertyKeys: [])
        for legacyURL in legacyURLs {
            let preferredName = legacyURL.deletingPathExtension().lastPathComponent
            let resolvedName = uniqueProjectName(from: preferredName)
            let destinationURL = fileURL(forProjectNamed: resolvedName)
            try fileManager.copyItem(at: legacyURL, to: destinationURL)
        }
    }

    private func loadProjectsFromDisk() throws
        -> (projects: [PersistedProject], failures: [String])
    {
        let urls = try jsonFiles(
            in: projectsDirectory,
            propertyKeys: [.contentModificationDateKey]
        )
        var loadedProjects: [PersistedProject] = []
        var loadFailures: [String] = []
        loadedProjects.reserveCapacity(urls.count)

        for url in urls {
            do {
                loadedProjects.append(try loadProject(from: url))
            } catch {
                loadFailures.append("\(url.lastPathComponent): \(error.localizedDescription)")
            }
        }

        return (loadedProjects, loadFailures)
    }

    private func jsonFiles(
        in directory: URL,
        propertyKeys: [URLResourceKey]
    ) throws -> [URL] {
        try fileManager.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: propertyKeys,
            options: [.skipsHiddenFiles]
        )
        .filter { $0.pathExtension.lowercased() == "json" }
        .sorted {
            $0.lastPathComponent.localizedStandardCompare($1.lastPathComponent) == .orderedAscending
        }
    }

    private func loadProject(from url: URL) throws -> PersistedProject {
        let data = try Data(contentsOf: url)
        let document = try decoder.decode(WorkspaceDocument.self, from: data)
            .validatedForPersistence()
        return PersistedProject(
            fileURL: url,
            name: url.deletingPathExtension().lastPathComponent,
            modifiedAt: modificationDate(for: url),
            document: document
        )
    }

    private func write(_ document: WorkspaceDocument, to url: URL) throws {
        let data = try encoder.encode(document.validatedForPersistence())
        try data.write(to: url, options: .atomic)
    }

    private func makePersistedProject(
        name: String,
        document: WorkspaceDocument,
        fileURL: URL
    ) -> PersistedProject {
        PersistedProject(
            fileURL: fileURL,
            name: name,
            modifiedAt: modificationDate(for: fileURL),
            document: document
        )
    }

    private func modificationDate(for url: URL) -> Date {
        (try? url.resourceValues(
            forKeys: [.contentModificationDateKey]
        ).contentModificationDate) ?? Date()
    }

    private func readExternalData(from url: URL) throws -> Data {
        let didAccess = url.startAccessingSecurityScopedResource()
        defer {
            if didAccess {
                url.stopAccessingSecurityScopedResource()
            }
        }
        return try Data(contentsOf: url)
    }

    private func fileURL(forProjectNamed name: String) -> URL {
        projectsDirectory
            .appendingPathComponent(name, isDirectory: false)
            .appendingPathExtension("json")
    }

    private func sortProjects() {
        projects.sort {
            $0.name.localizedStandardCompare($1.name) == .orderedAscending
        }
    }

    private func uniqueProjectName(from requestedName: String, excludingID: String? = nil) -> String {
        let sanitizedBase = sanitizeProjectName(requestedName)
        let existingNames = Set(
            projects
                .filter { $0.id != excludingID }
                .map { $0.name.lowercased() }
        )

        func isAvailable(_ candidate: String) -> Bool {
            guard !existingNames.contains(candidate.lowercased()) else {
                return false
            }
            let candidateURL = fileURL(forProjectNamed: candidate)
            if candidateURL.path == excludingID {
                return true
            }
            if
                let excludingID,
                urlsReferToSameFile(candidateURL, URL(fileURLWithPath: excludingID))
            {
                return true
            }
            return !fileManager.fileExists(atPath: candidateURL.path)
        }

        if isAvailable(sanitizedBase) {
            return sanitizedBase
        }

        var counter = 2
        while true {
            let candidate = "\(sanitizedBase) \(counter)"
            if isAvailable(candidate) {
                return candidate
            }
            counter += 1
        }
    }

    private func urlsReferToSameFile(_ lhs: URL, _ rhs: URL) -> Bool {
        let lhsURL = lhs.standardizedFileURL
        let rhsURL = rhs.standardizedFileURL
        if lhsURL == rhsURL {
            return true
        }
        guard
            fileManager.fileExists(atPath: lhsURL.path),
            fileManager.fileExists(atPath: rhsURL.path),
            let lhsIdentifier = try? lhsURL.resourceValues(
                forKeys: [.fileResourceIdentifierKey]
            ).fileResourceIdentifier as? AnyHashable,
            let rhsIdentifier = try? rhsURL.resourceValues(
                forKeys: [.fileResourceIdentifierKey]
            ).fileResourceIdentifier as? AnyHashable
        else {
            return false
        }
        return lhsIdentifier == rhsIdentifier
    }

    private func sanitizeProjectName(_ rawName: String) -> String {
        let trimmed = rawName.trimmedNonEmpty ?? "Untitled Workspace"
        let invalidCharacters = CharacterSet(charactersIn: "/:\\?%*|\"<>")
        let cleaned = trimmed
            .components(separatedBy: invalidCharacters)
            .joined(separator: " ")
            .replacingOccurrences(
                of: ".json",
                with: "",
                options: [.caseInsensitive, .anchored, .backwards]
            )
            .split(whereSeparator: \.isWhitespace)
            .joined(separator: " ")

        let visibleName = cleaned.trimmingCharacters(in: CharacterSet(charactersIn: ". "))
        return visibleName.isEmpty ? "Untitled Workspace" : visibleName
    }
}

nonisolated private extension String {
    var trimmedNonEmpty: String? {
        let trimmed = trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
