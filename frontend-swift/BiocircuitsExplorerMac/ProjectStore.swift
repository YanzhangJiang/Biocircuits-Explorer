import Combine
import Foundation

@MainActor
final class ProjectStore: ObservableObject {
    struct ProjectFile: Identifiable {
        let fileURL: URL
        var name: String
        var modifiedAt: Date
        var document: WorkspaceDocument

        var id: String { fileURL.path }
    }

    @Published private(set) var projects: [ProjectFile] = []
    @Published var lastErrorMessage: String?

    let projectsDirectory: URL
    let legacyProjectsDirectory: URL?

    private let decoder = JSONDecoder()
    private let encoder: JSONEncoder
    private let fileManager: FileManager

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager

        do {
            let appSupportURL = try fileManager.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            projectsDirectory = appSupportURL
                .appendingPathComponent("Biocircuits Explorer", isDirectory: true)
                .appendingPathComponent("Projects", isDirectory: true)
            legacyProjectsDirectory = appSupportURL
                .appendingPathComponent("ROP-Explorer", isDirectory: true)
                .appendingPathComponent("Projects", isDirectory: true)
        } catch {
            let fallbackURL = fileManager.temporaryDirectory
                .appendingPathComponent("Biocircuits Explorer", isDirectory: true)
                .appendingPathComponent("Projects", isDirectory: true)
            projectsDirectory = fallbackURL
            legacyProjectsDirectory = fileManager.temporaryDirectory
                .appendingPathComponent("ROP-Explorer", isDirectory: true)
                .appendingPathComponent("Projects", isDirectory: true)
            lastErrorMessage = "Failed to resolve Application Support. Falling back to \(fallbackURL.path)."
        }

        encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]

        bootstrap()
    }

    func project(withID id: String?) -> ProjectFile? {
        guard let id else {
            return nil
        }

        return projects.first(where: { $0.id == id })
    }

    @discardableResult
    func createProject(named requestedName: String? = nil) throws -> ProjectFile {
        let name = uniqueProjectName(from: requestedName ?? "Untitled Workspace")
        let document = WorkspaceDocument.starter(named: name)
        let url = fileURL(forProjectNamed: name)
        try write(document, to: url)

        let project = try loadProject(from: url)
        projects.append(project)
        sortProjects()
        return project
    }

    @discardableResult
    func duplicateProject(id: String) throws -> ProjectFile {
        guard let original = project(withID: id) else {
            throw ProjectStoreError.projectNotFound
        }

        let duplicated = try createProject(named: "\(original.name) Copy")
        try updateDocument(original.document, for: duplicated.id)
        return project(withID: duplicated.id) ?? duplicated
    }

    @discardableResult
    func renameProject(id: String, to requestedName: String) throws -> ProjectFile {
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStoreError.projectNotFound
        }

        let current = projects[index]
        let name = uniqueProjectName(from: requestedName, excludingID: id)
        guard name != current.name else {
            return current
        }

        let document = current.document
        let newURL = fileURL(forProjectNamed: name)
        try write(document, to: newURL)
        try fileManager.removeItem(at: current.fileURL)

        let renamed = try loadProject(from: newURL)
        projects[index] = renamed
        sortProjects()
        return renamed
    }

    func deleteProject(id: String) throws {
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStoreError.projectNotFound
        }

        let project = projects.remove(at: index)
        try fileManager.removeItem(at: project.fileURL)
    }

    func importProjects(from urls: [URL]) throws -> [ProjectFile] {
        try urls.map(importProject)
    }

    func updateDocument(_ document: WorkspaceDocument, for id: String) throws {
        guard let index = projects.firstIndex(where: { $0.id == id }) else {
            throw ProjectStoreError.projectNotFound
        }

        let url = projects[index].fileURL
        try write(document, to: url)

        projects[index].document = document
        projects[index].modifiedAt = Date()
    }

    private func bootstrap() {
        do {
            try fileManager.createDirectory(at: projectsDirectory, withIntermediateDirectories: true, attributes: nil)
            try migrateLegacyProjectsIfNeeded()
            try reloadProjects()
            if projects.isEmpty {
                _ = try createProject()
            }
        } catch {
            lastErrorMessage = error.localizedDescription
        }
    }

    private func migrateLegacyProjectsIfNeeded() throws {
        guard
            let legacyProjectsDirectory,
            legacyProjectsDirectory != projectsDirectory,
            fileManager.fileExists(atPath: legacyProjectsDirectory.path)
        else {
            return
        }

        let currentURLs = try fileManager.contentsOfDirectory(
            at: projectsDirectory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        .filter { $0.pathExtension.lowercased() == "json" }

        guard currentURLs.isEmpty else {
            return
        }

        let legacyURLs = try fileManager.contentsOfDirectory(
            at: legacyProjectsDirectory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        .filter { $0.pathExtension.lowercased() == "json" }

        for legacyURL in legacyURLs {
            let preferredName = legacyURL.deletingPathExtension().lastPathComponent
            let resolvedName = uniqueProjectName(from: preferredName)
            let destinationURL = fileURL(forProjectNamed: resolvedName)
            try fileManager.copyItem(at: legacyURL, to: destinationURL)
        }
    }

    private func reloadProjects() throws {
        let urls = try fileManager.contentsOfDirectory(
            at: projectsDirectory,
            includingPropertiesForKeys: [.contentModificationDateKey],
            options: [.skipsHiddenFiles]
        )
        let jsonURLs = urls.filter { $0.pathExtension.lowercased() == "json" }
        projects = try jsonURLs.map(loadProject)
        sortProjects()
    }

    private func loadProject(from url: URL) throws -> ProjectFile {
        let data = try Data(contentsOf: url)
        let document = try decoder.decode(WorkspaceDocument.self, from: data).validatedForPersistence()
        let name = url.deletingPathExtension().lastPathComponent

        let values = try url.resourceValues(forKeys: [.contentModificationDateKey])
        return ProjectFile(
            fileURL: url,
            name: name,
            modifiedAt: values.contentModificationDate ?? Date(),
            document: document
        )
    }

    private func importProject(from externalURL: URL) throws -> ProjectFile {
        let data = try readExternalData(from: externalURL)
        let document = try decoder.decode(WorkspaceDocument.self, from: data).validatedForPersistence()
        let preferredName = externalURL.deletingPathExtension().lastPathComponent
        let resolvedName = uniqueProjectName(from: preferredName)

        let destinationURL = fileURL(forProjectNamed: resolvedName)
        try write(document, to: destinationURL)

        let project = try loadProject(from: destinationURL)
        projects.append(project)
        sortProjects()
        return project
    }

    private func write(_ document: WorkspaceDocument, to url: URL) throws {
        let data = try encoder.encode(document.validatedForPersistence())
        try data.write(to: url, options: .atomic)
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
        projectsDirectory.appendingPathComponent(name, isDirectory: false).appendingPathExtension("json")
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

        if !existingNames.contains(sanitizedBase.lowercased()) {
            return sanitizedBase
        }

        var counter = 2
        while true {
            let candidate = "\(sanitizedBase) \(counter)"
            if !existingNames.contains(candidate.lowercased()) {
                return candidate
            }
            counter += 1
        }
    }

    private func sanitizeProjectName(_ rawName: String) -> String {
        let trimmed = rawName.trimmedNonEmpty ?? "Untitled Workspace"
        let invalidCharacters = CharacterSet(charactersIn: "/:\\?%*|\"<>")
        let cleaned = trimmed
            .components(separatedBy: invalidCharacters)
            .joined(separator: " ")
            .replacingOccurrences(of: ".json", with: "", options: [.caseInsensitive, .anchored, .backwards])
            .split(whereSeparator: \.isWhitespace)
            .joined(separator: " ")

        return cleaned.isEmpty ? "Untitled Workspace" : cleaned
    }
}

extension ProjectStore {
    enum ProjectStoreError: LocalizedError {
        case projectNotFound

        var errorDescription: String? {
            switch self {
            case .projectNotFound:
                return "The selected project file could not be found."
            }
        }
    }
}

private extension String {
    var trimmedNonEmpty: String? {
        let trimmed = trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
