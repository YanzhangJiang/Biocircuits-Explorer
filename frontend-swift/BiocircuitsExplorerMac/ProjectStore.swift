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

    struct BatchFailure: Equatable {
        let item: String
        let message: String
    }

    struct BatchResult<Success> {
        var successes: [Success] = []
        var failures: [BatchFailure] = []

        var isComplete: Bool { failures.isEmpty }
    }

    @Published private(set) var projects: [ProjectFile] = []
    @Published private(set) var isReady = false
    @Published var lastErrorMessage: String?

    let projectsDirectory: URL
    let legacyProjectsDirectory: URL?

    private let persistence: ProjectPersistence
    private var appliedRevision: UInt64 = 0
    private var bootstrapTask: Task<Void, Never>?

    init(
        fileManager: FileManager = .default,
        projectsDirectoryOverride: URL? = nil,
        legacyProjectsDirectoryOverride: URL? = nil,
        moveToTrash: ProjectTrashHandler? = nil
    ) {
        var directoryResolutionError: String?
        if let projectsDirectoryOverride {
            projectsDirectory = projectsDirectoryOverride
            legacyProjectsDirectory = legacyProjectsDirectoryOverride
        } else {
            do {
                let appSupportURL = try fileManager.url(
                    for: .applicationSupportDirectory,
                    in: .userDomainMask,
                    appropriateFor: nil,
                    create: false
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
                directoryResolutionError = "Failed to resolve Application Support. Falling back to \(fallbackURL.path)."
            }
        }

        let trashHandler: ProjectTrashHandler = moveToTrash ?? { url in
            try FileManager.default.trashItem(at: url, resultingItemURL: nil)
        }
        persistence = ProjectPersistence(
            projectsDirectory: projectsDirectory,
            legacyProjectsDirectory: legacyProjectsDirectory,
            fileManager: fileManager,
            moveToTrash: trashHandler
        )
        lastErrorMessage = directoryResolutionError

        bootstrapTask = Task { [weak self] in
            await self?.bootstrap(directoryResolutionError: directoryResolutionError)
        }
    }

    func waitUntilReady() async {
        await bootstrapTask?.value
    }

    func project(withID id: String?) -> ProjectFile? {
        guard let id else {
            return nil
        }
        return projects.first(where: { $0.id == id })
    }

    @discardableResult
    func createProject(named requestedName: String? = nil) async throws -> ProjectFile {
        await waitUntilReady()
        let result = try await persistence.createProject(named: requestedName)
        apply(result.snapshot)
        return makeProjectFile(result.value)
    }

    @discardableResult
    func duplicateProject(id: String) async throws -> ProjectFile {
        await waitUntilReady()
        let result = try await persistence.duplicateProject(id: id)
        apply(result.snapshot)
        return makeProjectFile(result.value)
    }

    func duplicateProjects(ids: [String]) async -> BatchResult<ProjectFile> {
        await waitUntilReady()
        do {
            let result = try await persistence.duplicateProjects(ids: ids)
            apply(result.snapshot)
            return BatchResult(
                successes: result.successes.map(makeProjectFile),
                failures: result.failures.map(makeBatchFailure)
            )
        } catch {
            return BatchResult(failures: [BatchFailure(
                item: "Projects",
                message: error.localizedDescription
            )])
        }
    }

    @discardableResult
    func renameProject(id: String, to requestedName: String) async throws -> ProjectFile {
        await waitUntilReady()
        let result = try await persistence.renameProject(id: id, to: requestedName)
        apply(result.snapshot)
        return makeProjectFile(result.value)
    }

    func deleteProject(id: String) async throws {
        await waitUntilReady()
        apply(try await persistence.deleteProject(id: id))
    }

    func deleteProjects(ids: [String]) async -> BatchResult<String> {
        await waitUntilReady()
        do {
            let result = try await persistence.deleteProjects(ids: ids)
            apply(result.snapshot)
            return BatchResult(
                successes: result.successes,
                failures: result.failures.map(makeBatchFailure)
            )
        } catch {
            return BatchResult(failures: [BatchFailure(
                item: "Projects",
                message: error.localizedDescription
            )])
        }
    }

    func importProjects(from urls: [URL]) async -> BatchResult<ProjectFile> {
        await waitUntilReady()
        do {
            let result = try await persistence.importProjects(from: urls)
            apply(result.snapshot)
            return BatchResult(
                successes: result.successes.map(makeProjectFile),
                failures: result.failures.map(makeBatchFailure)
            )
        } catch {
            return BatchResult(failures: [BatchFailure(
                item: "Projects",
                message: error.localizedDescription
            )])
        }
    }

    func updateDocument(_ document: WorkspaceDocument, for id: String) async throws {
        await waitUntilReady()
        apply(try await persistence.updateDocument(document, for: id))
    }

    private func bootstrap(directoryResolutionError: String?) async {
        do {
            let snapshot = try await persistence.bootstrap()
            apply(snapshot)

            let loadError = snapshot.loadFailures.isEmpty
                ? nil
                : "Skipped \(snapshot.loadFailures.count) unreadable project file(s): "
                    + snapshot.loadFailures.joined(separator: "; ")
            lastErrorMessage = [directoryResolutionError, loadError]
                .compactMap { $0 }
                .joined(separator: " ")
                .nilIfEmpty
        } catch {
            lastErrorMessage = [directoryResolutionError, error.localizedDescription]
                .compactMap { $0 }
                .joined(separator: " ")
                .nilIfEmpty
        }

        isReady = true
        bootstrapTask = nil
    }

    private func apply(_ snapshot: ProjectPersistenceSnapshot) {
        guard snapshot.revision >= appliedRevision else {
            return
        }
        appliedRevision = snapshot.revision
        projects = snapshot.projects.map(makeProjectFile)
    }

    private func makeProjectFile(_ project: PersistedProject) -> ProjectFile {
        ProjectFile(
            fileURL: project.fileURL,
            name: project.name,
            modifiedAt: project.modifiedAt,
            document: project.document
        )
    }

    private func makeBatchFailure(_ failure: ProjectPersistenceBatchFailure) -> BatchFailure {
        BatchFailure(item: failure.item, message: failure.message)
    }
}

extension ProjectStore {
    nonisolated enum ProjectStoreError: LocalizedError {
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
    var nilIfEmpty: String? {
        isEmpty ? nil : self
    }
}
