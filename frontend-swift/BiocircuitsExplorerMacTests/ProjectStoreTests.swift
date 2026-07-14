import Foundation
import Testing
@testable import BiocircuitsExplorerMac

@MainActor
struct ProjectStoreTests {
    nonisolated private final class TrashRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var recordedURLs: [URL] = []

        var urls: [URL] {
            lock.withLock { recordedURLs }
        }

        func record(_ url: URL) {
            lock.withLock {
                recordedURLs.append(url)
            }
        }
    }

    private func makeTemporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("BiocircuitsExplorer-ProjectStoreTests", isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        return directory
    }

    private func writeDocument(_ document: WorkspaceDocument, to url: URL) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        try encoder.encode(document.validatedForPersistence()).write(to: url, options: .atomic)
    }

    private func makeStore(
        projectsDirectory: URL,
        legacyProjectsDirectory: URL? = nil,
        trashRecorder: TrashRecorder? = nil
    ) async -> ProjectStore {
        let store = ProjectStore(
            projectsDirectoryOverride: projectsDirectory,
            legacyProjectsDirectoryOverride: legacyProjectsDirectory,
            moveToTrash: { url in
                try FileManager.default.removeItem(at: url)
                trashRecorder?.record(url)
            }
        )
        await store.waitUntilReady()
        return store
    }

    @Test
    func oneUnreadableProjectDoesNotHideValidProjects() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let validURL = directory.appendingPathComponent("Valid.json")
        let corruptURL = directory.appendingPathComponent("Corrupt.json")
        try writeDocument(
            WorkspaceDocument(rawObject: ["marker": .string("valid")]),
            to: validURL
        )
        try Data("{ not-json".utf8).write(to: corruptURL)

        let store = await makeStore(projectsDirectory: directory)

        #expect(store.projects.map(\.name) == ["Valid"])
        #expect(store.projects.first?.document.rawObject["marker"] == .string("valid"))
        #expect(store.lastErrorMessage?.contains("Corrupt.json") == true)
        #expect(FileManager.default.fileExists(atPath: corruptURL.path))
    }

    @Test
    func unloadedDiskNamesAreReservedWhenCreatingProjects() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let reservedURL = directory.appendingPathComponent("Reserved.json")
        try Data("{ unreadable".utf8).write(to: reservedURL)

        let created = try await store.createProject(named: "Reserved")

        #expect(created.name == "Reserved 2")
        #expect(FileManager.default.fileExists(atPath: reservedURL.path))
        #expect(FileManager.default.fileExists(atPath: created.fileURL.path))
    }

    @Test
    func failedDeleteKeepsTheInMemoryProject() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let project = try #require(store.projects.first)
        try FileManager.default.removeItem(at: project.fileURL)

        do {
            try await store.deleteProject(id: project.id)
            Issue.record("Deleting a missing file should fail")
        } catch {
            #expect(store.project(withID: project.id) != nil)
        }
    }

    @Test
    func renameMovesOneFileAndDoesNotMutateStateWhenTheMoveFails() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let original = try #require(store.projects.first)
        let document = WorkspaceDocument(rawObject: ["marker": .string("latest")])
        try await store.updateDocument(document, for: original.id)

        let renamed = try await store.renameProject(id: original.id, to: "Renamed")
        #expect(!FileManager.default.fileExists(atPath: original.fileURL.path))
        #expect(FileManager.default.fileExists(atPath: renamed.fileURL.path))
        #expect(renamed.document == document)
        #expect(store.project(withID: original.id) == nil)
        #expect(store.project(withID: renamed.id)?.document == document)

        try FileManager.default.removeItem(at: renamed.fileURL)
        do {
            _ = try await store.renameProject(id: renamed.id, to: "Cannot Move")
            Issue.record("Renaming a missing source file should fail")
        } catch {
            #expect(store.project(withID: renamed.id)?.name == "Renamed")
            #expect(store.project(withID: renamed.id)?.document == document)
        }
    }

    @Test
    func duplicateWritesTheCurrentDocumentWithoutAnIntermediateStarter() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let original = try #require(store.projects.first)
        let document = WorkspaceDocument(rawObject: ["marker": .string("current")])
        try await store.updateDocument(document, for: original.id)

        let duplicate = try await store.duplicateProject(id: original.id)

        #expect(duplicate.document == document)
        #expect(store.project(withID: original.id)?.document == document)
        #expect(FileManager.default.fileExists(atPath: duplicate.fileURL.path))
        let persisted = try JSONDecoder().decode(
            WorkspaceDocument.self,
            from: Data(contentsOf: duplicate.fileURL)
        )
        #expect(persisted == document)
    }

    @Test
    func batchDeleteReportsEachCommittedAndFailedIdentity() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let trashRecorder = TrashRecorder()
        let store = await makeStore(
            projectsDirectory: directory,
            trashRecorder: trashRecorder
        )
        let first = try #require(store.projects.first)
        let second = try await store.createProject(named: "Second")
        try FileManager.default.removeItem(at: second.fileURL)

        let result = await store.deleteProjects(ids: [first.id, second.id])

        #expect(result.successes == [first.id])
        #expect(result.failures.count == 1)
        #expect(result.failures.first?.item == "Second")
        #expect(store.project(withID: first.id) == nil)
        #expect(store.project(withID: second.id) != nil)
        #expect(trashRecorder.urls == [first.fileURL])
    }

    @Test
    func batchDuplicateAndImportReturnPartialResultsExplicitly() async throws {
        let directory = try makeTemporaryDirectory()
        let externalDirectory = try makeTemporaryDirectory()
        defer {
            try? FileManager.default.removeItem(at: directory)
            try? FileManager.default.removeItem(at: externalDirectory)
        }

        let store = await makeStore(projectsDirectory: directory)
        let original = try #require(store.projects.first)
        let duplication = await store.duplicateProjects(ids: [original.id, "missing-project"])
        #expect(duplication.successes.count == 1)
        #expect(duplication.failures.count == 1)
        #expect(duplication.successes.first?.document == original.document)

        let validImport = externalDirectory.appendingPathComponent("Valid Import.json")
        let invalidImport = externalDirectory.appendingPathComponent("Invalid Import.json")
        try writeDocument(
            WorkspaceDocument(rawObject: ["marker": .string("imported")]),
            to: validImport
        )
        try Data("{ invalid".utf8).write(to: invalidImport)

        let imported = await store.importProjects(from: [validImport, invalidImport])
        #expect(imported.successes.count == 1)
        #expect(imported.failures.count == 1)
        #expect(imported.successes.first?.document.rawObject["marker"] == .string("imported"))
        #expect(imported.failures.first?.item == "Invalid Import.json")
    }

    @Test
    func backgroundImportValidationMatchesTheWorkspaceDocumentContract() async throws {
        let directory = try makeTemporaryDirectory()
        let externalDirectory = try makeTemporaryDirectory()
        defer {
            try? FileManager.default.removeItem(at: directory)
            try? FileManager.default.removeItem(at: externalDirectory)
        }

        let store = await makeStore(projectsDirectory: directory)
        let cases: [(name: String, json: String)] = [
            ("Valid Unknown Fields", """
                {"version":1,"canvas":{"scale":2},"nodes":[],"connections":[],"unknown":{"flag":true,"values":[1,null,"x"]}}
                """),
            ("Missing Optional Fields", "{\"nodes\":[]}"),
            ("Missing Required Nodes", "{}"),
            ("Null Optional Fields", "{\"version\":null,\"canvas\":null,\"nodes\":[],\"connections\":null}"),
            ("Noninteger Version", "{\"version\":1.5}"),
            ("Future Version", "{\"version\":2}"),
            ("Invalid Canvas", "{\"canvas\":[],\"nodes\":[]}"),
            ("Invalid Nodes", "{\"nodes\":{}}"),
            ("Invalid Connections", "{\"nodes\":[],\"connections\":{}}"),
        ]

        for testCase in cases {
            let data = Data(testCase.json.utf8)
            let canonicalAccepts: Bool
            do {
                _ = try JSONDecoder().decode(WorkspaceDocument.self, from: data)
                    .validatedForPersistence()
                canonicalAccepts = true
            } catch {
                canonicalAccepts = false
            }

            let url = externalDirectory
                .appendingPathComponent(testCase.name)
                .appendingPathExtension("json")
            try data.write(to: url, options: .atomic)
            let result = await store.importProjects(from: [url])

            #expect(
                result.successes.count == (canonicalAccepts ? 1 : 0),
                "Background decoder disagreed for \(testCase.name)"
            )
            #expect(
                result.failures.count == (canonicalAccepts ? 0 : 1),
                "Background decoder disagreement was not reported for \(testCase.name)"
            )
        }
    }

    @Test
    func importRejectsDocumentsOutsideTheEmbeddedWorkspaceShapeContract() async throws {
        let directory = try makeTemporaryDirectory()
        let externalDirectory = try makeTemporaryDirectory()
        defer {
            try? FileManager.default.removeItem(at: directory)
            try? FileManager.default.removeItem(at: externalDirectory)
        }

        let store = await makeStore(projectsDirectory: directory)
        let invalidDocuments: [(name: String, json: String)] = [
            ("Missing Nodes", #"{"version":1}"#),
            ("Scale Below Range", #"{"canvas":{"scale":0.004},"nodes":[]}"#),
            ("Scale Above Range", #"{"canvas":{"scale":3.001},"nodes":[]}"#),
            ("Canvas Pan Outside Range", #"{"canvas":{"panX":1000000001},"nodes":[]}"#),
            ("Canvas Pan Wrong Type", #"{"canvas":{"panY":"0"},"nodes":[]}"#),
            ("Node Is Not Object", #"{"nodes":[null]}"#),
            ("Empty Node ID", #"{"nodes":[{"id":"  ","type":"reaction-network"}]}"#),
            ("Duplicate Node ID", #"{"nodes":[{"id":"n","type":"reaction-network"},{"id":"n","type":"markdown-note"}]}"#),
            ("Unsupported Node Type", #"{"nodes":[{"id":"n","type":"future-node"}]}"#),
            ("Node Data Is Array", #"{"nodes":[{"id":"n","type":"reaction-network","data":[]}]}"#),
            ("Node X Wrong Type", #"{"nodes":[{"id":"n","type":"reaction-network","x":"0"}]}"#),
            ("Node Y Is Nonfinite", #"{"nodes":[{"id":"n","type":"reaction-network","y":1e309}]}"#),
            ("Negative Node Width", #"{"nodes":[{"id":"n","type":"reaction-network","width":-1}]}"#),
            ("Node Height Wrong Type", #"{"nodes":[{"id":"n","type":"reaction-network","height":"100"}]}"#),
        ]

        var urls: [URL] = []
        for invalidDocument in invalidDocuments {
            let url = externalDirectory
                .appendingPathComponent(invalidDocument.name)
                .appendingPathExtension("json")
            try Data(invalidDocument.json.utf8).write(to: url, options: .atomic)
            urls.append(url)
        }

        let result = await store.importProjects(from: urls)

        #expect(result.successes.isEmpty)
        #expect(result.failures.count == invalidDocuments.count)
        #expect(Set(result.failures.map(\.item)) == Set(urls.map(\.lastPathComponent)))
    }

    @Test
    func importNormalizesWebDefaultsAndPreservesUnknownFields() async throws {
        let directory = try makeTemporaryDirectory()
        let externalDirectory = try makeTemporaryDirectory()
        defer {
            try? FileManager.default.removeItem(at: directory)
            try? FileManager.default.removeItem(at: externalDirectory)
        }

        let store = await makeStore(projectsDirectory: directory)
        let sourceURL = externalDirectory.appendingPathComponent("Forward Compatible.json")
        let source = #"{"version":null,"canvas":{"panX":null,"panY":-1000000000,"scale":0.005,"futureCanvas":"kept"},"nodes":[{"id":"node-1","type":"reaction-network","data":null,"x":null,"y":2,"width":0,"height":120,"futureNode":{"flag":true}}],"connections":null,"futureTop":[1,"kept"]}"#
        try Data(source.utf8).write(to: sourceURL, options: .atomic)

        let result = await store.importProjects(from: [sourceURL])
        let document = try #require(result.successes.first?.document)

        #expect(result.failures.isEmpty)
        #expect(document.version == WorkspaceDocument.currentVersion)
        #expect(document.rawObject["connections"] == .array([]))
        #expect(document.rawObject["futureTop"] == .array([.number(1), .string("kept")]))

        guard case let .object(canvas)? = document.rawObject["canvas"] else {
            Issue.record("Expected a normalized canvas object")
            return
        }
        #expect(canvas["panX"] == .number(0))
        #expect(canvas["panY"] == .number(-1_000_000_000))
        #expect(canvas["scale"] == .number(0.005))
        #expect(canvas["futureCanvas"] == .string("kept"))

        guard
            case let .array(nodes)? = document.rawObject["nodes"],
            case let .object(node)? = nodes.first
        else {
            Issue.record("Expected one normalized node")
            return
        }
        #expect(node["data"] == .object([:]))
        #expect(node["x"] == .number(0))
        #expect(node["y"] == .number(2))
        #expect(node["width"] == nil)
        #expect(node["height"] == .number(120))
        #expect(node["futureNode"] == .object(["flag": .bool(true)]))
    }

    @Test
    func caseOnlyRenameKeepsTheRequestedNameWithoutCreatingASuffix() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let original = try #require(store.projects.first)
        let requestedName = original.name.uppercased()

        let renamed = try await store.renameProject(id: original.id, to: requestedName)

        #expect(renamed.name == requestedName)
        #expect(!renamed.name.hasSuffix(" 2"))
        #expect(FileManager.default.fileExists(atPath: renamed.fileURL.path))
        #expect(store.projects.count == 1)
    }

    @Test
    func leadingDotNamesRemainVisibleAcrossReload() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let created = try await store.createProject(named: ".experiment")

        #expect(created.name == "experiment")
        #expect(!created.fileURL.lastPathComponent.hasPrefix("."))

        let reloaded = await makeStore(projectsDirectory: directory)
        #expect(reloaded.projects.contains(where: { $0.name == "experiment" }))
    }

    @Test
    func bootstrapPublishesOnlyAfterTheBackgroundReloadFinishes() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let projectURL = directory.appendingPathComponent("Existing.json")
        try writeDocument(
            WorkspaceDocument(rawObject: ["marker": .string("existing")]),
            to: projectURL
        )

        let store = ProjectStore(
            projectsDirectoryOverride: directory,
            legacyProjectsDirectoryOverride: nil,
            moveToTrash: { url in
                try FileManager.default.removeItem(at: url)
            }
        )

        #expect(!store.isReady)
        #expect(store.projects.isEmpty)

        await store.waitUntilReady()

        #expect(store.isReady)
        #expect(store.projects.map(\.name) == ["Existing"])
    }

    @Test
    func failedUpdateDoesNotPublishAnUnpersistedDocument() async throws {
        let directory = try makeTemporaryDirectory()
        let store = await makeStore(projectsDirectory: directory)
        let project = try #require(store.projects.first)
        let originalDocument = project.document
        let replacement = WorkspaceDocument(rawObject: ["marker": .string("not-persisted")])

        try FileManager.default.removeItem(at: directory)

        do {
            try await store.updateDocument(replacement, for: project.id)
            Issue.record("Writing into a removed project directory should fail")
        } catch {
            #expect(store.project(withID: project.id)?.document == originalDocument)
        }
    }

    @Test
    func concurrentAutosavesKeepPublishedAndPersistedDocumentsInSync() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = await makeStore(projectsDirectory: directory)
        let project = try #require(store.projects.first)
        let documents = (0..<12).map { index in
            WorkspaceDocument(rawObject: ["marker": .string("save-\(index)")])
        }
        let tasks = documents.map { document in
            Task { @MainActor in
                try await store.updateDocument(document, for: project.id)
            }
        }
        for task in tasks {
            try await task.value
        }

        let published = try #require(store.project(withID: project.id)?.document)
        let persisted = try JSONDecoder().decode(
            WorkspaceDocument.self,
            from: Data(contentsOf: project.fileURL)
        )
        #expect(published == persisted)
    }
}
