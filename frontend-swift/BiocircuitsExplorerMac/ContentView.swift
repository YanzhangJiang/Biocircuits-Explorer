import AppKit
import SwiftUI
import UniformTypeIdentifiers

struct ContentView: View {
    @AppStorage("biocircuitsExplorer.themeMode") private var themeMode = "auto"
    @StateObject private var store: ProjectStore
    @StateObject private var backendController: BiocircuitsBackendController
    @StateObject private var webController: WebShellController
    @State private var selectedProjectIDs: Set<String>
    @State private var activeProjectID: String?
    @State private var isSidebarPresented = true
    @State private var isImporting = false
    @State private var isRenaming = false
    @State private var renameDraft = ""
    @State private var errorMessage: String?

    private let sidebarTopInset: CGFloat = 14
    private let sidebarLeadingInset: CGFloat = 16
    private let sidebarBottomInset: CGFloat = 18

    private struct NodeMenuItem: Identifiable {
        let id: String
        let title: String
        let systemImage: String
    }

    private struct NodeMenuSection: Identifiable {
        let id: String
        let title: String
        let items: [NodeMenuItem]
    }

    private static let addNodeSections: [NodeMenuSection] = [
        NodeMenuSection(
            id: "utilities",
            title: "Utilities",
            items: [
                NodeMenuItem(id: "markdown-note", title: "Markdown Note", systemImage: "note.text")
            ]
        ),
        NodeMenuSection(
            id: "input",
            title: "Input",
            items: [
                NodeMenuItem(id: "reaction-network", title: "Reaction Network", systemImage: "point.3.connected.trianglepath")
            ]
        ),
        NodeMenuSection(
            id: "parameters",
            title: "Parameters",
            items: [
                NodeMenuItem(id: "siso-params", title: "SISO Config", systemImage: "slider.horizontal.3"),
                NodeMenuItem(id: "scan-1d-params", title: "Scan 1D Config", systemImage: "chart.line.uptrend.xyaxis"),
                NodeMenuItem(id: "scan-2d-params", title: "Scan 2D Config", systemImage: "square.grid.2x2"),
                NodeMenuItem(id: "rop-cloud-params", title: "ROP Cloud Config", systemImage: "cube.transparent"),
                NodeMenuItem(id: "fret-params", title: "FRET Config", systemImage: "sparkles.rectangle.stack"),
                NodeMenuItem(id: "rop-poly-params", title: "ROP Polyhedron Config", systemImage: "hexagon"),
                NodeMenuItem(id: "atlas-spec", title: "Atlas Spec", systemImage: "map"),
                NodeMenuItem(id: "atlas-query-config", title: "Atlas Query Config", systemImage: "line.3.horizontal.decrease.circle")
            ]
        ),
        NodeMenuSection(
            id: "process",
            title: "Process",
            items: [
                NodeMenuItem(id: "atlas-builder", title: "Atlas Builder", systemImage: "hammer")
            ]
        ),
        NodeMenuSection(
            id: "results",
            title: "Results",
            items: [
                NodeMenuItem(id: "model-summary", title: "Model Summary", systemImage: "list.bullet.rectangle"),
                NodeMenuItem(id: "vertices-table", title: "Vertices Table", systemImage: "tablecells"),
                NodeMenuItem(id: "regime-graph", title: "Regime Graph", systemImage: "point.topleft.down.curvedto.point.bottomright.up"),
                NodeMenuItem(id: "siso-result", title: "SISO Behaviors", systemImage: "waveform.path.ecg"),
                NodeMenuItem(id: "qk-poly-result", title: "qK-space Polyhedron", systemImage: "view.3d"),
                NodeMenuItem(id: "scan-1d-result", title: "1D Scan Result", systemImage: "chart.xyaxis.line"),
                NodeMenuItem(id: "scan-2d-result", title: "2D Scan Result", systemImage: "square.grid.3x3.square"),
                NodeMenuItem(id: "rop-cloud-result", title: "ROP Cloud Result", systemImage: "cloud"),
                NodeMenuItem(id: "fret-result", title: "FRET Result", systemImage: "camera.filters"),
                NodeMenuItem(id: "rop-poly-result", title: "ROP Polyhedron Result", systemImage: "cube"),
                NodeMenuItem(id: "atlas-query-result", title: "Atlas Query Result", systemImage: "scope")
            ]
        )
    ]

    private static let quickAddItems: [NodeMenuItem] = [
        NodeMenuItem(id: "siso-analysis", title: "SISO Analysis", systemImage: "waveform.path.ecg"),
        NodeMenuItem(id: "rop-cloud", title: "ROP Point Cloud", systemImage: "cloud"),
        NodeMenuItem(id: "fret-heatmap", title: "FRET Heatmap", systemImage: "camera.filters"),
        NodeMenuItem(id: "parameter-scan-1d", title: "Parameter Scan (1D)", systemImage: "chart.line.uptrend.xyaxis"),
        NodeMenuItem(id: "parameter-scan-2d", title: "Parameter Scan (2D)", systemImage: "square.grid.2x2"),
        NodeMenuItem(id: "rop-polyhedron", title: "ROP Polyhedron", systemImage: "hexagon"),
        NodeMenuItem(id: "atlas-workflow", title: "Atlas Workflow", systemImage: "map")
    ]

    init() {
        let store = ProjectStore()
        let backendController = BiocircuitsBackendController()
        let defaults = UserDefaults.standard
        let initialThemeMode =
            defaults.string(forKey: "biocircuitsExplorer.themeMode")
            ?? defaults.string(forKey: "ropExplorer.themeMode")
            ?? "auto"
        let webController = WebShellController(initialThemeMode: initialThemeMode)
        let initialProjectID = store.projects.first?.id

        webController.onProjectChange = { [weak store] projectID, document in
            do {
                try store?.updateDocument(document, for: projectID)
            } catch {
                store?.lastErrorMessage = error.localizedDescription
            }
        }

        _store = StateObject(wrappedValue: store)
        _backendController = StateObject(wrappedValue: backendController)
        _webController = StateObject(wrappedValue: webController)
        _selectedProjectIDs = State(initialValue: initialProjectID.map { Set([$0]) } ?? [])
        _activeProjectID = State(initialValue: initialProjectID)
    }

    var body: some View {
        NavigationStack {
            GeometryReader { proxy in
                let panelWidth = sidebarWidth(for: proxy.size.width)
                let panelHeight = max(360, proxy.size.height - sidebarTopInset - sidebarBottomInset)

                ZStack(alignment: .topLeading) {
                    detailContent
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .background(Color(nsColor: .windowBackgroundColor))

                    Color.black.opacity(isSidebarPresented ? (selectedProject == nil ? 0.06 : 0.12) : 0)
                        .ignoresSafeArea()
                        .allowsHitTesting(false)

                    sidebarOverlay(width: panelWidth, height: panelHeight)
                        .padding(.top, sidebarTopInset)
                        .padding(.leading, sidebarLeadingInset)
                        .padding(.bottom, sidebarBottomInset)
                        .offset(x: isSidebarPresented ? 0 : -(panelWidth + 32))
                        .opacity(isSidebarPresented ? 1 : 0.001)
                        .allowsHitTesting(isSidebarPresented)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .toolbar {
                ToolbarItem(placement: .navigation) {
                    Button {
                        toggleSidebar()
                    } label: {
                        Label(isSidebarPresented ? "Hide Projects" : "Show Projects", systemImage: "sidebar.left")
                    }
                }

                ToolbarItemGroup {
                    Button {
                        createProject()
                    } label: {
                        Label("New Project", systemImage: "plus")
                    }

                    Button {
                        isImporting = true
                    } label: {
                        Label("Import JSON", systemImage: "square.and.arrow.down")
                    }

                    Button {
                        duplicateSelection()
                    } label: {
                        Label("Duplicate", systemImage: "plus.square.on.square")
                    }
                    .disabled(selectedProjectIDs.isEmpty)

                    Button {
                        beginRename(singleSelectedProjectID)
                    } label: {
                        Label("Rename", systemImage: "pencil")
                    }
                    .disabled(singleSelectedProjectID == nil)

                    Button(role: .destructive) {
                        deleteSelection()
                    } label: {
                        Label("Delete", systemImage: "trash")
                    }
                    .disabled(selectedProjectIDs.isEmpty)
                }

                ToolbarItemGroup {
                    Button {
                        revealSelection()
                    } label: {
                        Label("Reveal in Finder", systemImage: "folder")
                    }
                    .disabled(selectedProjectIDs.isEmpty)

                    Button {
                        restartBackend()
                    } label: {
                        Label("Restart Backend", systemImage: "bolt.horizontal.circle")
                    }

                    Button {
                        webController.reloadShell()
                    } label: {
                        Label("Reload Shell", systemImage: "arrow.clockwise")
                    }
                    .disabled(!backendController.isReady)
                }

                ToolbarItemGroup {
                    Menu {
                        ForEach(Self.addNodeSections) { section in
                            Section(section.title) {
                                ForEach(section.items) { item in
                                    Button {
                                        webController.addNode(ofType: item.id)
                                    } label: {
                                        Label(item.title, systemImage: item.systemImage)
                                    }
                                }
                            }
                        }
                    } label: {
                        Label("Add Node", systemImage: "plus")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Menu {
                        ForEach(Self.quickAddItems) { item in
                            Button {
                                webController.addQuickAddWorkflow(item.id)
                            } label: {
                                Label(item.title, systemImage: item.systemImage)
                            }
                        }
                    } label: {
                        Label("Quick Add", systemImage: "square.stack.3d.up")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Button {
                        webController.runConnectedWorkspace()
                    } label: {
                        Label("Run Connected", systemImage: "play.circle")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Button {
                        webController.saveWorkspace()
                    } label: {
                        Label("Save Workspace", systemImage: "square.and.arrow.down")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Button {
                        webController.loadWorkspace()
                    } label: {
                        Label("Reload Workspace", systemImage: "arrow.counterclockwise")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Button {
                        webController.resetWorkspaceView()
                    } label: {
                        Label("Reset View", systemImage: "viewfinder")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Button {
                        webController.toggleDebugConsole()
                    } label: {
                        Label("Debug", systemImage: "waveform.path.ecg")
                    }
                    .disabled(!canUseEmbeddedWorkspaceControls)

                    Menu {
                        Button {
                            applyThemeMode("auto")
                        } label: {
                            appearanceMenuLabel("Follow System", isSelected: themeMode == "auto")
                        }

                        Button {
                            applyThemeMode("light")
                        } label: {
                            appearanceMenuLabel("Light", isSelected: themeMode == "light")
                        }

                        Button {
                            applyThemeMode("dark")
                        } label: {
                            appearanceMenuLabel("Dark", isSelected: themeMode == "dark")
                        }
                    } label: {
                        Label("Appearance", systemImage: appearanceToolbarSymbol)
                    }
                    .disabled(!backendController.isReady)
                }
            }
        }
        .navigationTitle(windowToolbarTitle)
        .sheet(isPresented: $isRenaming) {
            RenameProjectSheet(
                currentName: renameDraft,
                onCancel: { isRenaming = false },
                onSave: commitRename
            )
        }
        .fileImporter(
            isPresented: $isImporting,
            allowedContentTypes: [.json],
            allowsMultipleSelection: true,
            onCompletion: handleImport
        )
        .alert("Project Error", isPresented: Binding(
            get: { errorMessage != nil },
            set: { if !$0 { errorMessage = nil } }
        )) {
            Button("OK", role: .cancel) {
                errorMessage = nil
            }
        } message: {
            Text(errorMessage ?? "")
        }
        .task {
            if let launchError = store.lastErrorMessage {
                errorMessage = launchError
            }
            await startBackend()
        }
        .onChange(of: selectedProjectIDs) { oldValue, newValue in
            reconcileSelectionChange(from: oldValue, to: newValue)
            syncCurrentSelectionIntoWeb()
        }
        .onChange(of: backendController.isReady) { _, isReady in
            guard isReady else {
                return
            }

            webController.loadBackend(url: backendController.baseURL)
            syncCurrentSelectionIntoWeb()
        }
        .onChange(of: store.projects.map(\.id)) { _, ids in
            reconcileSelectionForAvailableProjects(ids)
        }
        .onChange(of: webController.lastErrorMessage) { _, newValue in
            if let newValue {
                errorMessage = newValue
            }
        }
        .onChange(of: webController.isReady) { _, isReady in
            guard isReady else {
                return
            }

            syncThemeModeToWeb(themeMode)
        }
    }

    private var selectedProjects: [ProjectStore.ProjectFile] {
        store.projects.filter { selectedProjectIDs.contains($0.id) }
    }

    private var orderedSelectedProjectIDs: [String] {
        selectedProjects.map(\.id)
    }

    private var singleSelectedProjectID: String? {
        let ids = orderedSelectedProjectIDs
        return ids.count == 1 ? ids[0] : nil
    }

    private var selectedProject: ProjectStore.ProjectFile? {
        if let activeProjectID, selectedProjectIDs.contains(activeProjectID) {
            return store.project(withID: activeProjectID)
        }

        return selectedProjects.first
    }

    private var canUseEmbeddedWorkspaceControls: Bool {
        backendController.isReady && webController.isReady && selectedProject != nil
    }

    private var windowToolbarTitle: String {
        "Biocircuits Explorer Node Edition"
    }

    private var appearanceToolbarSymbol: String {
        switch themeMode {
        case "light":
            return "sun.max"
        case "dark":
            return "moon.stars"
        default:
            return "circle.lefthalf.filled"
        }
    }

    @ViewBuilder
    private var detailContent: some View {
        if selectedProject != nil {
            Group {
                if backendController.isReady {
                    WebShellView(controller: webController)
                } else if let backendError = backendController.lastErrorMessage {
                    ContentUnavailableView(
                        "Backend Failed",
                        systemImage: "bolt.horizontal.circle",
                        description: Text(backendError)
                    )
                } else {
                    VStack(spacing: 12) {
                        ProgressView()
                        Text("Starting Biocircuits Explorer")
                            .font(.headline)
                        Text("If no compiled backend is available, the first Julia startup can take several minutes.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                }
            }
        } else {
            ContentUnavailableView(
                "Select a Project",
                systemImage: "sidebar.left",
                description: Text("Choose a workspace JSON file from the project drawer to load it into the embedded Biocircuits Explorer page.")
            )
        }
    }

    private func sidebarOverlay(width: CGFloat, height: CGFloat) -> some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                Text("Projects")
                    .font(.title3.weight(.semibold))

                Spacer()
            }
            .padding(.horizontal, 18)
            .padding(.top, 20)
            .padding(.bottom, 10)

            List(selection: $selectedProjectIDs) {
                ForEach(store.projects) { project in
                    ProjectRow(project: project)
                        .tag(project.id)
                        .contentShape(Rectangle())
                        .simultaneousGesture(
                            TapGesture().onEnded {
                                activeProjectID = project.id
                            }
                        )
                        .contextMenu {
                            let targetIDs = contextMenuTargetIDs(for: project.id)

                            Button("Duplicate") {
                                duplicate(projectIDs: targetIDs)
                            }
                            Button("Rename") {
                                beginRename(project.id)
                            }
                            .disabled(targetIDs.count != 1)
                            Divider()
                            Button("Reveal in Finder") {
                                reveal(projectIDs: targetIDs)
                            }
                            Button("Delete", role: .destructive) {
                                delete(projectIDs: targetIDs)
                            }
                        }
                }
            }
            .listStyle(.sidebar)
            .scrollContentBackground(.hidden)
            .background(Color.clear)
            .overlay {
                if store.projects.isEmpty {
                    ContentUnavailableView(
                        "No Projects",
                        systemImage: "doc.text",
                        description: Text("Create or import a workspace JSON file.")
                    )
                }
            }
        }
        .frame(width: width)
        .frame(height: height, alignment: .top)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 22, style: .continuous)
                .strokeBorder(.white.opacity(0.12))
        )
        .shadow(color: .black.opacity(0.16), radius: 24, x: 0, y: 14)
    }

    private func sidebarWidth(for totalWidth: CGFloat) -> CGFloat {
        min(360, max(280, totalWidth * 0.28))
    }

    private func toggleSidebar() {
        setSidebarPresented(!isSidebarPresented)
    }

    private func setSidebarPresented(_ isPresented: Bool) {
        withAnimation(.spring(response: 0.4, dampingFraction: 0.92, blendDuration: 0.2)) {
            isSidebarPresented = isPresented
        }
    }

    private func createProject() {
        do {
            let project = try store.createProject()
            selectedProjectIDs = [project.id]
            activeProjectID = project.id
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func duplicateSelection() {
        duplicate(projectIDs: orderedSelectedProjectIDs)
    }

    private func duplicate(_ projectID: String) {
        duplicate(projectIDs: [projectID])
    }

    private func duplicate(projectIDs: [String]) {
        guard !projectIDs.isEmpty else {
            return
        }

        do {
            var duplicatedIDs: [String] = []
            for projectID in projectIDs {
                let project = try store.duplicateProject(id: projectID)
                duplicatedIDs.append(project.id)
            }
            selectedProjectIDs = Set(duplicatedIDs)
            activeProjectID = duplicatedIDs.last
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func beginRename(_ projectID: String?) {
        guard let project = store.project(withID: projectID) else {
            return
        }

        renameDraft = project.name
        selectedProjectIDs = [project.id]
        activeProjectID = project.id
        isRenaming = true
    }

    private func commitRename(_ newName: String) {
        guard let selectedProjectID = singleSelectedProjectID else {
            isRenaming = false
            return
        }

        do {
            let renamed = try store.renameProject(id: selectedProjectID, to: newName)
            self.selectedProjectIDs = [renamed.id]
            self.activeProjectID = renamed.id
            isRenaming = false
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func handleImport(_ result: Result<[URL], Error>) {
        do {
            let urls = try result.get()
            guard !urls.isEmpty else {
                return
            }

            let imported = try store.importProjects(from: urls)
            let importedIDs = imported.map(\.id)
            selectedProjectIDs = Set(importedIDs)
            activeProjectID = importedIDs.last
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func deleteSelection() {
        delete(projectIDs: orderedSelectedProjectIDs)
    }

    private func delete(projectIDs: [String]) {
        let idsToDelete = orderedProjectIDs(from: Set(projectIDs))
        guard !idsToDelete.isEmpty else {
            return
        }

        do {
            let idsToDeleteSet = Set(idsToDelete)
            let remainingIDs = store.projects.map(\.id).filter { !idsToDeleteSet.contains($0) }
            for projectID in idsToDelete {
                try store.deleteProject(id: projectID)
            }
            if let nextID = remainingIDs.first {
                selectedProjectIDs = [nextID]
                activeProjectID = nextID
            } else {
                selectedProjectIDs = []
                activeProjectID = nil
            }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func syncCurrentSelectionIntoWeb() {
        guard let project = selectedProject else {
            return
        }

        webController.showProject(id: project.id, document: project.document)
    }

    private func startBackend() async {
        do {
            applyAppAppearance(themeMode)
            try await backendController.startIfNeeded()
            webController.loadBackend(url: backendController.baseURL)
            syncCurrentSelectionIntoWeb()
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func restartBackend() {
        Task {
            do {
                try await backendController.restart()
                webController.loadBackend(url: backendController.baseURL)
                syncCurrentSelectionIntoWeb()
            } catch {
                errorMessage = error.localizedDescription
            }
        }
    }

    private func revealSelection() {
        reveal(projectIDs: orderedSelectedProjectIDs)
    }

    private func reveal(projectIDs: [String]) {
        let urls = orderedProjectIDs(from: Set(projectIDs)).compactMap { id in
            store.project(withID: id)?.fileURL
        }
        guard !urls.isEmpty else {
            return
        }

        reveal(urls)
    }

    private func reveal(_ url: URL) {
        reveal([url])
    }

    private func reveal(_ urls: [URL]) {
        NSWorkspace.shared.activateFileViewerSelecting(urls)
    }

    private func orderedProjectIDs(from ids: Set<String>) -> [String] {
        store.projects.map(\.id).filter { ids.contains($0) }
    }

    private func applyThemeMode(_ mode: String) {
        guard themeMode != mode else {
            return
        }

        themeMode = mode
        let effectiveTheme = resolvedEffectiveTheme(for: mode)
        guard webController.isReady else {
            applyAppAppearance(mode)
            return
        }

        webController.setThemeMode(mode, effectiveThemeOverride: effectiveTheme) {
            applyAppAppearance(mode)
        }
    }

    private func syncThemeModeToWeb(_ mode: String) {
        DispatchQueue.main.async {
            webController.setThemeMode(mode, effectiveThemeOverride: resolvedEffectiveTheme(for: mode))
        }
    }

    private func applyAppAppearance(_ mode: String) {
        let appearance: NSAppearance?
        switch mode {
        case "light":
            appearance = NSAppearance(named: .aqua)
        case "dark":
            appearance = NSAppearance(named: .darkAqua)
        default:
            appearance = nil
        }

        NSApp.appearance = appearance
        NSApp.windows.forEach { window in
            window.appearance = appearance
            window.invalidateShadow()
            window.contentView?.needsDisplay = true
        }
    }

    private func resolvedEffectiveTheme(for mode: String) -> String {
        switch mode {
        case "light":
            return "light"
        case "dark":
            return "dark"
        default:
            return systemThemeMode()
        }
    }

    private func systemThemeMode() -> String {
        let interfaceStyle = UserDefaults.standard.string(forKey: "AppleInterfaceStyle")
        return interfaceStyle == "Dark" ? "dark" : "light"
    }

    @ViewBuilder
    private func appearanceMenuLabel(_ title: String, isSelected: Bool) -> some View {
        HStack(spacing: 10) {
            Image(systemName: isSelected ? "checkmark" : "circle")
                .foregroundStyle(isSelected ? Color.accentColor : Color.secondary)
            Text(title)
        }
    }

    private func contextMenuTargetIDs(for projectID: String) -> [String] {
        if selectedProjectIDs.contains(projectID) {
            return orderedSelectedProjectIDs
        }

        return [projectID]
    }

    private func reconcileSelectionChange(from oldValue: Set<String>, to newValue: Set<String>) {
        if newValue.isEmpty {
            activeProjectID = nil
            return
        }

        if newValue.count == 1 {
            activeProjectID = newValue.first
            return
        }

        if let activeProjectID, newValue.contains(activeProjectID) {
            return
        }

        let addedIDs = newValue.subtracting(oldValue)
        if addedIDs.count == 1 {
            activeProjectID = addedIDs.first
            return
        }

        activeProjectID = orderedProjectIDs(from: newValue).first
    }

    private func reconcileSelectionForAvailableProjects(_ availableIDs: [String]) {
        let availableIDSet = Set(availableIDs)
        selectedProjectIDs.formIntersection(availableIDSet)

        if let activeProjectID, !availableIDSet.contains(activeProjectID) {
            self.activeProjectID = nil
        }

        if selectedProjectIDs.isEmpty {
            if let firstID = availableIDs.first {
                selectedProjectIDs = [firstID]
                activeProjectID = firstID
            } else {
                activeProjectID = nil
            }
            return
        }

        if activeProjectID == nil || !selectedProjectIDs.contains(activeProjectID ?? "") {
            activeProjectID = orderedProjectIDs(from: selectedProjectIDs).first
        }
    }
}

private struct ProjectRow: View {
    let project: ProjectStore.ProjectFile

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(project.name)
                .font(.body.weight(.medium))
            Text(project.modifiedAt.formatted(date: .abbreviated, time: .shortened))
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }
}

private struct RenameProjectSheet: View {
    @Environment(\.dismiss) private var dismiss
    @State private var draft: String
    let onCancel: () -> Void
    let onSave: (String) -> Void

    init(currentName: String, onCancel: @escaping () -> Void, onSave: @escaping (String) -> Void) {
        _draft = State(initialValue: currentName)
        self.onCancel = onCancel
        self.onSave = onSave
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Rename Project")
                .font(.headline)

            TextField("Project Name", text: $draft)
                .textFieldStyle(.roundedBorder)

            HStack {
                Spacer()
                Button("Cancel") {
                    onCancel()
                    dismiss()
                }
                Button("Save") {
                    onSave(draft)
                    dismiss()
                }
                .keyboardShortcut(.defaultAction)
            }
        }
        .padding(20)
        .frame(width: 360)
    }
}

#Preview {
    ContentView()
}
