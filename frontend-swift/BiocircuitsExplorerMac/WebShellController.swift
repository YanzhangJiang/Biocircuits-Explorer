import Combine
import Foundation
import WebKit

@MainActor
final class WebShellController: NSObject, ObservableObject {
    @Published private(set) var isReady = false
    @Published var lastErrorMessage: String?

    let webView: WKWebView
    private let contentController: WKUserContentController

    var onProjectChange: ((String, WorkspaceDocument) -> Void)?

    private var currentProjectID: String?
    private var currentProjectDocument: WorkspaceDocument?
    private var pendingProject: PendingProject?
    private var currentURL: URL?
    private var isCapturingSnapshot = false
    private var isLoadingProject = false
    private var injectedThemeMode: String

    init(initialThemeMode: String = "auto") {
        injectedThemeMode = Self.normalizedThemeMode(initialThemeMode)
        let contentController = WKUserContentController()
        self.contentController = contentController
        let bridgeScript = WKUserScript(
            source: Self.bridgeScriptSource(initialThemeMode: injectedThemeMode),
            injectionTime: .atDocumentStart,
            forMainFrameOnly: true
        )
        contentController.addUserScript(bridgeScript)

        let configuration = WKWebViewConfiguration()
        configuration.userContentController = contentController

        webView = WKWebView(frame: .zero, configuration: configuration)
        super.init()

        webView.navigationDelegate = self
        contentController.add(self, name: Self.bridgeName)
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
        if currentProjectID == id {
            currentProjectDocument = document
        }

        guard isReady else {
            return
        }

        guard !isCapturingSnapshot, !isLoadingProject else {
            return
        }

        if let currentProjectID, currentProjectID != id {
            isCapturingSnapshot = true
            captureCurrentWorkspaceSnapshot { [weak self] documentSnapshot in
                guard let self else {
                    return
                }

                self.isCapturingSnapshot = false
                if let documentSnapshot, let loadedProjectID = self.currentProjectID {
                    self.currentProjectDocument = documentSnapshot
                    self.onProjectChange?(loadedProjectID, documentSnapshot)
                }

                self.pushPendingProject()
            }
            return
        }

        pushPendingProject()
    }

    func reloadShell() {
        isReady = false
        isCapturingSnapshot = false
        isLoadingProject = false
        if let currentURL {
            refreshBridgeScript()
            webView.load(URLRequest(url: currentURL))
        }
    }

    func loadBackend(url: URL) {
        if currentURL == url, webView.url == url {
            return
        }

        currentURL = url
        isReady = false
        isCapturingSnapshot = false
        isLoadingProject = false
        refreshBridgeScript()
        webView.load(URLRequest(url: url))
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
            evaluateNativeShellScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.setThemeMode?.(\(argument), \(effectiveArgument));") { _, _ in
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
            let data = try JSONEncoder().encode(pendingProject.document)
            let jsonString = String(decoding: data, as: UTF8.self)
            let argument = try javaScriptStringLiteral(for: jsonString)
            let projectID = try javaScriptStringLiteral(for: pendingProject.id)
            let script = "(window.BiocircuitsExplorerNativeShell || window.ROPNativeShell)?.loadProjectFromJSONString(\(argument), \(projectID));"

            webView.evaluateJavaScript(script) { [weak self] result, error in
                guard let self else {
                    return
                }

                self.isLoadingProject = false
                if let error {
                    self.lastErrorMessage = error.localizedDescription
                    return
                }

                if let loaded = result as? Bool, !loaded {
                    self.lastErrorMessage = "Failed to load the selected workspace."
                    return
                }

                self.currentProjectID = pendingProject.id
                self.currentProjectDocument = pendingProject.document
                if self.pendingProject == pendingProject {
                    self.pendingProject = nil
                }

                if self.pendingProject != nil {
                    self.pushPendingProject()
                }
            }
        } catch {
            isLoadingProject = false
            lastErrorMessage = error.localizedDescription
        }
    }

    private func handleMessage(_ body: [String: Any]) {
        guard let type = body["type"] as? String else {
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
            if
                let payload = body["payload"] as? [String: Any],
                let projectID = (payload["projectID"] as? String) ?? currentProjectID,
                let jsonString = payload["jsonString"] as? String,
                let document = decodeDocument(fromJSONString: jsonString)
            {
                if currentProjectID == projectID {
                    currentProjectDocument = document
                }
                onProjectChange?(projectID, document)
                return
            }

            guard
                let currentProjectID,
                let payload = body["payload"] as? String,
                let document = decodeDocument(fromJSONString: payload)
            else {
                return
            }

            currentProjectDocument = document
            onProjectChange?(currentProjectID, document)

        case "requestCurrentProject":
            if pendingProject == nil, let currentProjectID, let currentProjectDocument {
                pendingProject = PendingProject(id: currentProjectID, document: currentProjectDocument)
            }
            pushPendingProject()

        case "log":
            if let payload = body["payload"] as? String {
                lastErrorMessage = payload
            }

        default:
            break
        }
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

    private func captureCurrentWorkspaceSnapshot(completion: @escaping (WorkspaceDocument?) -> Void) {
        guard isReady else {
            completion(nil)
            return
        }

        webView.evaluateJavaScript("(window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.serializeWorkspace?.();") { [weak self] result, error in
            if let error {
                self?.lastErrorMessage = error.localizedDescription
                completion(nil)
                return
            }

            guard let jsonString = result as? String, !jsonString.isEmpty else {
                completion(nil)
                return
            }

            completion(self?.decodeDocument(fromJSONString: jsonString))
        }
    }

    private func evaluateNativeShellScript(_ script: String, completion: ((Any?, Error?) -> Void)? = nil) {
        guard isReady else {
            return
        }

        webView.evaluateJavaScript(script) { [weak self] result, error in
            if let error {
                self?.lastErrorMessage = error.localizedDescription
            }
            completion?(result, error)
        }
    }

    private func refreshBridgeScript() {
        contentController.removeAllUserScripts()
        let bridgeScript = WKUserScript(
            source: Self.bridgeScriptSource(initialThemeMode: injectedThemeMode),
            injectionTime: .atDocumentStart,
            forMainFrameOnly: true
        )
        contentController.addUserScript(bridgeScript)
    }
}

extension WebShellController: WKScriptMessageHandler {
    nonisolated func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        Task { @MainActor [weak self] in
            guard
                message.name == Self.bridgeName,
                let body = message.body as? [String: Any]
            else {
                return
            }

            self?.handleMessage(body)
        }
    }
}

extension WebShellController: WKNavigationDelegate {
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        lastErrorMessage = nil
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        lastErrorMessage = error.localizedDescription
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        lastErrorMessage = error.localizedDescription
    }
}

private extension WebShellController {
    static let bridgeName = "biocircuitsExplorerShell"
    static let supportedContractVersion = 1
    static let supportedWorkspaceVersion = 1
    static func normalizedThemeMode(_ mode: String) -> String {
        switch mode {
        case "light", "dark":
            return mode
        default:
            return "auto"
        }
    }

    static func bridgeScriptSource(initialThemeMode: String) -> String { #"""
    (() => {
      const nativeThemeMode = "\#(normalizedThemeMode(initialThemeMode))";
      const shellState = {
        ready: false,
        registered: false,
        suppressSync: false,
        lastSnapshot: '',
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
          html.biocircuits-native-shell[data-effective-theme="light"],
          html.biocircuits-native-shell[data-effective-theme="light"] body {
            background: #eef3f8 !important;
          }
          html.biocircuits-native-shell[data-effective-theme="dark"],
          html.biocircuits-native-shell[data-effective-theme="dark"] body {
            background: #1c1c1c !important;
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
        handler.postMessage({ type, payload });
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
        postToNative('projectChanged', {
          projectID: shellState.projectID,
          jsonString,
        });
      }

      const nativeShell = {
        loadProjectFromJSONString(jsonString, projectID = null) {
          const contract = currentContract();
          if (typeof contract?.applyWorkspaceFromJSONString !== 'function') return false;
          if (projectID) {
            shellState.projectID = projectID;
          }
          shellState.suppressSync = true;
          try {
            contract.applyWorkspaceFromJSONString(jsonString);
            shellState.lastSnapshot = contract.serializeWorkspace?.() || jsonString;
          } catch (error) {
            reportError(error?.message ?? String(error));
            return false;
          } finally {
            shellState.suppressSync = false;
          }
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
    """# }

    struct PendingProject: Equatable {
        let id: String
        let document: WorkspaceDocument
    }
}
