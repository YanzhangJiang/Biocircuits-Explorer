import SwiftUI

@main
struct BiocircuitsExplorerMacApp: App {
    @NSApplicationDelegateAdaptor(BiocircuitsExplorerAppDelegate.self) private var appDelegate

    var body: some Scene {
        // ProjectStore and the embedded WebShell form one coordinated editing
        // session. Until cross-window document revisions are implemented, a
        // single app window prevents two independent stores from silently
        // overwriting the same project JSON.
        Window("Biocircuits Explorer", id: "main") {
            ContentView()
        }
        .commands {
            CommandGroup(replacing: .newItem) { }
        }
    }
}
