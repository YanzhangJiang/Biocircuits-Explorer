import SwiftUI
import WebKit

struct WebShellView: NSViewRepresentable {
    @ObservedObject var controller: WebShellController

    func makeNSView(context: Context) -> WKWebView {
        controller.webView
    }

    func updateNSView(_ nsView: WKWebView, context: Context) {
    }
}
