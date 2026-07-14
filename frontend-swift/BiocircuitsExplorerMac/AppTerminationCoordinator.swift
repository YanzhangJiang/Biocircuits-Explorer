import AppKit
import Foundation
import SwiftUI

@MainActor
final class AppTerminationCoordinator {
    typealias Preparation = @MainActor () async -> Bool

    struct SessionToken: Hashable, Sendable {
        fileprivate let sequence: UInt64
        fileprivate let value: UUID
    }

    struct RegistrationToken: Hashable, Sendable {
        fileprivate let session: SessionToken
        fileprivate let value: UUID
    }

    private struct Registration {
        let session: SessionToken
        let token: RegistrationToken
        let preparation: Preparation
    }

    static let shared = AppTerminationCoordinator()

    private var registrations: [SessionToken: Registration] = [:]
    private var preparationTasks: [RegistrationToken: Task<Bool, Never>] = [:]
    private var successfulPreparations: Set<RegistrationToken> = []
    private var nextSessionSequence: UInt64 = 0
    private var isPreparing = false

    init() {}

    func makeSessionToken() -> SessionToken {
        nextSessionSequence += 1
        return SessionToken(sequence: nextSessionSequence, value: UUID())
    }

    @discardableResult
    func registerPreparation(
        for session: SessionToken,
        _ preparation: @escaping Preparation
    ) -> RegistrationToken {
        let token = RegistrationToken(session: session, value: UUID())
        if let previous = registrations[session] {
            successfulPreparations.remove(previous.token)
        }
        registrations[session] = Registration(
            session: session,
            token: token,
            preparation: preparation
        )
        return token
    }

    @discardableResult
    func unregisterPreparation(_ token: RegistrationToken) -> Bool {
        guard
            preparationTasks[token] == nil,
            registrations[token.session]?.token == token
        else {
            return false
        }

        registrations[token.session] = nil
        successfulPreparations.remove(token)
        return true
    }

    func runRegisteredPreparation() async -> Bool {
        guard let registration = activeRegistration else {
            return true
        }
        return await runPreparation(registration)
    }

    func prepareAndUnregister(_ token: RegistrationToken) async -> Bool {
        guard let registration = registrations[token.session], registration.token == token else {
            return true
        }

        let succeeded = await runPreparation(registration)
        if succeeded {
            _ = unregisterPreparation(token)
        }
        return succeeded
    }

    func applicationShouldTerminate(_ application: NSApplication) -> NSApplication.TerminateReply {
        guard let registration = activeRegistration else {
            return .terminateNow
        }
        guard !isPreparing else {
            return .terminateLater
        }

        isPreparing = true
        Task { @MainActor [weak self, weak application] in
            guard let self else {
                application?.reply(toApplicationShouldTerminate: false)
                return
            }
            let shouldTerminate = await self.runPreparation(registration)
            self.isPreparing = false
            application?.reply(toApplicationShouldTerminate: shouldTerminate)
        }
        return .terminateLater
    }

    private func runPreparation(_ registration: Registration) async -> Bool {
        if successfulPreparations.contains(registration.token) {
            return true
        }
        if let existing = preparationTasks[registration.token] {
            return await existing.value
        }

        let task = Task { @MainActor in
            await registration.preparation()
        }
        preparationTasks[registration.token] = task
        let succeeded = await task.value
        preparationTasks[registration.token] = nil
        if succeeded {
            successfulPreparations.insert(registration.token)
        }
        return succeeded
    }

    private var activeRegistration: Registration? {
        registrations.values.max { lhs, rhs in
            lhs.session.sequence < rhs.session.sequence
        }
    }
}

@MainActor
final class BiocircuitsExplorerAppDelegate: NSObject, NSApplicationDelegate {
    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        AppTerminationCoordinator.shared.applicationShouldTerminate(sender)
    }
}

/// Converts the standard macOS window-close action into application
/// termination. The window therefore stays visible while the app delegate's
/// asynchronous final-snapshot preparation runs; a failed save cancels the
/// termination without leaving the user with a closed, unrecoverable window.
struct ApplicationTerminationWindowBridge: NSViewRepresentable {
    @MainActor
    final class AttachmentView: NSView {
        weak var bridgeCoordinator: Coordinator?

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            bridgeCoordinator?.attach(to: window)
        }
    }

    @MainActor
    final class Coordinator: NSObject, NSWindowDelegate {
        private weak var window: NSWindow?
        private var previousDelegate: (any NSWindowDelegate)?
        private let requestTermination: @MainActor (NSWindow) -> Void

        init(
            requestTermination: @escaping @MainActor (NSWindow) -> Void = { window in
                NSApp.terminate(window)
            }
        ) {
            self.requestTermination = requestTermination
        }

        func attach(to window: NSWindow?) {
            guard self.window !== window else {
                return
            }
            detach()
            guard let window else {
                return
            }
            self.window = window
            previousDelegate = window.delegate
            window.delegate = self
        }

        func detach() {
            guard let window else {
                return
            }
            if window.delegate === self {
                window.delegate = previousDelegate
            }
            self.window = nil
            previousDelegate = nil
        }

        func windowShouldClose(_ sender: NSWindow) -> Bool {
            guard sender === window else {
                return previousDelegate?.windowShouldClose?(sender) ?? true
            }
            requestTermination(sender)
            return false
        }

        override func responds(to aSelector: Selector!) -> Bool {
            super.responds(to: aSelector)
                || (previousDelegate?.responds(to: aSelector) ?? false)
        }

        override func forwardingTarget(for aSelector: Selector!) -> Any? {
            if previousDelegate?.responds(to: aSelector) == true {
                return previousDelegate
            }
            return super.forwardingTarget(for: aSelector)
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> AttachmentView {
        let view = AttachmentView(frame: .zero)
        view.bridgeCoordinator = context.coordinator
        return view
    }

    func updateNSView(_ nsView: AttachmentView, context: Context) {
        nsView.bridgeCoordinator = context.coordinator
        context.coordinator.attach(to: nsView.window)
    }

    static func dismantleNSView(_ nsView: AttachmentView, coordinator: Coordinator) {
        nsView.bridgeCoordinator = nil
        coordinator.detach()
    }
}
