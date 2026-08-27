import AppKit
import Foundation
import SwiftUI

@MainActor
final class ApplicationShutdownGate {
    static let shared = ApplicationShutdownGate()

    private(set) var isCommitted = false
    private(set) var isPreparingTermination = false
    private(set) var isTransferringServiceOwnership = false

    func beginTerminationPreparation() {
        guard !isCommitted else {
            return
        }
        isPreparingTermination = true
    }

    func cancelTerminationPreparation() {
        guard !isCommitted else {
            return
        }
        isPreparingTermination = false
    }

    func commit() {
        isCommitted = true
        isPreparingTermination = false
        isTransferringServiceOwnership = false
    }

    func setServiceOwnershipTransferInProgress(_ inProgress: Bool) {
        guard !isCommitted else {
            return
        }
        isTransferringServiceOwnership = inProgress
    }

    func requireLaunchAllowed(
        allowDuringTerminationPreparation: Bool = false
    ) throws {
        guard
            !isCommitted,
            !isTransferringServiceOwnership,
            allowDuringTerminationPreparation || !isPreparingTermination
        else {
            throw CancellationError()
        }
    }

    func waitUntilLaunchAllowed() async throws {
        while
            (isPreparingTermination || isTransferringServiceOwnership),
            !isCommitted
        {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(50))
        }
        try requireLaunchAllowed()
    }
}

@MainActor
final class AppTerminationCoordinator {
    typealias Preparation = @MainActor (CommitLease) async -> Bool

    enum CommitLeaseError: LocalizedError, Equatable {
        case staleRegistration

        var errorDescription: String? {
            "Quit preparation was replaced before its irreversible shutdown commit."
        }
    }

    struct SessionToken: Hashable, Sendable {
        fileprivate let sequence: UInt64
        fileprivate let value: UUID
    }

    struct RegistrationToken: Hashable, Sendable {
        fileprivate let session: SessionToken
        fileprivate let value: UUID
    }

    struct ManagedCleanupToken: Hashable, Sendable {
        fileprivate let value: UUID
    }

    @MainActor
    final class CommitLease {
        private weak var coordinator: AppTerminationCoordinator?
        private let registrationToken: RegistrationToken
        private let authorizationRevision: UInt64

        fileprivate init(
            coordinator: AppTerminationCoordinator,
            registrationToken: RegistrationToken,
            authorizationRevision: UInt64
        ) {
            self.coordinator = coordinator
            self.registrationToken = registrationToken
            self.authorizationRevision = authorizationRevision
        }

        /// Validates the active registration and executes the irreversible
        /// body in the same MainActor turn. No replacement can interleave
        /// between authorization and the controller latch commits.
        func commit(_ body: () throws -> Void) throws {
            guard let coordinator else {
                throw CommitLeaseError.staleRegistration
            }
            try coordinator.commit(
                registrationToken,
                authorizationRevision: authorizationRevision,
                body: body
            )
        }
    }

    private struct Registration {
        let session: SessionToken
        let token: RegistrationToken
        let preparation: Preparation
    }

    private struct ManagedCleanup {
        let token: ManagedCleanupToken
        let operation: @MainActor () async -> Bool
    }

    private struct ManagedCleanupAttempt {
        let id: UUID
        let task: Task<Bool, Never>
    }

    private struct ManagedCleanupRun {
        let id: UUID
        let task: Task<Bool, Never>
    }

    private struct LaunchAdmissionAttempt {
        let id: UUID
        let task: Task<Bool, Never>
    }

    static let shared = AppTerminationCoordinator(applicationShutdownGate: .shared)

    private var registrations: [SessionToken: Registration] = [:]
    private var preparationTasks: [RegistrationToken: Task<Bool, Never>] = [:]
    private var managedCleanups: [ManagedCleanupToken: ManagedCleanup] = [:]
    private var managedCleanupTasks: [ManagedCleanupToken: ManagedCleanupAttempt] = [:]
    private var managedCleanupRuns: [ManagedCleanupToken: ManagedCleanupRun] = [:]
    private var successfulManagedCleanups: Set<ManagedCleanupToken> = []
    private var launchAdmissionTasks: [RegistrationToken: LaunchAdmissionAttempt] = [:]
    private var successfulPreparations: Set<RegistrationToken> = []
    private var supersededRegistrationsAwaitingRetirement: Set<RegistrationToken> = []
    private var nextSessionSequence: UInt64 = 0
    private var newestRegisteredSessionSequence: UInt64 = 0
    private var isPreparing = false
    private var irreversiblyCommittedRegistration: RegistrationToken?
    private var activeRegistrationRevision: UInt64 = 0

    private let applicationShutdownGate: ApplicationShutdownGate
    private let supersededRetirementTimeout: TimeInterval
    private let stableApprovalPause: (@MainActor () async -> Void)?

    var hasActiveManagedPreparation: Bool {
        activeRegistration != nil || !managedCleanups.isEmpty
    }

    init(
        applicationShutdownGate: ApplicationShutdownGate? = nil,
        supersededRetirementTimeout: TimeInterval = 3,
        stableApprovalPause: (@MainActor () async -> Void)? = nil
    ) {
        self.applicationShutdownGate = applicationShutdownGate ?? ApplicationShutdownGate()
        self.supersededRetirementTimeout = supersededRetirementTimeout
        self.stableApprovalPause = stableApprovalPause
    }

    func makeSessionToken() -> SessionToken {
        nextSessionSequence += 1
        return SessionToken(sequence: nextSessionSequence, value: UUID())
    }

    @discardableResult
    func registerPreparation(
        for session: SessionToken,
        _ preparation: @escaping Preparation
    ) -> RegistrationToken? {
        let priorActiveToken = activeRegistration?.token
        let token = RegistrationToken(session: session, value: UUID())
        guard session.sequence >= newestRegisteredSessionSequence else {
            // A delayed task from an already-replaced ContentView must never
            // become an invisible fallback after the newer view disappears.
            return nil
        }
        if session.sequence > newestRegisteredSessionSequence {
            newestRegisteredSessionSequence = session.sequence
            let supersededSessions = registrations.keys.filter {
                $0.sequence < session.sequence
            }
            for supersededSession in supersededSessions {
                guard let superseded = registrations[supersededSession] else {
                    continue
                }
                // A registered owner may already have launched direct
                // children even when no quit task is running. Keep every
                // older owner addressable until onDisappear retires it and
                // installs app-level cleanup; the newer owner cannot launch
                // or commit across this handoff tombstone.
                supersededRegistrationsAwaitingRetirement.insert(superseded.token)
            }
        }
        if let previous = registrations[session] {
            successfulPreparations.remove(previous.token)
        }
        registrations[session] = Registration(
            session: session,
            token: token,
            preparation: preparation
        )
        advanceActiveRegistrationRevision(ifChangedFrom: priorActiveToken)
        refreshServiceOwnershipTransferGate()
        return token
    }

    func waitUntilRegistrationMayLaunch(_ token: RegistrationToken) async -> Bool {
        if let existing = launchAdmissionTasks[token] {
            return await existing.task.value
        }
        let attemptID = UUID()
        let task = Task { @MainActor [weak self] in
            guard let self else {
                return false
            }
            return await self.driveRegistrationLaunchAdmission(token)
        }
        launchAdmissionTasks[token] = LaunchAdmissionAttempt(
            id: attemptID,
            task: task
        )
        let admitted = await task.value
        if launchAdmissionTasks[token]?.id == attemptID {
            launchAdmissionTasks[token] = nil
        }
        return admitted
    }

    private func driveRegistrationLaunchAdmission(
        _ token: RegistrationToken
    ) async -> Bool {
        while true {
            guard
                registrations[token.session]?.token == token,
                activeRegistration?.token == token
            else {
                return false
            }
            if supersededRegistrationsAwaitingRetirement.isEmpty,
               let cleanup = managedCleanups.values.first
            {
                // A disappeared owner keeps app-level ownership after a
                // failed TERM/KILL observation. A replacement mount is a
                // concrete retry opportunity: drive that same authoritative
                // cleanup before admitting either service instead of polling
                // forever until the user happens to request termination.
                guard await runManagedCleanup(cleanup) else {
                    return false
                }
                continue
            }
            if supersededRegistrationsAwaitingRetirement.isEmpty {
                return true
            }
            do {
                try Task.checkCancellation()
                try await Task.sleep(for: .milliseconds(50))
            } catch {
                return false
            }
        }
    }

    @discardableResult
    func unregisterPreparation(_ token: RegistrationToken) -> Bool {
        guard
            preparationTasks[token] == nil,
            registrations[token.session]?.token == token
        else {
            return false
        }

        let priorActiveToken = activeRegistration?.token
        registrations[token.session] = nil
        successfulPreparations.remove(token)
        finishSupersededRetirement(token)
        advanceActiveRegistrationRevision(ifChangedFrom: priorActiveToken)
        refreshServiceOwnershipTransferGate()
        return true
    }

    /// Removes a view registration immediately, even when its preparation is
    /// already running, and returns that task so disappearance cleanup can
    /// wait for rollback before touching the same controllers. Removing the
    /// registration advances commit authorization, making its lease stale.
    func retirePreparation(
        _ token: RegistrationToken
    ) -> Task<Bool, Never>? {
        guard registrations[token.session]?.token == token else {
            return nil
        }

        let priorActiveToken = activeRegistration?.token
        registrations[token.session] = nil
        successfulPreparations.remove(token)
        finishSupersededRetirement(token)
        advanceActiveRegistrationRevision(ifChangedFrom: priorActiveToken)
        refreshServiceOwnershipTransferGate()
        return preparationTasks[token]
    }

    @discardableResult
    func registerManagedCleanup(
        _ operation: @escaping @MainActor () async -> Bool
    ) -> ManagedCleanupToken {
        let token = ManagedCleanupToken(value: UUID())
        let cleanup = ManagedCleanup(token: token, operation: operation)
        managedCleanups[token] = cleanup
        activeRegistrationRevision &+= 1
        refreshServiceOwnershipTransferGate()
        startManagedCleanup(cleanup)
        return token
    }

    @discardableResult
    func unregisterManagedCleanup(_ token: ManagedCleanupToken) -> Bool {
        let removedActiveCleanup = managedCleanups.removeValue(forKey: token) != nil
        let removedSuccessfulCleanup = successfulManagedCleanups.remove(token) != nil
        guard removedActiveCleanup || removedSuccessfulCleanup else {
            return false
        }
        // A running task remains strongly owned in `managedCleanupTasks` until
        // it returns. Removing its registration makes that result superseded,
        // allowing a replacement cleanup to become authoritative without an
        // old completed-false entry poisoning every later quit attempt.
        // Consuming an already-successful token on owner remount is only
        // bookkeeping: the cleanup no longer owns a process, closure, or
        // transfer gate. Do not invalidate a CommitLease that was issued for
        // the unchanged active registration while that token was consumed.
        // Removing an authoritative cleanup still changes the set of owners
        // that a quit decision must observe and therefore advances revision.
        if removedActiveCleanup {
            activeRegistrationRevision &+= 1
        }
        refreshServiceOwnershipTransferGate()
        return true
    }

    /// Re-drives one disappeared owner's authoritative cleanup without
    /// creating a controller-local retry in parallel with Cmd-Q. Success is
    /// retained until that owner consumes/unregisters its token on remount.
    func retryManagedCleanupForOwner(_ token: ManagedCleanupToken) async -> Bool {
        if successfulManagedCleanups.contains(token) {
            return true
        }
        guard let cleanup = managedCleanups[token] else {
            return false
        }
        _ = await runManagedCleanup(cleanup)
        return successfulManagedCleanups.contains(token)
    }

    func runRegisteredPreparation() async -> Bool {
        applicationShutdownGate.beginTerminationPreparation()
        let succeeded = await runActivePreparationUntilStable()
        if succeeded {
            // A cleanup-only termination has no view-owned commit body. Close
            // the same app-wide tail window before returning approval.
            applicationShutdownGate.commit()
        } else {
            applicationShutdownGate.cancelTerminationPreparation()
        }
        return succeeded
    }

    func applicationShouldTerminate(_ application: NSApplication) -> NSApplication.TerminateReply {
        guard hasActiveManagedPreparation else {
            // AppKit will not call us back before honoring terminateNow. Close
            // the app-wide launch gate synchronously so a controller created
            // in the approval -> willTerminate tail cannot start a child.
            applicationShutdownGate.commit()
            return .terminateNow
        }
        guard !isPreparing else {
            return .terminateLater
        }

        isPreparing = true
        applicationShutdownGate.beginTerminationPreparation()
        Task { @MainActor [weak self, weak application] in
            guard let self else {
                application?.reply(toApplicationShouldTerminate: false)
                return
            }
            // A mounted replacement view can register while an older final
            // save is suspended. Never approve termination from that stale
            // result; prepare the latest registration until it remains active.
            let shouldTerminate = await self.runRegisteredPreparation()
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

        let lease = CommitLease(
            coordinator: self,
            registrationToken: registration.token,
            authorizationRevision: activeRegistrationRevision
        )
        let task = Task { @MainActor in
            await registration.preparation(lease)
        }
        preparationTasks[registration.token] = task
        let succeeded = await task.value
        preparationTasks[registration.token] = nil
        if succeeded {
            successfulPreparations.insert(registration.token)
        }
        return succeeded
    }

    private func runActivePreparationUntilStable() async -> Bool {
        if irreversiblyCommittedRegistration != nil {
            return true
        }
        while true {
            if !supersededRegistrationsAwaitingRetirement.isEmpty {
                guard await waitForSupersededRegistrationRetirement() else {
                    return false
                }
                continue
            }
            if let cleanup = managedCleanups.values.first {
                guard await runManagedCleanup(cleanup) else {
                    return false
                }
                continue
            }
            guard let registration = activeRegistration else {
                // This is the cleanup-only approval boundary. A test pause
                // models a view registration arriving after the last await;
                // recheck all app-level owners afterwards, then commit the
                // gate synchronously in the same MainActor turn.
                await stableApprovalPause?()
                guard
                    activeRegistration == nil,
                    managedCleanups.isEmpty,
                    supersededRegistrationsAwaitingRetirement.isEmpty
                else {
                    continue
                }
                applicationShutdownGate.commit()
                return true
            }
            let succeeded = await runPreparation(registration)
            if irreversiblyCommittedRegistration == registration.token {
                return true
            }
            guard let latest = activeRegistration else {
                if !managedCleanups.isEmpty {
                    continue
                }
                // Once a managed preparation has started, losing its active
                // replacement is not equivalent to the app having started
                // with no managed window. Preserve the actual result and fail
                // closed when the stale preparation rolled back.
                return succeeded
            }
            if latest.token != registration.token {
                continue
            }
            if !managedCleanups.isEmpty {
                continue
            }
            return succeeded
        }
    }

    private func runManagedCleanup(_ cleanup: ManagedCleanup) async -> Bool {
        if let existing = managedCleanupRuns[cleanup.token] {
            return await existing.task.value
        }
        let runID = UUID()
        let task = Task { @MainActor [weak self] in
            guard let self else {
                return false
            }
            return await self.driveManagedCleanup(cleanup)
        }
        managedCleanupRuns[cleanup.token] = ManagedCleanupRun(
            id: runID,
            task: task
        )
        let succeeded = await task.value
        if managedCleanupRuns[cleanup.token]?.id == runID {
            managedCleanupRuns[cleanup.token] = nil
        }
        return succeeded
    }

    private func driveManagedCleanup(_ cleanup: ManagedCleanup) async -> Bool {
        guard managedCleanups[cleanup.token] != nil else {
            return true
        }
        let joinedAutomaticAttempt = managedCleanupTasks[cleanup.token] != nil
        let attempt = managedCleanupTasks[cleanup.token] ?? startManagedCleanup(cleanup)
        let succeeded = await attempt.task.value
        let finished = finishManagedCleanup(
            cleanup.token,
            attemptID: attempt.id,
            succeeded: succeeded
        )
        guard
            !finished,
            joinedAutomaticAttempt,
            let authoritativeCleanup = managedCleanups[cleanup.token]
        else {
            return finished
        }

        // Cmd-Q may join the automatically-started disappearance attempt just
        // as it reports failure. That quit request itself is the next retry;
        // do not require an otherwise meaningless third user action merely to
        // create a fresh TERM/KILL observation.
        let retry = startManagedCleanup(authoritativeCleanup)
        let retrySucceeded = await retry.task.value
        return finishManagedCleanup(
            cleanup.token,
            attemptID: retry.id,
            succeeded: retrySucceeded
        )
    }

    /// Managed cleanup starts as soon as the disappearing owner hands it to
    /// the application. This is essential outside Cmd-Q: a replacement view
    /// is allowed to launch as soon as the old direct children are confirmed
    /// exited, without requiring a termination attempt to drive the cleanup.
    @discardableResult
    private func startManagedCleanup(
        _ cleanup: ManagedCleanup
    ) -> ManagedCleanupAttempt {
        if let existing = managedCleanupTasks[cleanup.token] {
            return existing
        }
        let attemptID = UUID()
        // A replacement cleanup can target the same controller instances as
        // an operation it supersedes. Keep application-level ownership
        // single-filed: the new attempt begins only after every older attempt
        // has returned, even when its registration was already retired.
        let precedingTasks = managedCleanupTasks.values.map(\.task)
        let task = Task { @MainActor in
            for precedingTask in precedingTasks {
                _ = await precedingTask.value
            }
            return await cleanup.operation()
        }
        let attempt = ManagedCleanupAttempt(id: attemptID, task: task)
        managedCleanupTasks[cleanup.token] = attempt
        Task { @MainActor [weak self] in
            let succeeded = await task.value
            _ = self?.finishManagedCleanup(
                cleanup.token,
                attemptID: attemptID,
                succeeded: succeeded
            )
        }
        return attempt
    }

    @discardableResult
    private func finishManagedCleanup(
        _ token: ManagedCleanupToken,
        attemptID: UUID,
        succeeded: Bool
    ) -> Bool {
        if managedCleanupTasks[token]?.id == attemptID {
            managedCleanupTasks[token] = nil
        }
        let remainsAuthoritative = managedCleanups[token] != nil
        if succeeded, remainsAuthoritative {
            managedCleanups[token] = nil
            successfulManagedCleanups.insert(token)
            activeRegistrationRevision &+= 1
            refreshServiceOwnershipTransferGate()
        }
        return succeeded || !remainsAuthoritative
    }

    private func waitForSupersededRegistrationRetirement() async -> Bool {
        guard !supersededRegistrationsAwaitingRetirement.isEmpty else {
            return true
        }
        let deadline = Date().addingTimeInterval(supersededRetirementTimeout)
        while
            !supersededRegistrationsAwaitingRetirement.isEmpty,
            Date() < deadline
        {
            do {
                try Task.checkCancellation()
                try await Task.sleep(for: .milliseconds(50))
            } catch {
                return false
            }
        }
        return supersededRegistrationsAwaitingRetirement.isEmpty
    }

    private func finishSupersededRetirement(_ token: RegistrationToken) {
        guard supersededRegistrationsAwaitingRetirement.remove(token) != nil else {
            return
        }
    }

    private func refreshServiceOwnershipTransferGate() {
        applicationShutdownGate.setServiceOwnershipTransferInProgress(
            !supersededRegistrationsAwaitingRetirement.isEmpty
                || !managedCleanups.isEmpty
        )
    }

    private func commit(
        _ registrationToken: RegistrationToken,
        authorizationRevision: UInt64,
        body: () throws -> Void
    ) throws {
        guard
            irreversiblyCommittedRegistration == nil,
            activeRegistrationRevision == authorizationRevision,
            activeRegistration?.token == registrationToken,
            managedCleanups.isEmpty,
            supersededRegistrationsAwaitingRetirement.isEmpty
        else {
            throw CommitLeaseError.staleRegistration
        }

        // Mark the atomic decision before running the synchronous body so a
        // registration queued immediately after this actor turn cannot make a
        // successfully latched app report termination failure.
        irreversiblyCommittedRegistration = registrationToken
        do {
            try body()
        } catch {
            irreversiblyCommittedRegistration = nil
            throw error
        }
    }

    private var activeRegistration: Registration? {
        registrations.values.max { lhs, rhs in
            lhs.session.sequence < rhs.session.sequence
        }
    }

    private func advanceActiveRegistrationRevision(
        ifChangedFrom priorToken: RegistrationToken?
    ) {
        guard activeRegistration?.token != priorToken else {
            return
        }
        activeRegistrationRevision &+= 1
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
