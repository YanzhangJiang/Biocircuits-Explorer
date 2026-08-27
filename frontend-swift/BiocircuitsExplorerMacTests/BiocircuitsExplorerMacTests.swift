//
//  BiocircuitsExplorerMacTests.swift
//  BiocircuitsExplorerMacTests
//
//  Created by jyzer resico on 3/16/26.
//

import AppKit
import Darwin
import Foundation
import Testing
import WebKit
@testable import BiocircuitsExplorerMac

struct BiocircuitsExplorerMacTests {

    private func workspaceFixtureData(named name: String) throws -> Data {
        let repositoryRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try Data(contentsOf: repositoryRoot
            .appendingPathComponent("tests/fixtures/workspace", isDirectory: true)
            .appendingPathComponent(name))
    }

    @MainActor
    @Test func terminationRegistrationRejectsStaleViewCleanup() async throws {
        let coordinator = AppTerminationCoordinator()
        var preparations: [String] = []
        let staleSession = coordinator.makeSessionToken()
        let currentSession = coordinator.makeSessionToken()

        let staleToken = coordinator.registerPreparation(for: staleSession) { _ in
            preparations.append("stale")
            return false
        }!
        let currentToken = coordinator.registerPreparation(for: currentSession) { _ in
            preparations.append("current")
            return true
        }!

        // A delayed callback from the older view is rejected permanently; it
        // cannot return as an invisible fallback after the current view goes.
        let delayedStaleToken = coordinator.registerPreparation(for: staleSession) { _ in
            preparations.append("delayed-stale")
            return false
        }

        #expect(delayedStaleToken == nil)
        #expect(coordinator.retirePreparation(staleToken) == nil)
        _ = coordinator.registerManagedCleanup { true }
        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparations == ["current"])

        #expect(coordinator.unregisterPreparation(currentToken))
        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparations == ["current"])

        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparations == ["current"])
    }

    @MainActor
    @Test func runningPreparationCannotBeUnregisteredWhileFinalSaveIsRunning() async throws {
        let coordinator = AppTerminationCoordinator()
        let session = coordinator.makeSessionToken()
        var saveStarted = false
        var finishSave: CheckedContinuation<Void, Never>?
        let token = coordinator.registerPreparation(for: session) { _ in
            saveStarted = true
            await withCheckedContinuation { continuation in
                finishSave = continuation
            }
            return true
        }!

        let preparation = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !saveStarted {
            await Task.yield()
        }

        #expect(!coordinator.unregisterPreparation(token))
        finishSave?.resume()
        #expect(await preparation.value)
        #expect(coordinator.unregisterPreparation(token))
        #expect(await coordinator.runRegisteredPreparation())
    }

    @MainActor
    @Test func suspendedDisappearanceCleanupDefersQuitUntilDirectChildExit() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        var cleanupStarted = false
        var finishDirectChildWait: CheckedContinuation<Void, Never>?

        _ = coordinator.registerManagedCleanup {
            cleanupStarted = true
            await withCheckedContinuation { continuation in
                finishDirectChildWait = continuation
            }
            return true
        }

        // No mounted-view registration exists. The app-level cleanup owner
        // must still take the terminateLater path and keep launches blocked.
        #expect(coordinator.hasActiveManagedPreparation)
        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !cleanupStarted {
            await Task.yield()
        }
        #expect(applicationGate.isPreparingTermination)
        #expect(!applicationGate.isCommitted)

        finishDirectChildWait?.resume()
        #expect(await termination.value)
        #expect(applicationGate.isCommitted)
        #expect(!applicationGate.isPreparingTermination)
    }

    @MainActor
    @Test func cleanupOnlyApprovalRechecksReplacementBeforeAtomicGateCommit() async throws {
        let applicationGate = ApplicationShutdownGate()
        var finalApprovalPaused = false
        var resumeFinalApproval: CheckedContinuation<Void, Never>?
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate,
            stableApprovalPause: {
                guard !finalApprovalPaused else {
                    return
                }
                finalApprovalPaused = true
                await withCheckedContinuation { continuation in
                    resumeFinalApproval = continuation
                }
            }
        )
        _ = coordinator.registerManagedCleanup { true }
        var replacementPrepared = false
        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !finalApprovalPaused {
            await Task.yield()
        }

        let replacementSession = coordinator.makeSessionToken()
        _ = coordinator.registerPreparation(for: replacementSession) { lease in
            replacementPrepared = true
            do {
                try lease.commit { }
                return true
            } catch {
                return false
            }
        }
        resumeFinalApproval?.resume()

        #expect(await termination.value)
        #expect(replacementPrepared)
        #expect(applicationGate.isCommitted)
    }

    @MainActor
    @Test func terminateNowWithoutManagedOwnerCommitsGlobalLaunchGateSynchronously() {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )

        #expect(
            coordinator.applicationShouldTerminate(NSApplication.shared)
                == .terminateNow
        )
        #expect(applicationGate.isCommitted)
        #expect(throws: CancellationError.self) {
            try applicationGate.requireLaunchAllowed()
        }
    }

    @MainActor
    @Test func disappearedInFlightRegistrationCannotReturnAsInvisibleFallback() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        var oldStarted = false
        var resumeOld: CheckedContinuation<Void, Never>?
        var events: [String] = []

        let oldToken = coordinator.registerPreparation(for: oldSession) { lease in
            oldStarted = true
            events.append("old-start")
            await withCheckedContinuation { continuation in
                resumeOld = continuation
            }
            do {
                try lease.commit { events.append("old-commit") }
                return true
            } catch {
                events.append("old-stale")
                return false
            }
        }!
        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !oldStarted {
            await Task.yield()
        }

        _ = coordinator.registerPreparation(for: replacementSession) { lease in
            events.append("replacement")
            try? lease.commit { events.append("replacement-commit") }
            return true
        }
        let retiredTask = coordinator.retirePreparation(oldToken)
        #expect(retiredTask != nil)
        _ = coordinator.registerManagedCleanup {
            _ = await retiredTask?.value
            events.append("old-cleanup")
            return true
        }
        resumeOld?.resume()

        #expect(await termination.value)
        #expect(events == [
            "old-start",
            "old-stale",
            "old-cleanup",
            "replacement",
            "replacement-commit",
        ])
        #expect(!events.contains("old-commit"))
    }

    @MainActor
    @Test func supersededFailedManagedCleanupCannotPoisonItsSuccessfulReplacement() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        var oldStarted = false
        var finishOld: CheckedContinuation<Void, Never>?
        var events: [String] = []

        let oldToken = coordinator.registerManagedCleanup {
            oldStarted = true
            events.append("old-start")
            await withCheckedContinuation { continuation in
                finishOld = continuation
            }
            events.append("old-failed")
            return false
        }
        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !oldStarted {
            await Task.yield()
        }

        #expect(coordinator.unregisterManagedCleanup(oldToken))
        _ = coordinator.registerManagedCleanup {
            events.append("replacement-succeeded")
            return true
        }
        finishOld?.resume()

        #expect(await termination.value)
        #expect(events == ["old-start", "old-failed", "replacement-succeeded"])
        #expect(applicationGate.isCommitted)
    }

    @MainActor
    @Test func successfulManagedCleanupAutomaticallyUnblocksReplacementLaunch() async throws {
        let coordinator = AppTerminationCoordinator()
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        let oldToken = coordinator.registerPreparation(for: oldSession) { _ in true }!
        let replacementToken = coordinator.registerPreparation(for: replacementSession) { _ in true }!
        var cleanupStarted = false
        var finishCleanup: CheckedContinuation<Void, Never>?

        #expect(coordinator.retirePreparation(oldToken) == nil)
        _ = coordinator.registerManagedCleanup {
            cleanupStarted = true
            await withCheckedContinuation { continuation in
                finishCleanup = continuation
            }
            return true
        }

        var replacementWasAdmitted = false
        let replacementAdmission = Task { @MainActor in
            replacementWasAdmitted = await coordinator.waitUntilRegistrationMayLaunch(
                replacementToken
            )
        }
        while !cleanupStarted {
            await Task.yield()
        }
        await Task.yield()
        #expect(!replacementWasAdmitted)

        finishCleanup?.resume()
        await replacementAdmission.value
        #expect(replacementWasAdmitted)
    }

    @MainActor
    @Test func manualRestartCannotBypassSupersededOwnerCleanup() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        var mount = ContentMountLifecycle()
        let mountGeneration = mount.appear()
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        let oldToken = coordinator.registerPreparation(for: oldSession) { _ in true }!
        let replacementToken = coordinator.registerPreparation(for: replacementSession) { _ in true }!
        #expect(applicationGate.isTransferringServiceOwnership)
        var finishCleanup: CheckedContinuation<Void, Never>?
        var restartCount = 0

        #expect(coordinator.retirePreparation(oldToken) == nil)
        _ = coordinator.registerManagedCleanup {
            await withCheckedContinuation { continuation in
                finishCleanup = continuation
            }
            return true
        }

        let restart = Task { @MainActor in
            guard
                mount.admitsStart(generation: mountGeneration, taskIsCancelled: false),
                await coordinator.waitUntilRegistrationMayLaunch(replacementToken),
                mount.admitsStart(generation: mountGeneration, taskIsCancelled: false)
            else {
                return
            }
            restartCount += 1
        }
        while finishCleanup == nil {
            await Task.yield()
        }
        await Task.yield()
        #expect(restartCount == 0)

        finishCleanup?.resume()
        await restart.value
        #expect(restartCount == 1)
        #expect(!applicationGate.isTransferringServiceOwnership)
    }

    @MainActor
    @Test func failedManagedCleanupIsRetriedByLaterTerminationWithoutRemount() async throws {
        let coordinator = AppTerminationCoordinator()
        var attempts = 0
        _ = coordinator.registerManagedCleanup {
            attempts += 1
            return attempts >= 2
        }

        // Registration owns and starts the first disappearance attempt even
        // when no quit is in progress. Its failure remains authoritative.
        while attempts == 0 {
            await Task.yield()
        }
        #expect(attempts == 1)
        #expect(coordinator.hasActiveManagedPreparation)

        // A later Cmd-Q reruns the operation instead of reading a permanently
        // cached false result from the first Task.
        #expect(await coordinator.runRegisteredPreparation())
        #expect(attempts == 2)
        #expect(!coordinator.hasActiveManagedPreparation)
    }

    @MainActor
    @Test func replacementLaunchRetriesFailedManagedCleanupWithoutCmdQ() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        let oldToken = coordinator.registerPreparation(for: oldSession) { _ in true }!
        let replacementToken = coordinator.registerPreparation(
            for: replacementSession
        ) { _ in true }!
        var attempts = 0

        #expect(coordinator.retirePreparation(oldToken) == nil)
        _ = coordinator.registerManagedCleanup {
            attempts += 1
            return attempts >= 2
        }
        while attempts == 0 {
            await Task.yield()
        }
        #expect(attempts == 1)
        #expect(applicationGate.isTransferringServiceOwnership)

        // A replacement view is itself the retry trigger. It must not wait
        // indefinitely for an unrelated future Cmd-Q after the first cleanup
        // observation failed, and it still cannot launch before retry success.
        #expect(await coordinator.waitUntilRegistrationMayLaunch(replacementToken))
        #expect(attempts == 2)
        #expect(!applicationGate.isTransferringServiceOwnership)
    }

    @MainActor
    @Test func concurrentReplacementLaunchWaitersShareOneBoundedCleanupRetry() async throws {
        let coordinator = AppTerminationCoordinator()
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        let oldToken = coordinator.registerPreparation(for: oldSession) { _ in true }!
        let replacementToken = coordinator.registerPreparation(
            for: replacementSession
        ) { _ in true }!
        var attempts = 0
        var retryStarted = false
        var finishFailedRetry: CheckedContinuation<Void, Never>?

        #expect(coordinator.retirePreparation(oldToken) == nil)
        _ = coordinator.registerManagedCleanup {
            attempts += 1
            if attempts == 2 {
                retryStarted = true
                await withCheckedContinuation { continuation in
                    finishFailedRetry = continuation
                }
            }
            return attempts >= 3
        }
        while attempts == 0 {
            await Task.yield()
        }
        #expect(attempts == 1)

        let juliaAdmission = Task { @MainActor in
            await coordinator.waitUntilRegistrationMayLaunch(replacementToken)
        }
        while !retryStarted {
            await Task.yield()
        }
        let designAdmission = Task { @MainActor in
            await coordinator.waitUntilRegistrationMayLaunch(replacementToken)
        }
        await Task.yield()
        finishFailedRetry?.resume()

        // Both service tasks join one registration-level admission result.
        // A joiner must not silently create a third cleanup attempt and launch
        // only one service after the shared bounded retry failed.
        #expect(!(await juliaAdmission.value))
        #expect(!(await designAdmission.value))
        #expect(attempts == 2)

        // A later explicit admission (for example, the user's Restart) is a
        // new bounded retry opportunity and can observe eventual child exit.
        #expect(await coordinator.waitUntilRegistrationMayLaunch(replacementToken))
        #expect(attempts == 3)
    }

    @MainActor
    @Test func replacementLaunchAndCmdQShareOneBoundedCleanupRetry() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate
        )
        let oldSession = coordinator.makeSessionToken()
        let replacementSession = coordinator.makeSessionToken()
        let oldToken = coordinator.registerPreparation(for: oldSession) { _ in true }!
        let replacementToken = coordinator.registerPreparation(
            for: replacementSession
        ) { _ in true }!
        var attempts = 0
        var retryStarted = false
        var finishFailedRetry: CheckedContinuation<Void, Never>?

        #expect(coordinator.retirePreparation(oldToken) == nil)
        _ = coordinator.registerManagedCleanup {
            attempts += 1
            if attempts == 2 {
                retryStarted = true
                await withCheckedContinuation { continuation in
                    finishFailedRetry = continuation
                }
            }
            return attempts >= 3
        }
        while attempts == 0 {
            await Task.yield()
        }

        let replacementAdmission = Task { @MainActor in
            await coordinator.waitUntilRegistrationMayLaunch(replacementToken)
        }
        while !retryStarted {
            await Task.yield()
        }
        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        await Task.yield()
        finishFailedRetry?.resume()

        // Cmd-Q and mount admission join the same cleanup-run single flight.
        // Neither consumer may create a hidden extra retry that leaves the
        // other permanently failed after cleanup has already become clear.
        #expect(!(await replacementAdmission.value))
        #expect(!(await termination.value))
        #expect(attempts == 2)
        #expect(!applicationGate.isPreparingTermination)
        #expect(!applicationGate.isCommitted)

        #expect(await coordinator.waitUntilRegistrationMayLaunch(replacementToken))
        #expect(attempts == 3)
    }

    @MainActor
    @Test func ownerRemountRetryJoinsCoordinatorCleanupInsteadOfRunningInParallel() async throws {
        let coordinator = AppTerminationCoordinator()
        var attempts = 0
        var coordinatorRetryStarted = false
        var finishCoordinatorRetry: CheckedContinuation<Void, Never>?
        let cleanupToken = coordinator.registerManagedCleanup {
            attempts += 1
            if attempts == 2 {
                coordinatorRetryStarted = true
                await withCheckedContinuation { continuation in
                    finishCoordinatorRetry = continuation
                }
            }
            return attempts >= 2
        }
        while attempts == 0 {
            await Task.yield()
        }

        let quitRetry = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !coordinatorRetryStarted {
            await Task.yield()
        }
        let remountRetry = Task { @MainActor in
            await coordinator.retryManagedCleanupForOwner(cleanupToken)
        }
        await Task.yield()
        #expect(attempts == 2)
        finishCoordinatorRetry?.resume()

        #expect(await quitRetry.value)
        #expect(await remountRetry.value)
        #expect(attempts == 2)
        #expect(coordinator.unregisterManagedCleanup(cleanupToken))
    }

    @MainActor
    @Test func consumingSuccessfulCleanupDoesNotInvalidateActiveQuitLease() async throws {
        let coordinator = AppTerminationCoordinator()
        var cleanupAttempts = 0
        let cleanupToken = coordinator.registerManagedCleanup {
            cleanupAttempts += 1
            return true
        }
        while cleanupAttempts == 0 {
            await Task.yield()
        }
        #expect(await coordinator.retryManagedCleanupForOwner(cleanupToken))

        let session = coordinator.makeSessionToken()
        var preparationStarted = false
        var finishPreparation: CheckedContinuation<Void, Never>?
        var irreversibleCommitCount = 0
        _ = coordinator.registerPreparation(for: session) { lease in
            preparationStarted = true
            await withCheckedContinuation { continuation in
                finishPreparation = continuation
            }
            do {
                try lease.commit {
                    irreversibleCommitCount += 1
                }
                return true
            } catch {
                return false
            }
        }

        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !preparationStarted {
            await Task.yield()
        }

        // The remount consumes a token whose direct children are already
        // confirmed gone while the current owner's lease is suspended.
        // That bookkeeping must not make the otherwise-current lease stale.
        #expect(coordinator.unregisterManagedCleanup(cleanupToken))
        finishPreparation?.resume()

        #expect(await termination.value)
        #expect(irreversibleCommitCount == 1)
    }

    @MainActor
    @Test func juliaAndDesignMountTasksShareOneHighLevelCleanupAdmission() async throws {
        let singleFlight = ContentMountPreparationSingleFlight()
        var operationCount = 0
        var operationStarted = false
        var finishOperation: CheckedContinuation<Void, Never>?

        let julia = Task { @MainActor in
            await singleFlight.run(generation: 7) {
                operationCount += 1
                operationStarted = true
                await withCheckedContinuation { continuation in
                    finishOperation = continuation
                }
                return true
            }
        }
        while !operationStarted {
            await Task.yield()
        }
        let design = Task { @MainActor in
            await singleFlight.run(generation: 7) {
                operationCount += 1
                return false
            }
        }
        await Task.yield()
        #expect(operationCount == 1)
        finishOperation?.resume()

        #expect(await julia.value)
        #expect(await design.value)
        #expect(operationCount == 1)
    }

    @MainActor
    @Test func mountedManualRetryCanRedriveFailedHighLevelCleanupAdmission() async throws {
        let singleFlight = ContentMountPreparationSingleFlight()
        var retryPolicy = ContentMountCleanupRetryPolicy()
        var attempts = 0

        let claimedAutomaticRetry = retryPolicy.claimAutomaticRetry(for: 11)
        #expect(claimedAutomaticRetry)
        #expect(!(await singleFlight.run(generation: 11) {
            attempts += 1
            return false
        }))
        let sameMountRetryWasBlocked = !retryPolicy.claimAutomaticRetry(for: 11)
        #expect(sameMountRetryWasBlocked)
        retryPolicy.reopenForExplicitUserRetry(generation: 11)
        let claimedExplicitRetry = retryPolicy.claimAutomaticRetry(for: 11)
        #expect(claimedExplicitRetry)
        #expect(await singleFlight.run(generation: 11) {
            attempts += 1
            return true
        })
        #expect(attempts == 2)
    }

    @MainActor
    @Test func designStartupCannotPublishAcrossServiceOwnershipTransfer() async throws {
        let applicationGate = ApplicationShutdownGate()
        var startupPaused = false
        var resumeStartup: CheckedContinuation<Void, Never>?
        let design = DesignChatBackendController(
            port: 8_765,
            enginePort: 18_088,
            environment: [:],
            applicationShutdownGate: applicationGate,
            startupPause: {
                startupPaused = true
                await withCheckedContinuation { continuation in
                    resumeStartup = continuation
                }
            }
        )
        let startup = Task { @MainActor in
            await design.startIfNeeded()
        }
        while !startupPaused {
            await Task.yield()
        }

        // Model a replacement registration arriving after the initial wait
        // returned but before launch resolution/publication. The second gate
        // check must reject this startup in the same way for ordinary starts
        // and termination-recovery starts.
        applicationGate.setServiceOwnershipTransferInProgress(true)
        resumeStartup?.resume()
        await startup.value

        #expect(!design.isStarting)
        #expect(!design.isReady)
        #expect(design.bearerToken == nil)
        #expect(applicationGate.isTransferringServiceOwnership)
        #expect(throws: CancellationError.self) {
            try applicationGate.requireLaunchAllowed(
                allowDuringTerminationPreparation: true
            )
        }
    }

    @MainActor
    @Test func temporaryApplicationTerminationGateSuspendsReplacementLaunch() async throws {
        let applicationGate = ApplicationShutdownGate()
        var replacementReachedStartup = false
        let replacement = BiocircuitsBackendController(
            port: 19_988,
            environment: [:],
            applicationShutdownGate: applicationGate,
            startupPause: {
                replacementReachedStartup = true
                throw CancellationError()
            }
        )

        applicationGate.beginTerminationPreparation()
        let start = Task { @MainActor in
            try? await replacement.startIfNeeded()
        }
        try await Task.sleep(for: .milliseconds(120))
        #expect(!replacementReachedStartup)

        applicationGate.cancelTerminationPreparation()
        await start.value
        #expect(replacementReachedStartup)
    }

    @MainActor
    @Test func newerSessionRejectsDelayedOlderRegistrationBeforeItCanStart() async throws {
        let coordinator = AppTerminationCoordinator()
        let olderSession = coordinator.makeSessionToken()
        let newerSession = coordinator.makeSessionToken()
        var olderStartCount = 0

        let newerToken = coordinator.registerPreparation(for: newerSession) { _ in
            true
        }
        #expect(newerToken != nil)
        let delayedOlderToken = coordinator.registerPreparation(for: olderSession) { _ in
            false
        }
        if let delayedOlderToken,
           await coordinator.waitUntilRegistrationMayLaunch(delayedOlderToken)
        {
            olderStartCount += 1
        }

        #expect(delayedOlderToken == nil)
        #expect(olderStartCount == 0)
    }

    @MainActor
    @Test func missingOlderRetirementFailsBoundedlyAndReopensTemporaryLaunchGate() async throws {
        let applicationGate = ApplicationShutdownGate()
        let coordinator = AppTerminationCoordinator(
            applicationShutdownGate: applicationGate,
            supersededRetirementTimeout: 0.05
        )
        let olderSession = coordinator.makeSessionToken()
        let newerSession = coordinator.makeSessionToken()
        let olderToken = coordinator.registerPreparation(for: olderSession) { _ in
            true
        }!
        _ = coordinator.registerPreparation(for: newerSession) { _ in
            true
        }

        #expect(!(await coordinator.runRegisteredPreparation()))
        #expect(!applicationGate.isCommitted)
        #expect(!applicationGate.isPreparingTermination)

        // The tombstone remains retryable; a later real onDisappear can still
        // retire it and attach cleanup instead of the timeout forcing commit.
        #expect(coordinator.retirePreparation(olderToken) == nil)
    }

    @MainActor
    @Test func removedReplacementCannotReauthorizeAnOlderSuspendedCommitLease() async throws {
        let coordinator = AppTerminationCoordinator()
        let session = coordinator.makeSessionToken()
        var olderStarted = false
        var resumeOlder: CheckedContinuation<Void, Never>?
        var irreversibleCommitCount = 0

        _ = coordinator.registerPreparation(for: session) { lease in
            olderStarted = true
            await withCheckedContinuation { continuation in
                resumeOlder = continuation
            }
            do {
                try lease.commit {
                    irreversibleCommitCount += 1
                }
                return true
            } catch {
                return false
            }
        }

        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !olderStarted {
            await Task.yield()
        }

        let replacement = coordinator.registerPreparation(for: session) { lease in
            try? lease.commit { irreversibleCommitCount += 1 }
            return true
        }!
        #expect(coordinator.unregisterPreparation(replacement))
        resumeOlder?.resume()

        #expect(!(await termination.value))
        #expect(irreversibleCommitCount == 0)
    }

    @MainActor
    @Test func terminationPreparationRechecksAReplacementBeforeApprovingQuit() async throws {
        let coordinator = AppTerminationCoordinator()
        let session = coordinator.makeSessionToken()
        var events: [String] = []
        var firstPreparationStarted = false
        var finishFirstPreparation: CheckedContinuation<Void, Never>?

        _ = coordinator.registerPreparation(for: session) { _ in
            events.append("first-start")
            firstPreparationStarted = true
            await withCheckedContinuation { continuation in
                finishFirstPreparation = continuation
            }
            events.append("first-end")
            return false
        }

        let preparation = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !firstPreparationStarted {
            await Task.yield()
        }

        _ = coordinator.registerPreparation(for: session) { _ in
            events.append("replacement")
            return true
        }
        finishFirstPreparation?.resume()

        #expect(await preparation.value)
        #expect(events == ["first-start", "first-end", "replacement"])
    }

    @MainActor
    @Test func staleCommitLeaseCannotLatchControllersAndGlobalGateBlocksTailStart() async throws {
        let coordinator = AppTerminationCoordinator()
        let applicationGate = ApplicationShutdownGate()
        let oldBackend = BiocircuitsBackendController(
            port: 18_088,
            environment: [:],
            applicationShutdownGate: applicationGate
        )
        let oldDesign = DesignChatBackendController(
            port: 8_765,
            enginePort: 18_088,
            environment: [:],
            applicationShutdownGate: applicationGate
        )
        let replacementBackend = BiocircuitsBackendController(
            port: 18_089,
            environment: [:],
            applicationShutdownGate: applicationGate
        )
        let replacementDesign = DesignChatBackendController(
            port: 8_766,
            enginePort: 18_089,
            environment: [:],
            applicationShutdownGate: applicationGate
        )
        let session = coordinator.makeSessionToken()
        var oldReachedCommit = false
        var resumeOld: CheckedContinuation<Void, Never>?

        _ = coordinator.registerPreparation(for: session) { lease in
            oldBackend.beginTerminationShutdown()
            oldDesign.beginTerminationShutdown()
            _ = try? await oldBackend.stopAndWait()
            _ = try? await oldDesign.stopAndWait()
            oldReachedCommit = true
            await withCheckedContinuation { continuation in
                resumeOld = continuation
            }
            do {
                try lease.commit {
                    try oldBackend.commitTerminationShutdown()
                    try oldDesign.commitTerminationShutdown()
                    applicationGate.commit()
                }
                return true
            } catch {
                try? await oldDesign.recoverFromCancelledTermination()
                try? await oldBackend.recoverFromCancelledTermination()
                return false
            }
        }

        let termination = Task { @MainActor in
            await coordinator.runRegisteredPreparation()
        }
        while !oldReachedCommit {
            await Task.yield()
        }

        _ = coordinator.registerPreparation(for: session) { lease in
            replacementBackend.beginTerminationShutdown()
            replacementDesign.beginTerminationShutdown()
            _ = try? await replacementBackend.stopAndWait()
            _ = try? await replacementDesign.stopAndWait()
            do {
                try lease.commit {
                    try replacementBackend.commitTerminationShutdown()
                    try replacementDesign.commitTerminationShutdown()
                    applicationGate.commit()
                }
                return true
            } catch {
                return false
            }
        }
        resumeOld?.resume()

        #expect(await termination.value)
        #expect(!oldBackend.shutdownIsLatched)
        #expect(!oldDesign.shutdownIsLatched)
        #expect(!oldBackend.shutdownIsInProgress)
        #expect(!oldDesign.shutdownIsInProgress)
        #expect(replacementBackend.shutdownIsLatched)
        #expect(replacementDesign.shutdownIsLatched)
        #expect(applicationGate.isCommitted)

        let tailBackend = BiocircuitsBackendController(
            port: 18_090,
            environment: [:],
            applicationShutdownGate: applicationGate
        )
        do {
            try await tailBackend.startIfNeeded()
            Issue.record("A controller created after commit must share the app-wide launch gate")
        } catch {
            #expect(error is CancellationError)
        }
    }

    @MainActor
    @Test func finalWorkspaceCaptureLocksBeforeItsPauseAndUnlocksOnlyOnFailure() async throws {
        enum SyntheticStopError: Error {
            case failed
        }

        var workspaceIsLocked = false
        var revision = 1
        var persistedRevision: Int?
        var events: [String] = []
        var captureStarted = false
        var finishCapture: CheckedContinuation<Void, Never>?

        let successfulPreparation = Task { @MainActor in
            try await WorkspaceTerminationPreparation.run(
                lockWorkspace: {
                    events.append("lock")
                    workspaceIsLocked = true
                },
                captureAndPersist: {
                    events.append("capture-start")
                    captureStarted = true
                    await withCheckedContinuation { continuation in
                        finishCapture = continuation
                    }
                    persistedRevision = revision
                    events.append("capture-end")
                },
                stopProcesses: {
                    events.append("stop")
                    // Model an asynchronous JavaScript result that settles
                    // after the first snapshot while native UI stays blocked.
                    revision = 2
                },
                verifyFinalState: {
                    events.append("verify")
                    persistedRevision = revision
                },
                commitShutdown: {
                    events.append("commit")
                },
                restoreWorkspace: {
                    events.append("unlock")
                    workspaceIsLocked = false
                }
            )
        }

        while !captureStarted {
            await Task.yield()
        }
        // This models an edit attempted while WebKit's document is inert. The
        // persisted final snapshot must remain the last admitted revision.
        if !workspaceIsLocked {
            revision = 2
        }
        finishCapture?.resume()
        try await successfulPreparation.value

        #expect(persistedRevision == 2)
        #expect(workspaceIsLocked)
        #expect(events == [
            "lock", "capture-start", "capture-end", "stop", "verify", "commit",
        ])

        events = []
        do {
            try await WorkspaceTerminationPreparation.run(
                lockWorkspace: {
                    events.append("lock")
                    workspaceIsLocked = true
                },
                captureAndPersist: {
                    events.append("capture")
                },
                stopProcesses: {
                    events.append("stop")
                    throw SyntheticStopError.failed
                },
                verifyFinalState: {
                    events.append("verify")
                },
                commitShutdown: {
                    events.append("commit")
                },
                restoreWorkspace: {
                    events.append("unlock")
                    workspaceIsLocked = false
                }
            )
            Issue.record("A failed direct-child shutdown must cancel termination")
        } catch SyntheticStopError.failed {
            // Expected: the original shutdown failure survives successful UI recovery.
        }
        #expect(!workspaceIsLocked)
        #expect(events == ["lock", "capture", "stop", "unlock"])

        events = []
        do {
            try await WorkspaceTerminationPreparation.run(
                lockWorkspace: {
                    events.append("lock-partial")
                    workspaceIsLocked = true
                    throw SyntheticStopError.failed
                },
                captureAndPersist: {
                    events.append("capture")
                },
                stopProcesses: {
                    events.append("stop")
                },
                verifyFinalState: {
                    events.append("verify")
                },
                commitShutdown: {
                    events.append("commit")
                },
                restoreWorkspace: {
                    events.append("unlock")
                    workspaceIsLocked = false
                }
            )
            Issue.record("A partially-applied workspace lock must fail")
        } catch SyntheticStopError.failed { }
        #expect(!workspaceIsLocked)
        #expect(events == ["lock-partial", "unlock"])

        // A completion rejected by the atomic seal may still update transient
        // Web state. If termination is cancelled, recovery must capture that
        // actual state before unsealing interaction.
        events = []
        workspaceIsLocked = false
        revision = 3
        persistedRevision = nil
        do {
            try await WorkspaceTerminationPreparation.run(
                lockWorkspace: {
                    workspaceIsLocked = true
                    events.append("seal")
                },
                captureAndPersist: {
                    persistedRevision = revision
                    events.append("capture-sealed")
                },
                stopProcesses: {
                    events.append("stop")
                },
                verifyFinalState: {
                    persistedRevision = revision
                    events.append("verify-sealed")
                },
                commitShutdown: {
                    // Models a stale registration lease. The late result is
                    // outside the sealed final revision, then becomes actual
                    // recoverable state when quit is cancelled.
                    revision = 4
                    throw SyntheticStopError.failed
                },
                restoreWorkspace: {
                    persistedRevision = revision
                    events.append("recovery-capture")
                    workspaceIsLocked = false
                    events.append("unseal")
                }
            )
            Issue.record("The stale commit lease must cancel termination")
        } catch SyntheticStopError.failed { }
        #expect(persistedRevision == 4)
        #expect(!workspaceIsLocked)
        #expect(events == [
            "seal", "capture-sealed", "stop", "verify-sealed",
            "recovery-capture", "unseal",
        ])
    }

    @MainActor
    @Test func failedPartialWebShellUnlockRemainsRetryable() async throws {
        enum SyntheticUnlockError: Error {
            case failed
        }

        var nativeLockIsActive = true
        do {
            try await WebShellInteractionLockRecovery.restore(
                unlock: {
                    throw SyntheticUnlockError.failed
                },
                markRestored: {
                    nativeLockIsActive = false
                }
            )
            Issue.record("A failed WebKit unlock must remain visible to recovery")
        } catch SyntheticUnlockError.failed { }
        #expect(nativeLockIsActive)

        try await WebShellInteractionLockRecovery.restore(
            unlock: { },
            markRestored: {
                nativeLockIsActive = false
            }
        )
        #expect(!nativeLockIsActive)
    }

    @MainActor
    @Test func terminationFenceRejectsLatePublicationOutsideAtomicCut() async throws {
        var fence = TerminationWorkspaceFence()
        fence.establish(epoch: 7, revision: 12, isSealed: true)
        fence.recordPersistedSnapshot(epoch: 7, revision: 12)
        #expect(!fence.canCommit)
        fence.recordAuthoritativeWebDocumentDisposed()
        #expect(fence.canCommit)

        // A Promise settling after the seal is explicitly outside the final
        // revision. It is observable for diagnostics but cannot advance the
        // sealed/persisted revision or invalidate the atomic cut.
        fence.observeRejectedPublication(epoch: 7)
        #expect(fence.rejectedPublicationCount == 1)
        #expect(fence.sealedRevision == 12)
        #expect(fence.persistedRevision == 12)
        #expect(fence.canCommit)

        // Cancellation recovery creates a new seal from the actual current
        // document before Web interaction is released.
        fence.establish(epoch: 8, revision: 13, isSealed: true)
        fence.recordPersistedSnapshot(epoch: 8, revision: 13)
        fence.recordAuthoritativeWebDocumentDisposed()
        #expect(fence.rejectedPublicationCount == 0)
        #expect(fence.persistedRevision == 13)
        #expect(fence.canCommit)
    }

    @MainActor
    @Test func disposedFinalSealRejectsLateAIAutoSpawnAndSBMLImportMutations() async throws {
        var fence = TerminationWorkspaceFence()
        fence.establish(epoch: 41, revision: 9, isSealed: true)
        fence.recordPersistedSnapshot(epoch: 41, revision: 9)

        // Persisting alone is insufficient: both callbacks below are examples
        // of async owners that historically did not use state.js epochs.
        #expect(!fence.canCommit)
        fence.recordAuthoritativeWebDocumentDisposed()
        #expect(fence.canCommit)

        var authoritativeRevision = 9
        func tryLateMutation(owner: String) -> Bool {
            _ = owner
            guard !fence.authoritativeWebDocumentWasDisposed else {
                return false
            }
            authoritativeRevision += 1
            return true
        }

        #expect(!tryLateMutation(owner: "agent-node executeAgentNode autoSpawn/dispatch"))
        #expect(!tryLateMutation(owner: "sbml-io importSbml DOM rewrite/build"))
        #expect(authoritativeRevision == 9)
    }

    @MainActor
    @Test func transientDisappearanceCleanupDoesNotLatchAndRemountCanStartAgain() async throws {
        enum SyntheticStartupStop: Error {
            case reached
        }

        var backendStartAttempts = 0
        let backend = BiocircuitsBackendController(
            port: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate(),
            startupPause: {
                backendStartAttempts += 1
                throw SyntheticStartupStop.reached
            }
        )
        let design = DesignChatBackendController(
            port: 18_089,
            enginePort: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate()
        )

        #expect(try await backend.stopAndWaitForDisappearance() == .alreadyExited)
        #expect(try await design.stopAndWaitForDisappearance() == .alreadyExited)
        #expect(backend.disappearanceCleanupIsInProgress)
        #expect(design.disappearanceCleanupIsInProgress)
        #expect(!backend.shutdownIsInProgress)
        #expect(!backend.shutdownIsLatched)
        #expect(!design.shutdownIsInProgress)
        #expect(!design.shutdownIsLatched)

        do {
            try await backend.startIfNeeded()
            Issue.record("A start must not pass the disappearance cleanup gate")
        } catch is CancellationError { }
        do {
            try await backend.restart()
            Issue.record("A restart must not pass the disappearance cleanup gate")
        } catch is CancellationError { }
        #expect(backendStartAttempts == 0)

        for _ in 0..<2 {
            try backend.completeDisappearanceCleanupForRemount()
            try design.completeDisappearanceCleanupForRemount()
            do {
                try await backend.startIfNeeded()
                Issue.record("Expected the injected startup pause")
            } catch SyntheticStartupStop.reached { }
            #expect(try await backend.stopAndWaitForDisappearance() == .alreadyExited)
            #expect(try await design.stopAndWaitForDisappearance() == .alreadyExited)
        }
        #expect(backendStartAttempts == 2)
        #expect(!backend.shutdownIsLatched)
    }

    @MainActor
    @Test func retainedQuitGateCanBecomeSuccessfulDisappearanceCleanup() async throws {
        let backend = BiocircuitsBackendController(
            port: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate()
        )
        let design = DesignChatBackendController(
            port: 18_089,
            enginePort: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate()
        )

        // Model a failed quit recovery: the Process owner and the stronger
        // termination gate intentionally remain. onDisappear must accept that
        // barrier instead of caching a false gate error.
        backend.beginTerminationShutdown()
        design.beginTerminationShutdown()
        try backend.beginDisappearanceCleanup()
        try design.beginDisappearanceCleanup()

        #expect(try await design.stopAndWaitForRetiredTerminationOwner() == .alreadyExited)
        #expect(try await backend.stopAndWaitForRetiredTerminationOwner() == .alreadyExited)
        #expect(!backend.shutdownIsInProgress)
        #expect(!design.shutdownIsInProgress)
        #expect(backend.disappearanceCleanupIsInProgress)
        #expect(design.disappearanceCleanupIsInProgress)

        try backend.completeDisappearanceCleanupForRemount()
        try design.completeDisappearanceCleanupForRemount()
        #expect(!backend.disappearanceCleanupIsInProgress)
        #expect(!design.disappearanceCleanupIsInProgress)
    }

    @MainActor
    @Test func secondDisappearWhileCleanupWaitsCannotRegisterOrStartAfterCompletion() async throws {
        var mount = ContentMountLifecycle()
        let coordinator = AppTerminationCoordinator()
        let session = coordinator.makeSessionToken()
        var preparationCount = 0

        let firstMount = mount.appear()
        let firstRegistration = coordinator.registerPreparation(for: session) { _ in
            preparationCount += 1
            return true
        }!
        mount.disappear()
        #expect(coordinator.unregisterPreparation(firstRegistration))

        // Remount while the prior cleanup is suspended, then disappear again
        // before it completes. The stale task's captured generation must fail.
        let remount = mount.appear()
        let remountRegistration = coordinator.registerPreparation(for: session) { _ in
            preparationCount += 1
            return true
        }!
        mount.disappear()
        #expect(coordinator.unregisterPreparation(remountRegistration))
        #expect(!mount.admitsStart(generation: firstMount, taskIsCancelled: false))
        #expect(!mount.admitsStart(generation: remount, taskIsCancelled: false))
        var staleCallerRegistered = false
        #expect(!mount.performIfStartIsStillAdmitted(
            generation: remount,
            taskIsCancelled: false
        ) {
            staleCallerRegistered = true
        })
        #expect(!staleCallerRegistered)
        #expect(await coordinator.runRegisteredPreparation())
        #expect(preparationCount == 0)
    }

    @MainActor
    @Test func failedDisappearanceCleanupRetainsGateUntilLaterRetryConfirmsExit() async throws {
        var lifecycle = BackendLaunchLifecycle()
        var cleanupAttempts = 0

        let beganFirstCleanup = lifecycle.beginDisappearanceCleanup()
        #expect(beganFirstCleanup)
        cleanupAttempts += 1 // first TERM/KILL wait failed; retain ownership
        #expect(lifecycle.disappearanceCleanupIsInProgress)
        #expect(throws: CancellationError.self) {
            try lifecycle.requireLaunchAllowed()
        }

        // A later remount retries the same retained owner. Once that retry
        // observes the eventual exit it may explicitly reopen launches.
        let beganRetryCleanup = lifecycle.beginDisappearanceCleanup()
        #expect(beganRetryCleanup)
        cleanupAttempts += 1
        lifecycle.completeDisappearanceCleanup()
        #expect(cleanupAttempts == 2)
        #expect(!lifecycle.disappearanceCleanupIsInProgress)
        try lifecycle.requireLaunchAllowed()
    }

    @MainActor
    @Test func failedWebRetirementAlwaysReloadsInsteadOfUnlockingRevokedRealm() async throws {
        for failurePoint in ["nil navigation", "didFail", "timeout"] {
            _ = failurePoint
            var retirement = TerminationWorkspaceRetirement()
            var fence = TerminationWorkspaceFence()
            fence.establish(epoch: 3, revision: 5, isSealed: true)
            fence.recordPersistedSnapshot(epoch: 3, revision: 5)

            retirement.begin()
            #expect(retirement.requiresReloadOnRollback)
            #expect(!retirement.authoritativeDocumentWasDisposed)
            #expect(!fence.canCommit)

            // nil/didFail/timeout from the fresh recovery load retain the same
            // retry requirement; merely attempting a load is not restoration.
            #expect(retirement.requiresReloadOnRollback)
        }

        var successfulRecovery = TerminationWorkspaceRetirement()
        successfulRecovery.begin()
        successfulRecovery.resetAfterReloadOrNewPreparation()
        #expect(!successfulRecovery.requiresReloadOnRollback)
    }

    @Test func terminationRecoveryKeepsExternalProjectRestoreClosedUntilTrustedReload() {
        #expect(!WebShellController.pendingProjectRestoreIsAdmitted(
            terminationPreparationIsActive: true,
            recoveryGeneration: nil,
            currentGeneration: "new"
        ))
        #expect(!WebShellController.pendingProjectRestoreIsAdmitted(
            terminationPreparationIsActive: true,
            recoveryGeneration: "retired",
            currentGeneration: "new"
        ))
        #expect(WebShellController.pendingProjectRestoreIsAdmitted(
            terminationPreparationIsActive: true,
            recoveryGeneration: "new",
            currentGeneration: "new"
        ))
        #expect(WebShellController.pendingProjectRestoreIsAdmitted(
            terminationPreparationIsActive: false,
            recoveryGeneration: nil,
            currentGeneration: "ordinary"
        ))
    }

    @Test func deletingLastProjectStillAdmitsAtomicUnboundTerminationSeal() {
        #expect(WebShellController.terminationWorkspaceBindingIsAdmitted(
            currentProjectID: nil,
            hasCurrentProjectDocument: false,
            currentProjectIsApplied: false
        ))
        #expect(!WebShellController.terminationWorkspaceBindingIsAdmitted(
            currentProjectID: "deleted-project",
            hasCurrentProjectDocument: false,
            currentProjectIsApplied: false
        ))
        #expect(!WebShellController.terminationWorkspaceBindingIsAdmitted(
            currentProjectID: nil,
            hasCurrentProjectDocument: true,
            currentProjectIsApplied: false
        ))

        // The unbound state has no file write, but it is not allowed to skip
        // the producer fence: commit becomes possible only after the sealed
        // revision is recorded and the authoritative Web realm is disposed.
        var fence = TerminationWorkspaceFence()
        fence.establish(epoch: 8, revision: 13, isSealed: true)
        fence.recordPersistedSnapshot(epoch: 8, revision: 13)
        #expect(!fence.canCommit)
        fence.recordAuthoritativeWebDocumentDisposed()
        #expect(fence.canCommit)
    }

    @Test func cancelledRetirementReloadReplaysEveryNativeWebPreference() {
        var events: [String] = []
        WebShellPreferenceReplay.run(
            isReady: false,
            applyTheme: { events.append("theme") },
            applyCloudCompute: { events.append("cloud") },
            applySurface: { events.append("surface") },
            applyDesignEndpoint: { events.append("endpoint") }
        )
        #expect(events.isEmpty)

        WebShellPreferenceReplay.run(
            isReady: true,
            applyTheme: { events.append("theme") },
            applyCloudCompute: { events.append("cloud") },
            applySurface: { events.append("surface") },
            applyDesignEndpoint: { events.append("endpoint") }
        )
        #expect(events == ["theme", "cloud", "surface", "endpoint"])
    }

    @MainActor
    @Test func mainWindowCloseIsRedirectedThroughApplicationTermination() async throws {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 320, height: 240),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        var requestedWindow: NSWindow?
        let coordinator = ApplicationTerminationWindowBridge.Coordinator { closingWindow in
            requestedWindow = closingWindow
        }
        coordinator.attach(to: window)

        #expect(window.delegate === coordinator)
        #expect(!coordinator.windowShouldClose(window))
        #expect(requestedWindow === window)

        coordinator.detach()
        #expect(window.delegate == nil)
    }

    @Test func bundledDesignChatDiscoveryIncludesPackagedBackendTree() async throws {
        let resources = URL(fileURLWithPath: "/Applications/Biocircuits Explorer.app/Contents/Resources")
        let candidates = DesignChatBackendController
            .bundledChatScriptCandidates(resourceURL: resources)
            .map(\.path)

        #expect(candidates.first == URL(fileURLWithPath:
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/" +
            "BiocircuitsExplorerBackend/share/biocircuits-explorer/" +
            "webapp/scripts/chat_api.py"
        ).path)
        #expect(candidates.contains(
            resources.appendingPathComponent(
                "backend/share/biocircuits-explorer/webapp/scripts/chat_api.py"
            ).path
        ))

        let pythonCandidates = DesignChatBackendController
            .bundledPythonExecutableCandidates(resourceURL: resources)
            .map(\.path)
        #expect(pythonCandidates.first == URL(fileURLWithPath:
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/" +
            "BiocircuitsExplorerBackend/python/bin/python3"
        ).path)
        #expect(pythonCandidates.contains(
            resources.appendingPathComponent("backend/python/bin/python3").path
        ))

        let backendRoots = BiocircuitsBackendController.bundledBackendRootCandidates(
            bundleURL: URL(fileURLWithPath: "/Applications/Biocircuits Explorer.app"),
            resourceURL: resources
        ).map(\.path)
        #expect(backendRoots.first ==
            "/Applications/Biocircuits Explorer.app/Contents/Helpers/BiocircuitsExplorerBackend")
        #expect(backendRoots.contains(
            "/Applications/Biocircuits Explorer.app/Contents/Resources/backend"
        ))
    }

    @Test func backendReadinessProbeRejectsUnrelatedOrWarmingServers() async throws {
        let nonce = String(repeating: "a", count: 64)
        let ready = Data(#"{"status":"ready","service":"biocircuits-explorer-backend","instance_nonce":"\#(nonce)","checks":{"static_assets":true}}"#.utf8)
        let warming = Data(#"{"status":"not_ready","service":"biocircuits-explorer-backend","instance_nonce":"\#(nonce)"}"#.utf8)
        let missingIdentity = Data(#"{"status":"ready","checks":{"static_assets":true}}"#.utf8)
        let unrelated = Data("<html>another service</html>".utf8)

        #expect(BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: ready, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 503, body: warming, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: ready, expectedNonce: String(repeating: "b", count: 64)
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: missingIdentity, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 200, body: unrelated, expectedNonce: nonce
        ))
        #expect(!BiocircuitsBackendController.readinessProbeSucceeded(
            statusCode: 204, body: Data(), expectedNonce: nonce
        ))
    }

    @MainActor
    @Test func backendLaunchLifecycleRejectsOldStartupAndTerminationEvents() async throws {
        var lifecycle = BackendLaunchLifecycle()
        let firstLaunch = lifecycle.advance()
        #expect(lifecycle.accepts(firstLaunch))

        let replacementLaunch = lifecycle.advance()
        #expect(!lifecycle.accepts(firstLaunch))
        #expect(lifecycle.accepts(replacementLaunch))
        do {
            try lifecycle.requireCurrent(firstLaunch)
            Issue.record("A superseded backend launch must not return as successful")
        } catch {
            #expect(error is CancellationError)
        }
        try lifecycle.requireCurrent(replacementLaunch)

        let stoppedGeneration = lifecycle.advance()
        #expect(!lifecycle.accepts(replacementLaunch))
        #expect(lifecycle.accepts(stoppedGeneration))

        lifecycle.cancelShutdown()
        #expect(lifecycle.generation == stoppedGeneration)

        lifecycle.beginShutdown()
        #expect(lifecycle.shutdownIsInProgress)
        #expect(throws: CancellationError.self) {
            try lifecycle.requireLaunchAllowed()
        }
        lifecycle.cancelShutdown()
        #expect(!lifecycle.shutdownIsInProgress)
        try lifecycle.requireLaunchAllowed()

        lifecycle.beginShutdown()
        lifecycle.commitShutdown()
        #expect(lifecycle.shutdownIsLatched)
        #expect(throws: CancellationError.self) {
            try lifecycle.requireLaunchAllowed()
        }
    }

    @MainActor
    @Test func cancelledShutdownKeepsOwnedProcessOutputObservable() async throws {
        var lifecycle = BackendLaunchLifecycle()
        let launchGeneration = lifecycle.advance()
        let ownedPipe = Pipe()
        let replacementPipe = Pipe()

        lifecycle.beginShutdown()
        lifecycle.cancelShutdown()

        #expect(!lifecycle.accepts(launchGeneration))
        #expect(LocalProcessOutputOwnership.accepts(
            currentPipe: ownedPipe,
            sourcePipe: ownedPipe
        ))
        #expect(!LocalProcessOutputOwnership.accepts(
            currentPipe: replacementPipe,
            sourcePipe: ownedPipe
        ))
        #expect(!LocalProcessOutputOwnership.accepts(
            currentPipe: nil,
            sourcePipe: ownedPipe
        ))
    }

    @MainActor
    @Test func joinedBackendStartupCannotReportSuccessAfterShutdownInvalidatesOwner() async throws {
        var startupCount = 0
        var startupPaused = false
        var resumeStartup: CheckedContinuation<Void, Never>?
        let backend = BiocircuitsBackendController(
            port: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate(),
            startupPause: {
                startupCount += 1
                startupPaused = true
                await withCheckedContinuation { continuation in
                    resumeStartup = continuation
                }
            }
        )

        let owner = Task { @MainActor in
            do {
                try await backend.startIfNeeded()
                return false
            } catch {
                return error is CancellationError
            }
        }
        while !startupPaused {
            await Task.yield()
        }
        let joiner = Task { @MainActor in
            do {
                try await backend.startIfNeeded()
                return false
            } catch {
                return error is CancellationError
            }
        }
        await Task.yield()

        backend.beginTerminationShutdown()
        backend.stop()
        resumeStartup?.resume()

        #expect(await owner.value)
        #expect(await joiner.value)
        #expect(startupCount == 1)
        #expect(!backend.isReady)
    }

    @MainActor
    @Test func failedBackendRecoveryRetainsGateAndCanRetryIntent() async throws {
        enum SyntheticRecoveryError: Error {
            case failed
        }

        var startupCount = 0
        var initialStartupPaused = false
        var resumeInitialStartup: CheckedContinuation<Void, Never>?
        let backend = BiocircuitsBackendController(
            port: 18_088,
            environment: [:],
            applicationShutdownGate: ApplicationShutdownGate(),
            startupPause: {
                startupCount += 1
                if startupCount == 1 {
                    initialStartupPaused = true
                    await withCheckedContinuation { continuation in
                        resumeInitialStartup = continuation
                    }
                    return
                }
                throw SyntheticRecoveryError.failed
            }
        )

        let initialStartup = Task { @MainActor in
            try await backend.startIfNeeded()
        }
        while !initialStartupPaused {
            await Task.yield()
        }
        backend.beginTerminationShutdown()
        backend.stop()
        resumeInitialStartup?.resume()
        do {
            try await initialStartup.value
            Issue.record("The invalidated initial startup must be cancelled")
        } catch {
            #expect(error is CancellationError)
        }

        do {
            try await backend.recoverFromCancelledTermination()
            Issue.record("The injected first recovery must fail")
        } catch SyntheticRecoveryError.failed { }
        #expect(backend.shutdownIsInProgress)
        #expect(!backend.shutdownIsLatched)
        #expect(startupCount == 2)

        do {
            try await backend.startIfNeeded()
            Issue.record("Ordinary start must not pass an incomplete recovery gate")
        } catch {
            #expect(error is CancellationError)
        }
        #expect(startupCount == 2)

        do {
            try await backend.recoverFromCancelledTermination()
            Issue.record("The injected retry must still expose its failure")
        } catch SyntheticRecoveryError.failed { }
        #expect(startupCount == 3)
        #expect(backend.shutdownIsInProgress)
        #expect(!backend.shutdownIsLatched)

        // Once the owner view is retired, cleanup must continue through the
        // retained termination gate instead of trying (and failing) to begin a
        // normal disappearance cleanup. Confirmed exit converts the state to
        // a reversible disappearance gate for a possible later remount.
        #expect(try await backend.stopAndWaitForRetiredTerminationOwner() == .alreadyExited)
        #expect(!backend.shutdownIsInProgress)
        #expect(backend.disappearanceCleanupIsInProgress)
        try backend.completeDisappearanceCleanupForRemount()
        #expect(!backend.disappearanceCleanupIsInProgress)
    }

    @MainActor
    @Test func finalShutdownLatchBlocksBackendStartAndRestartPermanently() async throws {
        let backend = BiocircuitsBackendController(port: 18_088, environment: [:])
        backend.beginTerminationShutdown()
        #expect(try await backend.stopAndWait() == .alreadyExited)
        #expect(backend.shutdownIsInProgress)
        #expect(!backend.shutdownIsLatched)
        #expect(backend.terminationShutdownCanCommit)
        try backend.commitTerminationShutdown()
        #expect(backend.shutdownIsLatched)

        do {
            try await backend.startIfNeeded()
            Issue.record("A final-shutdown backend must never start again")
        } catch {
            #expect(error is CancellationError)
        }
        do {
            try await backend.restart()
            Issue.record("A final-shutdown backend must never restart")
        } catch {
            #expect(error is CancellationError)
        }

        let designBackend = DesignChatBackendController(
            port: 8_765,
            enginePort: 18_088,
            environment: [:]
        )
        designBackend.beginTerminationShutdown()
        #expect(try await designBackend.stopAndWait() == .alreadyExited)
        #expect(designBackend.shutdownIsInProgress)
        #expect(!designBackend.shutdownIsLatched)
        #expect(designBackend.terminationShutdownCanCommit)
        try designBackend.commitTerminationShutdown()
        #expect(designBackend.shutdownIsLatched)
        await designBackend.startIfNeeded()
        await designBackend.restart()
        #expect(designBackend.statusMessage == "Design backend shut down")
    }

    @MainActor
    @Test func cancelledTerminationClearsOnlyTheTemporaryLaunchGate() async throws {
        let backend = BiocircuitsBackendController(port: 18_088, environment: [:])
        backend.beginTerminationShutdown()
        do {
            try await backend.startIfNeeded()
            Issue.record("A temporary shutdown gate must block a delayed backend start")
        } catch {
            #expect(error is CancellationError)
        }
        #expect(try await backend.stopAndWait() == .alreadyExited)
        #expect(backend.shutdownIsInProgress)
        try await backend.recoverFromCancelledTermination()
        #expect(!backend.shutdownIsInProgress)
        #expect(!backend.shutdownIsLatched)

        let designBackend = DesignChatBackendController(
            port: 8_765,
            enginePort: 18_088,
            environment: [:]
        )
        designBackend.beginTerminationShutdown()
        await designBackend.startIfNeeded()
        #expect(designBackend.statusMessage == "Design backend shutting down")
        #expect(try await designBackend.stopAndWait() == .alreadyExited)
        #expect(designBackend.shutdownIsInProgress)
        try await designBackend.recoverFromCancelledTermination()
        #expect(!designBackend.shutdownIsInProgress)
        #expect(!designBackend.shutdownIsLatched)
    }

    @Test func forcedDirectChildShutdownReportsSignalFailureAndTimeout() async throws {
        func makeTermIgnoringProcess() throws -> Process {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/bin/sh")
            process.arguments = [
                "-c",
                "trap '' TERM; exec /usr/bin/tail -f /dev/null",
            ]
            try process.run()
            usleep(50_000)
            return process
        }

        let signalFailureProcess = try makeTermIgnoringProcess()
        defer {
            if signalFailureProcess.isRunning {
                _ = Darwin.kill(signalFailureProcess.processIdentifier, SIGKILL)
                signalFailureProcess.waitUntilExit()
            }
        }
        signalFailureProcess.terminate()
        do {
            _ = try await LocalProcessShutdown.waitForExitOrKill(
                signalFailureProcess,
                gracefulTimeout: 0.05,
                forcedTimeout: 0.05,
                sendSIGKILL: { _ in -1 }
            )
            Issue.record("A failed SIGKILL must not be reported as a stopped child")
        } catch let error as LocalProcessShutdown.Error {
            guard case .signalFailed = error else {
                Issue.record("Expected signalFailed, got \(error)")
                return
            }
        }
        #expect(signalFailureProcess.isRunning)

        let timeoutProcess = try makeTermIgnoringProcess()
        defer {
            if timeoutProcess.isRunning {
                _ = Darwin.kill(timeoutProcess.processIdentifier, SIGKILL)
                timeoutProcess.waitUntilExit()
            }
        }
        timeoutProcess.terminate()
        do {
            _ = try await LocalProcessShutdown.waitForExitOrKill(
                timeoutProcess,
                gracefulTimeout: 0.05,
                forcedTimeout: 0.05,
                sendSIGKILL: { _ in 0 }
            )
            Issue.record("A running child after the forced deadline must not report success")
        } catch let error as LocalProcessShutdown.Error {
            guard case .forcedTerminationTimedOut = error else {
                Issue.record("Expected forcedTerminationTimedOut, got \(error)")
                return
            }
        }
        #expect(timeoutProcess.isRunning)

        let forcedProcess = try makeTermIgnoringProcess()
        defer {
            if forcedProcess.isRunning {
                _ = Darwin.kill(forcedProcess.processIdentifier, SIGKILL)
                forcedProcess.waitUntilExit()
            }
        }
        forcedProcess.terminate()
        let forcedOutcome = try await LocalProcessShutdown.waitForExitOrKill(
            forcedProcess,
            gracefulTimeout: 0.05,
            forcedTimeout: 1
        )
        switch forcedOutcome {
        case .killed:
            break
        default:
            Issue.record("Expected the forced child to require SIGKILL")
        }
        #expect(!forcedProcess.isRunning)
    }

    @Test func aCleanBackendExitIsUnexpectedUnlessStopWasRequested() async throws {
        #expect(!BiocircuitsBackendController.processTerminationWasExpected(
            stopRequested: false,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(BiocircuitsBackendController.processTerminationWasExpected(
            stopRequested: true,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(!DesignChatBackendController.processTerminationWasExpected(
            stopRequested: false,
            terminationReason: .exit,
            terminationStatus: 0
        ))
        #expect(DesignChatBackendController.processTerminationWasExpected(
            stopRequested: true,
            terminationReason: .exit,
            terminationStatus: 0
        ))
    }

    @Test func designChatUsesTheJuliaBackendPortWithoutChangingItsOwnPort() async throws {
        #expect(DesignChatBackendController.resolveConfiguredEnginePort(from: [:]) == 18_088)
        #expect(DesignChatBackendController.resolveConfiguredEnginePort(
            from: ["BIOCIRCUITS_EXPLORER_PORT": "19001", "ROP_PORT": "19002"]
        ) == 19_001)

        let engineEnvironment = DesignChatBackendController.enginePortEnvironment(18_088)
        #expect(engineEnvironment["BIOCIRCUITS_EXPLORER_HOST"] == "127.0.0.1")
        #expect(engineEnvironment["BIOCIRCUITS_EXPLORER_PORT"] == "18088")
        #expect(engineEnvironment["ROP_HOST"] == "127.0.0.1")
        #expect(engineEnvironment["ROP_PORT"] == "18088")
        #expect(engineEnvironment["BNE_CHAT_PORT"] == nil)

        #expect(BiocircuitsBackendController.resolveConfiguredPort(
            from: ["BIOCIRCUITS_EXPLORER_PORT": "19001"]
        ) == 19_001)
        #expect(BiocircuitsBackendController.resolveConfiguredPort(from: [:]) == 18_088)
        #expect(DesignChatBackendController.resolveConfiguredPort(
            from: ["BNE_CHAT_PORT": "19002"],
            excluding: 19_001
        ) == 19_002)
    }

    @Test func nativeBackendKeepsStableCognitoOriginWhileChatUsesAnEphemeralPort() async throws {
        let enginePort = BiocircuitsBackendController.resolveConfiguredPort(from: [:])
        let chatPort = DesignChatBackendController.resolveConfiguredPort(
            from: [:],
            excluding: enginePort
        )
        #expect(enginePort == 18_088)
        #expect((1...65_535).contains(chatPort))
        #expect(chatPort != enginePort)
    }

    @Test func nativeDesignChatRotatesAndPropagatesItsBearerContract() async throws {
        let firstToken = DesignChatBackendController.makeBearerToken()
        let secondToken = DesignChatBackendController.makeBearerToken()

        #expect(firstToken.count == 64)
        #expect(firstToken.range(of: "^[0-9a-f]{64}$", options: .regularExpression) != nil)
        #expect(secondToken.count == 64)
        #expect(firstToken != secondToken)

        let securityEnvironment = DesignChatBackendController.nativeSecurityEnvironment(
            enginePort: 18_088,
            bearerToken: firstToken,
            instanceNonce: secondToken
        )
        #expect(securityEnvironment["BNE_CHAT_ALLOWED_ORIGIN"] == "http://127.0.0.1:18088")
        #expect(securityEnvironment["BNE_CHAT_BEARER_TOKEN"] == firstToken)
        #expect(securityEnvironment["BNE_CHAT_INSTANCE_NONCE"] == secondToken)
        #expect(securityEnvironment["BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK"] == "0")

        let request = DesignChatBackendController.authenticatedRequest(
            url: URL(string: "http://127.0.0.1:8765/health")!,
            bearerToken: firstToken,
            allowedOrigin: "http://127.0.0.1:18088"
        )
        #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer \(firstToken)")
        #expect(request.value(forHTTPHeaderField: "Origin") == "http://127.0.0.1:18088")

        let identityBody = Data(#"{"service":"biocircuits-design-chat","instance_nonce":"\#(secondToken)"}"#.utf8)
        let healthBody = Data(#"{"ok":true,"service":"biocircuits-design-chat","instance_nonce":"\#(secondToken)"}"#.utf8)
        #expect(DesignChatBackendController.identityProbeSucceeded(
            statusCode: 200,
            body: identityBody,
            expectedNonce: secondToken
        ))
        #expect(!DesignChatBackendController.identityProbeSucceeded(
            statusCode: 200,
            body: identityBody,
            expectedNonce: firstToken
        ))
        #expect(DesignChatBackendController.healthProbeSucceeded(
            statusCode: 200,
            body: healthBody,
            expectedNonce: secondToken
        ))
    }

    @Test func nativeRuntimeWritesStayInApplicationSupport() async throws {
        let appSupport = URL(fileURLWithPath: "/tmp/BiocircuitsExplorerTests/Application Support")
        let nonce = String(repeating: "c", count: 64)
        let backendEnvironment = BiocircuitsBackendController.runtimeStorageEnvironment(
            applicationSupportDirectory: appSupport,
            instanceNonce: nonce
        )
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_INSTANCE_NONCE"] == nonce)
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_JOB_STORE"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/Jobs")
        #expect(backendEnvironment["BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/Atlas")

        let designEnvironment = DesignChatBackendController.runtimeStorageEnvironment(
            applicationSupportDirectory: appSupport
        )
        #expect(designEnvironment["BNE_TRACE_DIR"] ==
            "/tmp/BiocircuitsExplorerTests/Application Support/Biocircuits Explorer/Runtime/DesignAgentTraces")
    }

    @Test func awsRuntimeFileCannotOverrideNativeBackendBootstrap() async throws {
        let runtimeEnvironment = [
            "AWS_REGION": "us-west-2",
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE": "trusted-queue",
            "BIOCIRCUITS_EXPLORER_IMAGE": "unrelated-to-the-native-runtime",
            "BIOCIRCUITS_EXPLORER_HOST": "0.0.0.0",
            "BIOCIRCUITS_EXPLORER_PORT": "9999",
            "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": "/tmp/attacker-public",
            "BIOCIRCUITS_EXPLORER_PARENT_PID": "1",
            "ROP_HOST": "0.0.0.0",
            "ROP_PORT": "9999",
            "ROP_PUBLIC_DIR": "/tmp/attacker-public",
            "ROP_PARENT_PID": "1",
            "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES": "1",
            "UNRELATED_OPERATOR_SETTING": "ignored",
        ]
        let bootstrapEnvironment = [
            "HOME": "/tmp/biocircuits-test-home",
            "BIOCIRCUITS_EXPLORER_HOST": "127.0.0.1",
            "BIOCIRCUITS_EXPLORER_PORT": "18088",
            "BIOCIRCUITS_EXPLORER_PUBLIC_DIR": "/safe/public",
            "BIOCIRCUITS_EXPLORER_PARENT_PID": "4242",
            "ROP_HOST": "127.0.0.1",
            "ROP_PORT": "18088",
            "ROP_PUBLIC_DIR": "/safe/public",
            "ROP_PARENT_PID": "4242",
        ]

        let secured = BiocircuitsBackendController.securedBackendEnvironment(
            runtimeEnvironment: runtimeEnvironment,
            bootstrapEnvironment: bootstrapEnvironment
        )

        for (key, expected) in bootstrapEnvironment {
            #expect(secured[key] == expected)
        }
        #expect(secured["AWS_REGION"] == "us-west-2")
        #expect(secured["BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE"] == "trusted-queue")
        #expect(secured["BIOCIRCUITS_EXPLORER_IMAGE"] == nil)
        #expect(secured["BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES"] == nil)
        #expect(secured["UNRELATED_OPERATOR_SETTING"] == nil)
    }

    @Test func workspaceDocumentNormalizesRequiredFields() async throws {
        let data = """
        {
          "custom": "kept",
          "nodes": []
        }
        """.data(using: .utf8)!

        let document = try JSONDecoder().decode(WorkspaceDocument.self, from: data)

        #expect(document.version == WorkspaceDocument.currentVersion)
        #expect(document.rawObject["schema_version"] == .string(WorkspaceDocument.currentSchemaVersion))
        #expect(document.rawObject["custom"] == JSONValue.string("kept"))
        #expect(document.rawObject["nodes"] == JSONValue.array([]))
        #expect(document.rawObject["connections"] == JSONValue.array([]))

        guard case let .object(canvas)? = document.rawObject["canvas"] else {
            Issue.record("Expected a normalized canvas object")
            return
        }

        #expect(canvas["panX"] == JSONValue.number(0))
        #expect(canvas["panY"] == JSONValue.number(0))
        #expect(canvas["scale"] == JSONValue.number(1))
    }

    @Test func workspaceV1MigrationMatchesTheSharedExpectedV2Fixture() throws {
        let source = try workspaceFixtureData(named: "valid-v1.json")
        let expectedData = try workspaceFixtureData(named: "valid-v1.expected-v2.json")
        let expected = try JSONDecoder().decode([String: JSONValue].self, from: expectedData)
        let migrated = try JSONDecoder().decode(WorkspaceDocument.self, from: source)

        #expect(migrated.rawObject == expected)
        #expect(migrated.version == WorkspaceDocument.currentVersion)
        #expect(migrated.rawObject["schema_version"] == .string(WorkspaceDocument.currentSchemaVersion))

        guard
            case let .array(nodes)? = migrated.rawObject["nodes"],
            case let .object(model)? = nodes.first(where: { value in
                value.objectValue?["id"] == .string("model")
            }),
            case let .object(modelData)? = model["data"],
            case let .object(lifecycle)? = modelData["lifecycle"]
        else {
            Issue.record("Expected the migrated model result and historical lifecycle")
            return
        }
        #expect(modelData["evidence"] == .object(["grade": .string("engine-computed")]))
        #expect(lifecycle["freshness"] == .string("historical"))
        #expect(lifecycle["evidence"] == modelData["evidence"])
        #expect(modelData["modelContext"]?.objectValue?["sessionId"] == nil)
        #expect(modelData["modelContext"]?.objectValue?["executionTicket"] == nil)
        #expect(modelData["modelContext"]?.objectValue?["ownerToken"] == nil)
    }

    @Test func workspaceV1StrictConfigMatrixKeepsOnlySevenSameFamilyConnections() throws {
        let source = try workspaceFixtureData(named: "strict-config-invalid-v1.json")
        let migrated = try JSONDecoder().decode(WorkspaceDocument.self, from: source)
        let connections = try #require(migrated.rawObject["connections"]?.arrayValue)

        #expect(connections.count == 7)
        let keys = Set(connections.compactMap { value -> String? in
            guard case let .object(connection) = value,
                  case let .string(fromNode)? = connection["fromNode"],
                  case let .string(toNode)? = connection["toNode"]
            else {
                return nil
            }
            return "\(fromNode):\(toNode)"
        })
        #expect(keys == [
            "siso-p:siso-r",
            "scan1-p:scan1-r",
            "scan2-p:scan2-r",
            "cloud-p:cloud-r",
            "fret-p:fret-r",
            "poly-p:poly-r",
            "placer-p:placer-r",
        ])
    }

    @Test func workspaceV2RequiresItsSchemaIdentityAndRejectsFutureVersions() throws {
        let missingSchema = #"{"version":2,"canvas":{},"nodes":[],"connections":[]}"#
        let wrongSchema = #"{"version":2,"schema_version":"bne-workspace/v3.0.0","canvas":{},"nodes":[],"connections":[]}"#
        for source in [missingSchema, wrongSchema] {
            #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
                _ = try JSONDecoder().decode(WorkspaceDocument.self, from: Data(source.utf8))
            }
        }

        let legacyV2 = #"{"version":2,"schema_version":"bne-workspace/v2.0.0","canvas":{},"nodes":[{"id":"legacy","type":"siso-analysis","data":{}}],"connections":[]}"#
        #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
            _ = try JSONDecoder().decode(WorkspaceDocument.self, from: Data(legacyV2.utf8))
        }

        let incompatibleV2 = #"{"version":2,"schema_version":"bne-workspace/v2.0.0","canvas":{},"nodes":[{"id":"source","type":"siso-params","data":{}},{"id":"result","type":"scan-1d-result","data":{}}],"connections":[{"fromNode":"source","fromPort":"params","toNode":"result","toPort":"params"}]}"#
        #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
            _ = try JSONDecoder().decode(WorkspaceDocument.self, from: Data(incompatibleV2.utf8))
        }

        #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
            _ = try JSONDecoder().decode(
                WorkspaceDocument.self,
                from: try workspaceFixtureData(named: "future-v3.json")
            )
        }
    }

    @Test func workspaceV2DowngradesCurrentLifecycleEvenWithoutSavedResultPayload() throws {
        let source = #"{"version":2,"schema_version":"bne-workspace/v2.0.0","canvas":{},"nodes":[{"id":"atlas","type":"atlas-builder","data":{"lifecycle":{"state":"current","freshness":"current","evidence":{"evidence_grade":"current-computation"}}}}],"connections":[]}"#
        let document = try JSONDecoder().decode(
            WorkspaceDocument.self,
            from: Data(source.utf8)
        )
        let nodes = try #require(document.rawObject["nodes"]?.arrayValue)
        let node = try #require(nodes.first?.objectValue)
        let data = try #require(node["data"]?.objectValue)
        let lifecycle = try #require(data["lifecycle"]?.objectValue)

        #expect(lifecycle["state"] == .string("historical"))
        #expect(lifecycle["freshness"] == .string("historical"))
        #expect(lifecycle["evidence"] == .object([
            "evidence_grade": .string("current-computation"),
        ]))
    }

    @Test func workspaceDocumentRejectsMalformedRequiredFields() throws {
        let malformedDocuments = [
            #"{"version":"1","nodes":[],"connections":[],"canvas":{}}"#,
            #"{"version":1,"nodes":{},"connections":[],"canvas":{}}"#,
            #"{"version":1,"nodes":[],"connections":{},"canvas":{}}"#,
            #"{"version":1,"nodes":[],"connections":[],"canvas":[]}"#,
        ]

        for json in malformedDocuments {
            #expect(throws: WorkspaceDocument.WorkspaceDocumentError.self) {
                _ = try JSONDecoder().decode(
                    WorkspaceDocument.self,
                    from: Data(json.utf8)
                )
            }
        }
    }

    @Test func workspaceDocumentPreservesUnknownFieldsDuringRoundTrip() async throws {
        let original = WorkspaceDocument(rawObject: [
            "version": JSONValue.number(2),
            "timestamp": JSONValue.string("2026-03-17T00:00:00Z"),
            "canvas": JSONValue.object([
                "panX": JSONValue.number(12),
                "panY": JSONValue.number(-4),
                "scale": JSONValue.number(1.5),
                "future": JSONValue.string("field"),
            ]),
            "nodes": JSONValue.array([
                JSONValue.object([
                    "id": JSONValue.string("node-1"),
                    "type": JSONValue.string("reaction-network"),
                    "extra": JSONValue.bool(true),
                ]),
            ]),
            "connections": JSONValue.array([]),
            "futureTopLevel": JSONValue.object([
                "flag": JSONValue.bool(true),
            ]),
        ])

        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(WorkspaceDocument.self, from: encoded)

        #expect(decoded.rawObject["futureTopLevel"] == JSONValue.object(["flag": JSONValue.bool(true)]))
        #expect(decoded.rawObject["schema_version"] == .string(WorkspaceDocument.currentSchemaVersion))
        guard
            case let .array(nodes)? = decoded.rawObject["nodes"],
            case let .object(firstNode) = nodes.first
        else {
            Issue.record("Expected a preserved node payload")
            return
        }

        #expect(firstNode["extra"] == JSONValue.bool(true))
    }

    @MainActor
    @Test func webShellBridgeLifecycleRejectsStaleNavigationWork() async throws {
        var lifecycle = WebShellBridgeLifecycle(generation: "navigation-1")

        #expect(lifecycle.accepts("navigation-1"))
        let initialApplyAccepted = lifecycle.markProjectApplied(for: "navigation-1")
        #expect(initialApplyAccepted)
        #expect(lifecycle.currentProjectIsApplied)
        #expect(lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: false,
            isCapturingSnapshot: false
        ))
        #expect(!lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: true,
            isCapturingSnapshot: false
        ))
        #expect(!lifecycle.shouldCaptureBeforeNavigation(
            isReady: true,
            isLoadingProject: false,
            isCapturingSnapshot: true
        ))

        lifecycle.beginNavigation(generation: "navigation-2")

        #expect(!lifecycle.accepts("navigation-1"))
        #expect(lifecycle.accepts("navigation-2"))
        #expect(!lifecycle.currentProjectIsApplied)
        let staleApplyAccepted = lifecycle.markProjectApplied(for: "navigation-1")
        #expect(!staleApplyAccepted)
        let currentApplyAccepted = lifecycle.markProjectApplied(for: "navigation-2")
        #expect(currentApplyAccepted)
        #expect(lifecycle.currentProjectIsApplied)
        lifecycle.markProjectUnapplied()
        #expect(!lifecycle.currentProjectIsApplied)
    }

    @MainActor
    @Test func webShellDoesNotPersistWebsiteDataAcrossAppSessions() async throws {
        let controller = WebShellController()
        #expect(!controller.webView.configuration.websiteDataStore.isPersistent)
    }

    @MainActor
    @Test func webShellNavigationQueueIsLatestWins() async throws {
        var queue = WebShellNavigationQueue()
        let firstURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        let latestURL = URL(string: "https://login.example.test/oauth2/authorize")!

        queue.enqueue(firstURL)
        queue.enqueue(latestURL, trust: .externalAuthentication)

        let selectedRequest = queue.takeLatestRequest()
        #expect(selectedRequest == WebShellNavigationRequest(
            url: latestURL,
            trust: .externalAuthentication
        ))
        #expect(queue.latestURL == nil)
        let noRemainingURL = queue.takeLatest()
        #expect(noRemainingURL == nil)
    }

    @MainActor
    @Test func webShellPersistenceQueuePreservesAutosaveAndFinalSnapshotOrder() async throws {
        let queue = WebShellPersistenceQueue()
        var events: [String] = []

        let autosave = queue.enqueue {
            events.append("autosave-start")
            try await Task.sleep(nanoseconds: 5_000_000)
            events.append("autosave-end")
        }
        let finalSnapshot = queue.enqueue {
            events.append("final-snapshot")
        }

        try await autosave.value.get()
        try await finalSnapshot.value.get()

        #expect(events == ["autosave-start", "autosave-end", "final-snapshot"])
    }

    @MainActor
    @Test func webShellOriginPolicyIsExactAndExternalAuthIsHTTPSOnly() async throws {
        guard let trustedOrigin = WebShellOrigin(
            url: URL(string: "http://127.0.0.1:18088/index-node.html")!
        ) else {
            Issue.record("Expected a valid local workspace origin")
            return
        }
        let authenticationOrigin = WebShellOrigin(
            url: URL(string: "https://login.example.test/oauth2/authorize")!
        )
        let policy = WebShellOriginPolicy(
            trustedOrigin: trustedOrigin,
            authenticationOrigin: authenticationOrigin
        )

        #expect(trustedOrigin.serialized == "http://127.0.0.1:18088")
        #expect(trustedOrigin.contains(
            URL(string: "http://127.0.0.1:18088/auth-callback.html?code=abc")!
        ))
        #expect(!trustedOrigin.contains(
            URL(string: "http://localhost:18088/index-node.html")!
        ))
        #expect(!trustedOrigin.contains(
            URL(string: "http://127.0.0.1:18089/index-node.html")!
        ))
        #expect(trustedOrigin.matches(
            protocol: "http",
            host: "127.0.0.1",
            port: 18088
        ))
        #expect(!trustedOrigin.matches(
            protocol: "http",
            host: "127.0.0.1",
            port: 18089
        ))

        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/index-node.html")!
        ) == .trustedWorkspace)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/auth-callback.html?code=abc")!
        ) == .localAuthenticationCallback)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/wiki.html")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "http://127.0.0.1:18088/")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "https://login.example.test/oauth2/authorize")!
        ) == .externalAuthentication)
        #expect(policy.disposition(
            for: URL(string: "https://phishing.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(WebShellOriginPolicy(trustedOrigin: trustedOrigin).disposition(
            for: URL(string: "https://login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "http://login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "https://user:password@login.example.test/oauth2/authorize")!
        ) == .blocked)
        #expect(policy.disposition(
            for: URL(string: "javascript:alert(1)")!
        ) == .blocked)
        #expect(WebShellController.allowsSubframeNavigation(
            disposition: .trustedWorkspace,
            navigationTrust: .trustedWorkspace
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: .trustedWorkspace
        ))
        #expect(WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: .externalAuthentication
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .trustedWorkspace,
            navigationTrust: .externalAuthentication
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .localAuthenticationCallback,
            navigationTrust: .localAuthenticationCallback
        ))
        #expect(!WebShellController.allowsSubframeNavigation(
            disposition: .externalAuthentication,
            navigationTrust: nil
        ))
        #expect(WebShellController.navigationTrust(for: .trustedWorkspace) == .trustedWorkspace)
        #expect(WebShellController.navigationTrust(
            for: .localAuthenticationCallback
        ) == .localAuthenticationCallback)
        #expect(WebShellController.navigationTrust(
            for: .externalAuthentication
        ) == .externalAuthentication)
        #expect(WebShellController.navigationTrust(for: .blocked) == nil)

        let enabledConfiguration = Data(
            #"{"enabled":true,"cognito_domain":"Login.Example.Test"}"#.utf8
        )
        #expect(WebShellController.authenticationOrigin(
            statusCode: 200,
            body: enabledConfiguration
        ) == authenticationOrigin)
        #expect(WebShellController.authenticationOrigin(
            statusCode: 503,
            body: enabledConfiguration
        ) == nil)
        for invalidBody in [
            Data(#"{"enabled":false,"cognito_domain":"login.example.test"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"evil.test/path"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"user@evil.test"}"#.utf8),
            Data(#"{"enabled":true,"cognito_domain":"evil.test:444"}"#.utf8),
        ] {
            #expect(WebShellController.authenticationOrigin(
                statusCode: 200,
                body: invalidBody
            ) == nil)
        }
        #expect(WebShellController.isExternalHTTPSURL(
            URL(string: "https://docs.example.test/guide")!
        ))
        #expect(!WebShellController.isExternalHTTPSURL(
            URL(string: "https://user:password@docs.example.test/guide")!
        ))
    }

    @MainActor
    @Test func webShellSnapshotCompletionIsBoundedAndExactlyOnce() async throws {
        var results: [WebShellSnapshotCaptureResult] = []
        let completion = WebShellSnapshotCaptureCompletion(generation: "navigation-1") {
            results.append($0)
        }

        #expect(completion.generation == "navigation-1")
        #expect(completion.resolve(.failed))
        #expect(!completion.resolve(.captured(WebShellCapturedProjectSnapshot(
            document: WorkspaceDocument(rawObject: ["revision": .number(2)]),
            sequence: 1
        ))))
        #expect(results == [.failed])
        #expect(WebShellController.snapshotCaptureTimeoutNanoseconds <= 3_000_000_000)
    }

    @MainActor
    @Test func webShellProjectChangeRequiresExactExplicitIdentity() async throws {
        let exactPayload: [String: Any] = ["projectID": "project-a"]
        let wrongPayload: [String: Any] = ["projectID": "project-b"]
        let nullPayload: [String: Any] = ["projectID": NSNull()]

        #expect(WebShellController.admittedProjectID(
            from: exactPayload,
            currentProjectID: "project-a"
        ) == "project-a")
        #expect(WebShellController.admittedProjectID(
            from: wrongPayload,
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.admittedProjectID(
            from: nullPayload,
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.admittedProjectID(
            from: [:],
            currentProjectID: "project-a"
        ) == nil)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 7]) == 7)
        #expect(WebShellController.snapshotSequence(from: ["sequence": NSNumber(value: 8)]) == 8)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 0]) == nil)
        #expect(WebShellController.snapshotSequence(from: ["sequence": 1.5]) == nil)
    }

    @MainActor
    @Test func webContentTerminationRecoveryTargetsCanonicalTrustedWorkspace() async throws {
        let workspaceURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        let callbackURL = URL(
            string: "http://127.0.0.1:18088/auth-callback.html?code=abc&state=xyz"
        )!
        let logoutReturnURL = URL(string: "http://127.0.0.1:18088/")!

        #expect(WebShellController.webContentRecoveryRequest(for: workspaceURL)
            == WebShellNavigationRequest(url: workspaceURL, trust: .trustedWorkspace))
        #expect(WebShellController.webContentRecoveryRequest(for: nil) == nil)
        #expect(WebShellController.trustedReturnURL(
            navigationURL: callbackURL,
            canonicalWorkspaceURL: workspaceURL
        ) == callbackURL)
        #expect(WebShellController.trustedReturnURL(
            navigationURL: logoutReturnURL,
            canonicalWorkspaceURL: workspaceURL
        ) == workspaceURL)
    }

    @MainActor
    @Test func failedWorkspaceCaptureDiscardsQueuedNavigation() async throws {
        var queue = WebShellNavigationQueue()
        let reloadURL = URL(string: "http://127.0.0.1:18088/index-node.html")!
        queue.enqueue(reloadURL)

        let failedCaptureMayNavigate = queue.allowNavigation(after: .failed)
        #expect(!failedCaptureMayNavigate)
        #expect(queue.latestURL == nil)

        queue.enqueue(reloadURL)
        let document = WorkspaceDocument(rawObject: ["name": .string("captured")])
        let successfulCaptureMayNavigate = queue.allowNavigation(after: .captured(
            WebShellCapturedProjectSnapshot(document: document, sequence: 1)
        ))
        #expect(successfulCaptureMayNavigate)
        #expect(queue.latestURL == reloadURL)
    }

    @MainActor
    @Test func webShellNavigationRequeuesCurrentProjectWithoutOverwritingNewerPendingWork() async throws {
        let currentDocument = WorkspaceDocument(rawObject: ["name": .string("current")])
        let pendingDocument = WorkspaceDocument(rawObject: ["name": .string("pending")])
        let pendingProject = WebShellController.PendingProject(
            id: "project-newer",
            document: pendingDocument
        )

        #expect(WebShellController.projectToReapply(
            pendingProject: nil,
            currentProjectID: "project-current",
            currentProjectDocument: currentDocument
        ) == WebShellController.PendingProject(id: "project-current", document: currentDocument))
        #expect(WebShellController.projectToReapply(
            pendingProject: pendingProject,
            currentProjectID: "project-current",
            currentProjectDocument: currentDocument
        ) == pendingProject)

        #expect(!WebShellController.hasNewerPendingProject(
            than: pendingProject,
            pendingProject: pendingProject
        ))
        #expect(WebShellController.hasNewerPendingProject(
            than: WebShellController.PendingProject(id: "project-old", document: currentDocument),
            pendingProject: pendingProject
        ))
    }

    @MainActor
    @Test func returningToCapturedProjectDoesNotReapplyItsStaleStoreCopy() async throws {
        let staleDocument = WorkspaceDocument(rawObject: ["revision": .number(1)])
        let otherDocument = WorkspaceDocument(rawObject: ["revision": .number(2)])

        let returnToCurrent = WebShellController.PendingProject(
            id: "project-a",
            document: staleDocument
        )
        #expect(WebShellController.pendingProjectAfterCapture(
            pendingProject: returnToCurrent,
            capturedProjectID: "project-a"
        ) == nil)

        let switchToOther = WebShellController.PendingProject(
            id: "project-b",
            document: otherDocument
        )
        #expect(WebShellController.pendingProjectAfterCapture(
            pendingProject: switchToOther,
            capturedProjectID: "project-a"
        ) == switchToOther)

        let renamedProject = WebShellController.PendingProject(
            id: "project-renamed",
            document: otherDocument
        )
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: returnToCurrent,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == nil)
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: renamedProject,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == nil)
        #expect(WebShellController.pendingProjectAfterIdentityChange(
            pendingProject: switchToOther,
            sourceProjectID: "project-a",
            targetProjectID: "project-renamed"
        ) == switchToOther)
    }

    @MainActor
    @Test func identityHandoffKeepsWorkspaceLockedUntilQueuedReplacement() async throws {
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: true,
            hasPendingProject: false
        ) == .replaceByNavigation)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: false,
            hasPendingProject: true
        ) == .loadPendingProjectWhileLocked)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: true,
            hasPendingProject: true
        ) == .replaceByNavigation)
        #expect(WebShellController.identityHandoffAction(
            navigationStarted: false,
            hasPendingProject: false
        ) == .unlockCurrentWorkspace)
    }

    @MainActor
    @Test func fileOperationPersistsImmediateWebEditBeforeRename() async throws {
        let latestDocument = WorkspaceDocument(rawObject: ["revision": .number(2)])
        let snapshot = WebShellProjectSnapshot(id: "project-a", document: latestDocument)
        var events: [String] = []
        var persistedDocument: WorkspaceDocument?

        let renamedID = try await ProjectFileOperationCoordinator.run(
            capture: {
                events.append("capture")
                // WebShell capture now returns only after its ordered final
                // snapshot persistence has completed successfully.
                events.append("persist")
                persistedDocument = snapshot.document
                return snapshot
            },
            operation: {
                events.append("rename")
                #expect(persistedDocument == latestDocument)
                return "project-renamed"
            }
        )

        #expect(renamedID == "project-renamed")
        #expect(events == ["capture", "persist", "rename"])
    }

    @MainActor
    @Test func captureFailureDoesNotRenameOrOverwriteStoredDocument() async throws {
        let storedDocument = WorkspaceDocument(rawObject: ["revision": .number(1)])
        let persistedDocument = storedDocument
        var renamePerformed = false

        do {
            let _: String = try await ProjectFileOperationCoordinator.run(
                capture: {
                    throw WebShellFileOperationError.captureFailed("synthetic capture failure")
                },
                operation: {
                    renamePerformed = true
                    return "project-renamed"
                }
            )
            Issue.record("A failed capture must abort the file operation")
        } catch {
            #expect(error as? WebShellFileOperationError == .captureFailed("synthetic capture failure"))
        }

        #expect(!renamePerformed)
        #expect(persistedDocument == storedDocument)
    }

    @MainActor
    @Test func committedRenameUpdatesNativeIdentityBeforeWebRebindFailure() async throws {
        enum SyntheticRebindError: Error {
            case failed
        }

        var events: [String] = []
        var selectedProjectID = "project-old"

        do {
            _ = try await ProjectRenameCommitCoordinator.run(
                renameOnDisk: {
                    events.append("disk-rename")
                    return "project-new"
                },
                commitNativeState: { renamedProjectID in
                    events.append("native-selection")
                    selectedProjectID = renamedProjectID
                },
                rebindWebIdentity: { _ in
                    events.append("web-rebind")
                    throw SyntheticRebindError.failed
                }
            )
            Issue.record("Expected the synthetic WebKit rebind to fail")
        } catch SyntheticRebindError.failed {
            // The native identity commit must survive the later WebKit error.
        }

        #expect(events == ["disk-rename", "native-selection", "web-rebind"])
        #expect(selectedProjectID == "project-new")
    }

    @MainActor
    @Test func staleThemeCompletionCannotOverrideTheCurrentMode() async throws {
        #expect(ThemeCompletionLifecycle.accepts(
            requestedMode: "dark",
            currentMode: "dark"
        ))
        #expect(!ThemeCompletionLifecycle.accepts(
            requestedMode: "light",
            currentMode: "dark"
        ))
    }

    @MainActor
    @Test func webShellBridgeTagsMessagesAndCommitsIdentityAfterWorkspaceApply() async throws {
        #expect(WebShellController.supportedContractVersion == 2)
        #expect(WebShellController.supportedWorkspaceVersion == WorkspaceDocument.currentVersion)
        let source = WebShellController.bridgeScriptSource(
            initialThemeMode: "auto",
            generation: "navigation-42",
            trustedOrigin: "http://127.0.0.1:18088"
        )

        #expect(source.contains("const bridgeGeneration = \"navigation-42\";"))
        #expect(source.contains("setFileOperationLocked(locked)"))
        #expect(source.contains("document.documentElement.inert = shouldLock"))
        #expect(source.contains("snapshotSequence: 0"))
        #expect(source.contains("publicationRevision: 0"))
        #expect(source.contains("terminationSealed: false"))
        #expect(source.contains("sequence: shellState.snapshotSequence"))
        #expect(source.contains("captureProjectSnapshot()"))
        #expect(source.contains("sealAndCaptureProjectSnapshot()"))
        #expect(source.contains("async function performTerminationSealAndCapture()"))
        #expect(source.contains("await import('./js/state.js')"))
        #expect(source.contains("runtimeState.advanceWorkspaceRuntimeEpoch();"))
        #expect(source.contains("rebindProjectIDAndCapture(projectID)"))
        #expect(source.contains("return postSnapshot(jsonString, true);"))
        #expect(source.contains("return applyFileOperationLock(locked);"))
        #expect(source.contains("const trustedOrigin = \"http://127.0.0.1:18088\";"))
        #expect(source.contains("const trustedPath = \"/index-node.html\";"))
        #expect(source.contains("metadata.contractVersion !== 2"))
        #expect(source.contains("metadata.workspaceVersion > 2"))
        #expect(source.contains("if (window.location.origin !== trustedOrigin) return;"))
        #expect(source.contains("if (window.location.pathname !== trustedPath) return;"))
        #expect(source.contains(
            "handler.postMessage({ generation: bridgeGeneration, type, payload });"
        ))
        #expect(source.contains("rebindProjectID(projectID)"))
        #expect(source.contains("shellState.projectID = projectID;"))

        guard
            let postStart = source.range(of: "function postSnapshot(jsonString, force = false)"),
            let captureStart = source.range(
                of: "function captureSnapshotWithoutPosting()",
                range: postStart.upperBound..<source.endIndex
            ),
            let sealStart = source.range(
                of: "async function performTerminationSealAndCapture()",
                range: captureStart.upperBound..<source.endIndex
            ),
            let descriptorStart = source.range(
                of: "function describeTerminationFence()",
                range: sealStart.upperBound..<source.endIndex
            )
        else {
            Issue.record("Expected the termination publication-fence bridge")
            return
        }
        let postBody = String(source[postStart.lowerBound..<captureStart.lowerBound])
        let sealBody = String(source[sealStart.lowerBound..<descriptorStart.lowerBound])
        let sealedGuard = try #require(postBody.range(of: "if (shellState.terminationSealed)"))
        let rejectedReturn = try #require(postBody.range(of: "return null;"))
        let revisionAdvance = try #require(postBody.range(of: "shellState.publicationRevision += 1;"))
        #expect(sealedGuard.lowerBound < rejectedReturn.lowerBound)
        #expect(rejectedReturn.lowerBound < revisionAdvance.lowerBound)

        let sealCommit = try #require(sealBody.range(of: "shellState.terminationSealed = true;"))
        let runtimeImport = try #require(sealBody.range(of: "await import('./js/state.js')"))
        let epochAdvance = try #require(sealBody.range(of: "runtimeState.advanceWorkspaceRuntimeEpoch();"))
        let finalSerialization = try #require(
            sealBody.range(of: "jsonString = contract.serializeWorkspace();", options: .backwards)
        )
        #expect(sealCommit.lowerBound < runtimeImport.lowerBound)
        #expect(runtimeImport.lowerBound < epochAdvance.lowerBound)
        #expect(epochAdvance.lowerBound < finalSerialization.lowerBound)
        #expect(!sealBody.contains("applyWorkspaceFromJSONString"))

        guard
            let finalCaptureStart = source.range(of: "rebindProjectIDAndCapture(projectID)"),
            let setLockStart = source.range(
                of: "setFileOperationLocked(locked)",
                range: finalCaptureStart.upperBound..<source.endIndex
            )
        else {
            Issue.record("Expected the identity final-capture bridge body")
            return
        }
        let finalCaptureBody = String(
            source[finalCaptureStart.lowerBound..<setLockStart.lowerBound]
        )
        #expect(finalCaptureBody.contains("return postSnapshot(jsonString, true);"))
        #expect(!finalCaptureBody.contains("applyFileOperationLock(false)"))

        guard
            let loadStart = source.range(of: "loadProjectFromJSONString(jsonString, projectID)"),
            let copyStart = source.range(of: "copyText(text)", range: loadStart.upperBound..<source.endIndex)
        else {
            Issue.record("Expected the native project-load bridge body")
            return
        }
        let loadBody = String(source[loadStart.lowerBound..<copyStart.lowerBound])
        guard
            let apply = loadBody.range(of: "applied = contract.applyWorkspaceFromJSONString(jsonString);"),
            let projectCommit = loadBody.range(of: "shellState.projectID = projectID;"),
            let fallbackSnapshotCommit = loadBody.range(of: "shellState.lastSnapshot = jsonString;"),
            let serialization = loadBody.range(
                of: "shellState.lastSnapshot = contract.serializeWorkspace?.() || jsonString;"
            ),
            let serializationFailure = loadBody.range(
                of: "Workspace applied, but snapshot serialization failed:"
            ),
            let successfulReturn = loadBody.range(
                of: "return true;",
                options: .backwards
            )
        else {
            Issue.record("Expected an apply-then-commit bridge sequence")
            return
        }

        #expect(loadBody.contains("if (applied === false) return false;"))
        #expect(loadBody.contains(
            "if (typeof projectID !== 'string' || projectID.length === 0) return false;"
        ))
        #expect(!loadBody.contains("projectID || shellState.projectID"))
        #expect(apply.lowerBound < projectCommit.lowerBound)
        #expect(projectCommit.lowerBound < serialization.lowerBound)
        #expect(fallbackSnapshotCommit.lowerBound < serialization.lowerBound)
        #expect(serialization.lowerBound < serializationFailure.lowerBound)
        #expect(serializationFailure.lowerBound < successfulReturn.lowerBound)
        #expect(loadBody.contains("diagnostics must not undo it"))
    }

}
