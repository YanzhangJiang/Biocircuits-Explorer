# macOS Host

`module_id: macos-host`

## Purpose

Wrap the web workspace in a native SwiftUI application, persist project JSON in
the platform Application Support area, and supervise the local Julia backend
and optional Python Design Agent helper.

## Non-goals

- The native shell does not implement scientific computation or reinterpret
  evidence returned by the web and backend layers.
- It is not the canonical owner of workspace-node fields; it preserves the JSON
  document and delegates workspace semantics to the web bridge.
- A successful Xcode project parse or placeholder launch test is not proof of a
  signed, notarized, distributable application.

## Owner paths

- Application UI and lifecycle: `frontend-swift/BiocircuitsExplorerMac/`
- Web bridge: `frontend-swift/BiocircuitsExplorerMac/WebShellController.swift`
- Backend supervision:
  `frontend-swift/BiocircuitsExplorerMac/BiocircuitsBackendController.swift` and
  `frontend-swift/BiocircuitsExplorerMac/DesignChatBackendController.swift`
- Project persistence: `frontend-swift/BiocircuitsExplorerMac/ProjectStore.swift`
  and `frontend-swift/BiocircuitsExplorerMac/WorkspaceDocument.swift`
- Xcode target: `frontend-swift/BiocircuitsExplorerMac.xcodeproj/`
- Packaging helpers: `frontend-swift/scripts/copy_backend_into_app.sh` and
  `scripts/build_macos_dmg.sh`

## Inputs

- A workspace JSON document from an existing project, import, or the web bridge.
- A bundled compiled backend or a discoverable development checkout with Julia.
- An optional Python interpreter and chat service script for the Design Agent.
- Runtime environment overrides for ports, backend roots, repository discovery,
  and backend preference.

## Outputs

- A native app embedding `index-node.html` in `WKWebView`.
- Atomically written project JSON files and imported/duplicated/renamed projects.
- Locally supervised Julia and Python child processes plus bridge callbacks for
  workspace changes, save/load, theme, cloud-compute preference, and execution.

## Contract sources

- JavaScript side: `webapp/public/js/workspace.js` and
  `webapp/public/js/state.js`
- Swift bridge version checks and callback surface:
  `frontend-swift/BiocircuitsExplorerMac/WebShellController.swift`
- Forward-compatible JSON preservation and version rejection:
  `frontend-swift/BiocircuitsExplorerMac/WorkspaceDocument.swift`
- Backend discovery, readiness probe, and parent-process contract: both backend
  controller files and `webapp/src/config.jl`
- Build embedding: Xcode project build phase and
  `frontend-swift/scripts/copy_backend_into_app.sh`

## Tests

`frontend-swift/BiocircuitsExplorerMacTests/` checks required workspace-field
normalization and preservation of unknown JSON through a round trip.
`frontend-swift/BiocircuitsExplorerMacUITests/` contains only launch-template
and launch-performance coverage; it does not exercise workspace or backend
flows.

## CI

No workflow under `.github/workflows/` invokes `xcodebuild`, Swift unit tests,
UI tests, backend bundling, code signing, notarization, or DMG creation. The
macOS capability is therefore present in source but not CI-verified.

## Invariants

- Workspace documents reject unsupported future versions while preserving
  unknown fields in supported documents.
- The Swift and JavaScript shell contract versions must advance together.
- Swift probes both child services through loopback before marking them ready.
  The Python chat service explicitly binds loopback; the Julia server currently
  binds all interfaces and is not a loopback-only invariant.
- A child process started by the app is stopped with the app; parent-watchdog
  environment values protect against orphan helpers.
- Failure of the optional chat helper must not prevent the node workspace from
  operating.

## Known gaps

- Backend controllers, bridge callbacks, project migration, and process-race
  behavior lack focused Swift tests.
- Bundled resource completeness, compiled-backend fallback, and Python helper
  discovery are not exercised in CI.
- Signing, notarization, update distribution, and a reproducible release lane
  are not established by the checked-in workflows.
- Some Swift comments describe older Design Agent behavior; runtime truth comes
  from `webapp/scripts/design_agent.py` and its contracts, not those comments.
- The native controller reaches Julia through loopback, but the Julia server
  currently listens on `0.0.0.0`; host firewall/exposure and a configurable bind
  address are not covered by Swift or CI contracts.

## Change protocol

1. Change the bridge only with a matching JavaScript contract change, version
   decision, and tests on both sides.
2. Keep project decoding lossless for unknown fields and add an explicit
   migration before bumping the supported workspace version.
3. Preserve chat loopback, readiness, and parent-watchdog behavior when changing
   process launch or discovery order. Do not claim Julia is loopback-only without
   adding a bind-address contract and test.
4. Run a local `xcodebuild test` and a no-sign build before calling a Swift
   change verified; run packaging smoke tests before release claims.

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: Swift sources, Xcode target configuration, Swift test
  files, bridge counterpart, packaging scripts, and absence of Xcode CI wiring.
- Boundary: source review only; no Swift, UI, signing, notarization, or packaged
  app execution is reported as verified.
