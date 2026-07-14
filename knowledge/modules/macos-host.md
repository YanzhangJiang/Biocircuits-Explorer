# macOS Host

`module_id: macos-host`

## Purpose

Provide a native SwiftUI shell that stores workspace documents and supervises
the local Julia and optional Python helpers. The native runtime contract is
local by design: Julia must bind to loopback, must return an explicit ready
payload before use, and the chat helper receives the Julia port actually chosen
by the app.

Each chat-helper launch gets a fresh bearer secret. Browser requests must also
come from the one exact loopback origin derived from the Julia port; possession
of a stale secret or use of a different port is not enough.

## Non-goals

- The native shell does not implement scientific computation or reinterpret
  evidence returned by the web and backend layers.
- It is not the canonical owner of workspace-node fields; it preserves the JSON
  document and delegates workspace semantics to the web bridge.
- Source-level tests and an ad-hoc signature are not proof of a signed,
  notarized, distributable application.

## Owner paths

- Application UI and lifecycle: `frontend-swift/BiocircuitsExplorerMac/`
- Web bridge: `frontend-swift/BiocircuitsExplorerMac/WebShellController.swift`
- Julia supervision:
  `frontend-swift/BiocircuitsExplorerMac/BiocircuitsBackendController.swift`
- Chat supervision and native authentication:
  `frontend-swift/BiocircuitsExplorerMac/DesignChatBackendController.swift`
- Controller and workspace contracts:
  `frontend-swift/BiocircuitsExplorerMacTests/`
- Project persistence: `frontend-swift/BiocircuitsExplorerMac/ProjectStore.swift`
  and `frontend-swift/BiocircuitsExplorerMac/WorkspaceDocument.swift`
- macOS 14 target: `frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj`
- Bundle construction and resource allowlist: `packaging/`, especially
  `packaging/design-runtime-files.txt`, plus `scripts/build_macos_dmg.sh` and
  `frontend-swift/scripts/copy_backend_into_app.sh`
- Chat-side enforcement: `webapp/scripts/chat_api.py` and
  `webapp/scripts/test_chat_api.py`

## Inputs

- A workspace JSON document from an existing project, import, or web-bridge
  update.
- A bundled backend or a development checkout with Julia.
- The Julia port selected by `BiocircuitsBackendController`; the chat helper has
  a separate service port.
- For distributable packaging, an operator-supplied, pinned and already-unpacked
  relocatable Python 3.9+ runtime rooted at `DESIGN_PYTHON_SOURCE`, plus the
  allowlisted Design Agent source/data files included in the backend bundle.
  Local development may instead use an explicit interpreter or `PATH` fallback.
- An optional `deploy/aws-runtime.env`; only explicitly approved AWS, Cognito,
  and quota keys are accepted from it.

## Outputs

- A native app embedding `index-node.html` in `WKWebView`.
- Atomically written project JSON plus import, duplicate, rename, and delete
  operations.
- Supervised loopback Julia and Python processes with parent-watchdog values.
- A per-chat-process bearer secret and an exact origin
  `http://127.0.0.1:<actual Julia port>` supplied to both the helper and the web
  shell.
- An optional embedded authentication page only for the exact HTTPS Cognito
  origin returned by `/api/v1/auth/config`; unrelated HTTPS navigation is sent
  to the user's default browser instead of being trusted inside the WebView.
- A macOS 14 app/DMG build path containing the backend, public assets, version
  file, allowlisted Design Agent runtime files, and—when packaging a release—a
  self-contained Python runtime under the standard Helpers backend tree.

## Contract sources

- Julia loopback launch, `/ready` parsing, environment filtering, and parent
  supervision: `BiocircuitsBackendController.swift`
- Actual engine-port propagation, fresh bearer generation, exact-origin
  injection, authenticated helper probes, and token cleanup:
  `DesignChatBackendController.swift` and `ContentView.swift`
- Bearer/origin enforcement and loopback-only chat binding:
  `webapp/scripts/chat_api.py`
- JavaScript bridge surface and token handoff: `WebShellController.swift`,
  `webapp/public/js/workspace.js`, and `webapp/public/js/agent-view.js`
- Workspace v2 migration, strict typed connections, historical restore, and
  forward-compatible field preservation: `WorkspaceDocument.swift`,
  `webapp/public/js/workspace-v2.js`, `schemas/workspace.schema.json`, and the
  shared fixtures under `tests/fixtures/workspace/`
- Minimum OS and release build settings: the Xcode project and
  `scripts/build_macos_dmg.sh`
- Packaged-resource allowlist: `packaging/design-runtime-files.txt`; this list,
  rather than a broad repository copy, defines the Design Agent files shipped.

## Tests

- `BiocircuitsExplorerMacTests.swift` checks that readiness requires HTTP 200
  with `status: ready`, the chat helper receives the real Julia port without
  changing its own port, fresh bearer values differ, the exact Origin and
  Authorization headers are propagated, and an AWS runtime file cannot replace
  native host/port/assets/parent settings or enable local-image access.
- The same Swift target covers packaged chat discovery and lossless workspace
  normalization/round trips, strict imported-node validation, exact external
  authentication origin matching, termination registration and window-close
  interception, sequenced rename snapshots, queued-project handoff, and the
  post-rename WebKit-failure path. UI tests remain launch-template coverage.
- Workspace tests consume the same v1 input, expected v2 document, strict
  seven-family port matrix, and future-version fixtures as the JavaScript
  contracts. They require exact structural equality for v1 migration,
  reject unsupported future documents and invalid v2 connections, strip
  nested session/ticket fields, and downgrade restored current results to
  historical without changing their scientific evidence.
- `webapp/scripts/test_chat_api.py` checks loopback binding, exact-origin
  validation, bearer enforcement, preflight rules, and parent-watchdog behavior.
- `packaging/test_design_runtime.jl` stages only the resource allowlist, checks
  required chat/Reader/schema paths, removes Python bytecode caches, and probes
  stdlib-only chat imports when Python is available.
- `tests/version_resource_contract.jl` checks version lookup in the installed
  backend resource layout.

## CI

`.github/workflows/ci.yml` runs the Python chat contracts, the packaged resource
allowlist test, version-resource lookup, and repository checks that keep release
identity and the macOS 14 target aligned. The current committed revision also adds a
no-sign `build-for-testing` plus the Swift unit bundle on `macos-15-intel` and
`macos-26`.

That workflow configuration is not evidence that a remote run passed. It does
not run Swift UI tests, launch the packaged backend and chat processes end to
end, create a DMG, perform Developer ID signing/notarization, or test a clean-host
installation.

## Invariants

- Native Julia launch sets both current and legacy host variables to
  `127.0.0.1`, sets both port variables to the controller's port, and supplies
  the app PID. It is not marked ready until `/ready` returns HTTP 200 JSON with
  `status` exactly `ready`.
- `ContentView` passes `backendController.port` into the chat controller. That
  port drives the engine environment and exact allowed origin; the independent
  chat port is not substituted for it.
- Every newly launched native chat process gets a random 32-byte value rendered
  as 64 hexadecimal characters. The token is cleared on stop or process exit
  and is sent as `Authorization: Bearer ...` for native and WebView requests.
- The chat service accepts only a literal loopback bind host and one canonical
  HTTP(S) loopback origin without path/query/fragment. Native launch disables
  the unauthenticated loopback development exception.
- Values loaded from `aws-runtime.env` pass through an explicit key allowlist.
  Native bootstrap values for HOME, loopback binding, actual port, public
  assets, and parent PID always win; image selection, local-image opt-in, and
  unrelated operator keys do not pass through.
- Workspace v2 documents reject unsupported future versions before project
  replacement while preserving unknown extension fields in supported
  documents. V1 merged compute nodes expand into their typed config/result
  pairs, invalid cross-family wires fail closed, runtime session/ticket fields
  are removed recursively, and persisted derived output restores as historical.
  Imported canvas and node records must also pass finite-number, range,
  identity, node-type, port, and shape rules. The repository verifier keeps the
  native supported-node set equal to the Web `NODE_TYPES` registry; Swift,
  JavaScript, Schema, and shared fixture versions advance together.
- Only the exact configured HTTPS Cognito origin may remain in the embedded
  authentication context. A merely HTTPS URL, subdomain, lookalike, path, or
  malformed/disabled authentication configuration does not gain that trust.
- The Xcode project and release helper target macOS 14.0. The packaged Design
  Agent surface is limited to `packaging/design-runtime-files.txt`; optional
  Reader host dependencies are not implied by that list.
- Release-mode packaging fails closed without `DESIGN_PYTHON_SOURCE`. The
  staged runtime must expose `bin/python3`, remain internally relocatable, load
  the required standard library and `chat_api` in isolated mode, and contain the
  target architecture in every Mach-O file. Local packaging may omit it with an
  explicit warning and retain development-only interpreter discovery.
- Chat failure is non-fatal to the node workspace, and app-started helpers are
  stopped with the app or exit through their parent watchdog.

## Working-tree lifecycle and persistence maintenance

- Web navigation, workspace apply, snapshot capture, bridge messages, backend
  launches, and theme callbacks carry generation/identity checks. A stale
  callback cannot commit project identity, readiness, diagnostics, or app
  appearance for its replacement.
- Project switching, reload, rename, duplicate, and delete capture and persist
  the latest applied Web workspace first. Capture or persistence failure stops
  the requested operation instead of navigating with or writing an older copy.
- Rename freezes Web interaction before its final snapshot and retains that lock
  through the same-directory move, explicit project-identity rebind, sequenced
  final persistence, and any queued project replacement. A navigation replaces
  the still-locked document; a queued project unlocks only after it is applied.
  This closes the interval in which a late edit could otherwise be saved under
  the old project identity after the rename had already committed. Once the
  file move succeeds, native selection commits to the new identity even if a
  later WebKit rebind reports an error.
- Closing the sole main window requests application termination instead of
  destroying the editing session first. The app deduplicates concurrent quit
  preparations, captures the final workspace, and keeps the window recoverable
  when persistence fails and termination is cancelled.
- One unreadable project JSON is isolated and reported without hiding valid
  projects. Delete changes in-memory state only after disk removal succeeds;
  rename uses a same-directory file move; duplicate writes the current document
  directly without publishing an intermediate starter file.
- The app exposes one main `Window` per process while project stores still own
  one on-disk file per workspace. This avoids multiple independent window stores
  racing on the same directory until a revisioned multi-window model exists.

## Known gaps

- P2 — Swift unit tests are now wired into checked-in CI, but no resulting
  remote run is claimed. Actual packaged-process startup, port contention,
  interactive WebView traffic, termination races, UI automation, and fallback
  discovery are not exercised end to end.
- P2 — No checked-in lane produces a Developer ID-signed and notarized DMG,
  verifies Gatekeeper on a clean macOS 14 host, or distributes updates. The
  release script's default signature is ad hoc. The prepared external release
  evidence schema, template, and runbook define this lane but have not executed
  it.
- P2 — The complete packaged app has not been launched and probed in CI. The
  release script now validates an operator-supplied Python runtime before
  signing, but no specific runtime artifact is supplied by this repository, and
  clean-host launch, optional Reader dependencies, and bundled-backend fallback
  remain release qualification checks.
- P2 — Focused Swift and JavaScript tests exercise lifecycle state machines but
  do not drive rapid real pointer input through `WKWebView` or launch two app
  processes against the same project directory.
- P2 — Exact Cognito-origin policy is covered with native unit contracts, but a
  real Cognito/federated-provider sign-in and callback has not been exercised.
- P2 — The resource allowlist proves which tracked files are staged; it does not
  bundle or validate every optional dataset and third-party Python dependency.

## Change protocol

1. Change the bridge only with a matching JavaScript contract/version decision
   and tests on both sides.
   Keep the native supported-node set and the Web `NODE_TYPES` registry equal.
2. Preserve loopback binding, exact `/ready` parsing, the actual Julia port,
   bearer rotation, exact-origin checks, and parent supervision when changing
   process launch or discovery.
3. Add a runtime environment key only after deciding that an operator file may
   control it; keep network binding, assets, parent PID, and local-file exposure
   under native ownership.
4. Add a packaged Design Agent file through
   `packaging/design-runtime-files.txt` and its staging test, not through a broad
   directory copy.
5. Run local Swift tests, a no-sign build, packaged backend probes, signing
   verification, and clean-host Gatekeeper checks at the evidence level claimed
   by a release.

## Verified against

- Source commit: `b91cf41`
- Lifecycle/persistence extension: a local macOS 27 arm64 no-sign test build
  and 51/51 Swift unit tests passed on 2026-07-15, including shared Workspace v2
  migration fixtures, with no remote-CI, packaged-app, or notarization claim.
- Evidence inspected: Swift controllers/tests, chat enforcement/tests, WebView
  handoff, Workspace v2 JavaScript/Schema/fixture parity, Xcode target, runtime
  environment allowlist, packaging scripts, packaged-resource allowlist/tests,
  and CI wiring.
- Boundary: loopback/readiness/real-port/bearer/origin/environment contracts are
  present and focused tests exist; no CI Swift run, packaged-app execution,
  signed/notarized DMG, or clean-host installation is claimed verified.
