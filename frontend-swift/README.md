# frontend-swift

Native macOS shell for Biocircuits Explorer.

This Xcode project is the supported native macOS shell. It reuses the repo root backend and web frontend instead of copying them into a separate app workspace.

The native shell loads `webapp/public/index-node.html` and talks to the
embedded web UI through the explicit `window.BiocircuitsExplorerWorkspaceShell`
contract installed by [`webapp/public/js/main.js`](../webapp/public/js/main.js)
(see also `js/workspace.js` for the contract implementation). Save/load actions
and workspace change notifications flow through that contract instead of
depending on the Swift layer to patch internal frontend functions.

Imported project documents pass the same fail-closed workspace shape checks as
the embedded Web loader before they are written. The repository verifier also
keeps the native supported-node set equal to the Web `NODE_TYPES` registry.
During rename, the shell freezes Web interaction from the final snapshot through
the file move, project-identity rebind, ordered final persistence, and any queued
project replacement so an edit cannot be attributed to the old filename after
that snapshot. The single main window also routes Close through the application
termination coordinator: a failed final save cancels termination while the
window and editing session remain available for recovery.

Backend launch order:

1. `BIOCIRCUITS_EXPLORER_BACKEND_ROOT` if explicitly configured
2. In Debug builds, `webapp/server.jl` with a local Julia installation
3. Bundled or repo-local `dist/BiocircuitsExplorerBackend`
4. Source-mode fallback if no compiled backend is available

The Xcode target includes a build phase that copies `dist/BiocircuitsExplorerBackend`
into `BiocircuitsExplorerMac.app/Contents/Helpers/BiocircuitsExplorerBackend` when
that bundle exists. The older `Contents/Resources/backend` layout remains a
runtime read-compatibility path, but new packages do not write nested code there.
If the compiled backend is missing, the build still succeeds and the app falls
back to source-mode startup.

The app-managed workspace JSON files live under:

- `~/Library/Application Support/Biocircuits Explorer/Projects/`

Backend jobs, Atlas data, Design Agent traces, and Julia's user depot also live
under `~/Library/Application Support/Biocircuits Explorer/`; a packaged app never
writes into its signed `.app` bundle.

Runtime overrides:

- `BIOCIRCUITS_EXPLORER_BACKEND_ROOT`: explicit compiled backend bundle to launch
- `BIOCIRCUITS_EXPLORER_REPO_ROOT`: explicit repo root used for source-mode fallback
- `BIOCIRCUITS_EXPLORER_PREFER_SOURCE_BACKEND`: optional `true`/`false` override for Debug-style source preference
- `BIOCIRCUITS_EXPLORER_PORT`: explicit local backend port used by the Swift shell
- `JULIA_EXECUTABLE`: explicit Julia binary for source-mode fallback
- `BNE_PYTHON` / `PYTHON_EXECUTABLE`: explicit Python 3.9+ executable for development fallback only; packaged releases prefer their bundled runtime
- `BIOCIRCUITS_EXPLORER_BACKEND_BUNDLE_SOURCE`: build-time source for the Xcode copy script

Legacy `ROP_*` overrides are still accepted for backward compatibility.

Local build example:

```bash
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

The source project requires Xcode 16.3 or newer for Swift Testing and the
Swift 6.1 `nonisolated` declarations used by background project persistence. Checked-in CI
builds the host tests on both macOS 15 and macOS 26; the deployment target stays
macOS 14.

Build with an embedded backend bundle:

```bash
julia --startup-file=no --project=packaging packaging/build_backend_app.jl
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

Create an explicitly local, ad-hoc-signed DMG:

```bash
RELEASE_MODE=local scripts/build_macos_dmg.sh
```

Packaging invokes `JULIA_BIN` (default: `julia`) directly and verifies that it
is Julia 1.12 before building the backend. When `JULIA_BIN` is managed by
`juliaup`, an operator may explicitly select a channel, for example
`JULIA_CHANNEL=1.12 scripts/build_macos_dmg.sh`; no channel suffix is added by
default. Every packaging invocation disables the user startup file so a local
`startup.jl` cannot replace the selected project or mutate the release build.

Local packaging may omit Design Chat Python. In that case the script prints a
warning, the Julia node workspace still works, and development launches can use
`BNE_PYTHON` or a `python3` found on `PATH`.

Distributable builds do not rely on `/usr/bin/python3`, Homebrew, or another
interpreter installed on the destination Mac. Set `DESIGN_PYTHON_SOURCE` to a
trusted, already-unpacked, relocatable Python 3.9+ runtime whose executable is
`bin/python3`. The release script copies it to
`Contents/Helpers/BiocircuitsExplorerBackend/python`, rejects external or broken
symlinks, rejects non-system absolute Mach-O dependencies and escaping rpaths,
and checks every Mach-O slice against `TARGET_ARCH`. Its isolated probe requires
the interpreter prefix, import paths, standard-library modules, extension
modules, and `chat_api` to resolve inside the staged backend. The same probe runs
again after inside-out signing, so Hardened Runtime loading failures stop the
build before the DMG is created. The script never downloads a runtime; pinning
and obtaining that input remains a release-operator responsibility.

A distributable build fails closed unless a Developer ID identity and a stored
`notarytool` profile and the Python runtime input are supplied. That path signs
with Hardened Runtime, submits the DMG for notarization, staples the ticket, and
asks Gatekeeper to assess both the app and disk image:

```bash
RELEASE_MODE=release \
SIGN_IDENTITY='Developer ID Application: Example (TEAMID)' \
NOTARY_PROFILE='biocircuits-notary' \
DESIGN_PYTHON_SOURCE='/absolute/path/to/pinned-python-runtime' \
scripts/build_macos_dmg.sh
```

`SKIP_BACKEND=1` is not an unaudited formal-release shortcut. A prebuilt backend
must carry `macos-release-metadata.txt` declaring the exact application version,
target architecture, backend mode, Julia 1.12 version, and a deterministic
payload hash. In release mode, set `PREBUILT_BACKEND_SHA256` to that lowercase
hash from the trusted build record; the script recomputes it before injecting
the separately validated Python runtime. The hash excludes the provenance file
itself and the injected `python/` tree to avoid a circular digest.

`CFBundleVersion` maps a supported SemVer `major.minor.patch` to
`(major + 1001).minor.patch`, so `0.1.0`, `1.0.0`, and `12.34.56` become
`1001.1.0`, `1002.0.0`, and `1013.34.56`. The 1001 offset keeps new packages
above the Xcode project's historical build 1000 and remains monotonic for
`major <= 8998` and `minor, patch <= 99`. For a formal rebuild of the same
marketing version, pass an override strictly greater than its derived value,
such as `APPLE_BUILD_NUMBER=1002` for `0.1.0`; the script rejects equal or lower
values as well as values outside Apple's one-to-three-component limits. Formal
prerelease or build-metadata versions fail closed without this override because
their Apple marketing version shares the numeric core. A later stable release
must use an override greater than every distributed prerelease/rebuild; the
repository cannot infer that external release-history maximum on its own.

Successful script execution is local release evidence; installation on clean
macOS 14/15/26 machines remains a separate release qualification step.
